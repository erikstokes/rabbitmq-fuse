#![allow(unused_imports)]

use libc::stat;
use polyfuse::op::SetAttrTime;
use polyfuse::reply;
use std::ops::Deref;
use std::time::UNIX_EPOCH;
use std::{
    cell::RefCell,
    fmt,
    io::{self, BufRead, Cursor},
    mem,
    os::unix::prelude::*,
    rc::Rc,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use dashmap::DashMap;
use polyfuse::{
    op,
    reply::*,
    Request,
};
use std::collections::hash_map::{Entry, HashMap, RandomState};
use tokio::sync::RwLock;

use lapin::{
    // message::DeliveryResult,
    options::*,
    // publisher_confirm::Confirmation,
    types::ShortString,
    BasicProperties,
    ConnectionProperties,
};

use tokio_amqp::*;
// use pinky_swear::PinkySwear;
#[allow(unused_imports)]
use tracing::{debug, error, info, warn};
// use tracing_subscriber::fmt;
mod connection;
pub mod table;
use crate::cli;
mod descriptor;
use descriptor::FHno;
pub mod dir_iter;

mod options;
pub(crate) use options::WriteOptions;

const TTL: Duration = Duration::from_secs(1);

/// Main filesytem  handle. Representes  the connection to  the rabbit
/// server and the one-deep list of directories inside it.
pub(crate) struct Rabbit {
    connection: Arc<RwLock<lapin::Connection>>,
    exchange: String,
    routing_keys: table::DirectoryTable,
    file_handles: descriptor::FileHandleTable,
    uid: u32,
    gid: u32,
    ttl: Duration,
}

impl Rabbit {
    pub async fn new(args: &cli::Args) -> Rabbit {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let root = table::DirEntry::root(uid, gid, 0o700);

        Rabbit {
            connection: Arc::new(RwLock::new(
                connection::get_connection(args, ConnectionProperties::default().with_tokio())
                    .await
                    .unwrap(),
            )),
            uid,
            gid,
            ttl: TTL,
            exchange: args.exchange.to_string(),
            routing_keys: table::DirectoryTable::new(&root),
            file_handles: descriptor::FileHandleTable::new(args.buffer_size),
        }
    }

    pub async fn statfs(&self, req: &Request, _op: op::Statfs<'_>) -> io::Result<()> {
        let mut out = StatfsOut::default();
        let stat = out.statfs();
        stat.files(self.routing_keys.map.len() as u64);
        stat.namelen(255);

        req.reply(out)
    }

    pub async fn lookup(&self, req: &Request, op: op::Lookup<'_>) -> io::Result<()> {
        info!(
            "Doing lookup of {:?} in parent inode {}",
            op.name(),
            op.parent()
        );

        use dashmap::mapref::entry::Entry;

        let parent = match self.routing_keys.map.get(&op.parent()) {
            None => {
                error!("Parent directory does not exist");
                return req.reply_error(libc::ENOENT);
            }
            Some(entry) => {
                entry
            }
        };

        let mut out = EntryOut::default();
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        // The name is a [u8] (i.e. a `char*`), so we have to cast it to unicode
        let name = match op.name().to_str() {
            Some(name) => name,
            None => {
                return req.reply_error(libc::EINVAL);
            }
        };

        let ino = match parent.value().lookup(&name.to_string()) {
            Some(ino) => ino,
            None => {
                return req.reply_error(libc::ENOENT);
            }
        };
        info!("Found inode {} for {}", ino, name);

        match self.routing_keys.map.entry(ino) {
            Entry::Vacant(..) => {
                error!("No such file {}", name);
                req.reply_error(libc::ENOENT)
            }
            Entry::Occupied(entry) => {
                let dir = entry.get();
                out.ino(dir.ino());
                fill_attr(out.attr(), dir.attr());
                req.reply(out)
            }
        }
    }

    pub async fn getattr(&self, req: &Request, op: op::Getattr<'_>) -> io::Result<()> {
        info!("Getting attributes of {}", op.ino());

        let entry = match self.routing_keys.map.entry(op.ino()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry,
            dashmap::mapref::entry::Entry::Vacant(..) => {
                // handle files here
                info!("File does not exist");
                return req.reply_error(libc::ENOENT);
            }
        };

        // let fill_attr = Self::fill_dir_attr;

        let mut out = AttrOut::default();
        let node = entry.get();
        fill_attr(out.attr(), node.attr());
        out.ttl(self.ttl);
        debug!("getattr for {}: {:?}", node.name(), StatWrap::from(*node.attr()));
        req.reply(out)
    }

    pub async fn setattr(&self, req: &Request, op: op::Setattr<'_>) -> io::Result<()> {
        match self.routing_keys.map.entry(op.ino()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let mut out = AttrOut::default();
                let node = &mut entry.get_mut();
                set_attr(node.attr_mut(), &op);
                fill_attr(out.attr(), node.attr());
                out.ttl(self.ttl);
                req.reply(out)
            },
            dashmap::mapref::entry::Entry::Vacant(..) => {
                info!("File does not exist");
                req.reply_error(libc::ENOENT)
            }
        }

    }

    pub async fn readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        info!("Reading directory {} with offset {}", op.ino(), op.offset());

        use dashmap::try_result::TryResult;

        let dir = match self.routing_keys.map.try_get(&op.ino()) {
            TryResult::Present(entry) => entry,
            TryResult::Absent => {
                return req.reply_error(libc::ENOENT);
            }
            TryResult::Locked => {
                return req.reply_error(libc::EWOULDBLOCK);
            }
        };

        debug!(
            "Looking for directory {} in parent {}",
            dir.ino(), dir.parent_ino
        );

        // Otherwise we are reading '.', so list all the directories.
        // There are no top level files.
        let mut out = ReaddirOut::new(op.size() as usize);

        for (i,entry) in dir_iter::DirIterator::new(&self.routing_keys, &dir).skip(op.offset() as usize).enumerate() {
            info!("Found directory entry {} in inode {}", entry.name, op.ino());
            debug!("Adding dirent {}  {:?}", i, entry);
            let full = out.entry(
                entry.name.as_ref(),
                entry.ino,
                entry.typ,
                i as u64 + 1, // offset
            );
            if full {
                debug!("readdir request full");
                break;
            }
        }

        debug!("Returning readdir reply");
        req.reply(out)
    }

    pub async fn mkdir(&self, req: &Request, op: op::Mkdir<'_>) -> io::Result<()> {
        let parent_ino = op.parent();
        if parent_ino != self.routing_keys.root_ino() {
            error!("Can only create top-level directories");
            return req.reply_error(libc::EINVAL);
        }
        let name = op.name();
        info!("Creating directory {:?} in parent {}", name, parent_ino);
        let mut out = EntryOut::default();
        let str_name = match name.to_str() {
            Some(s) => s,
            None => {
                error!("Invalid filename");
                return req.reply_error(libc::EINVAL);
            }
        };
        let stat = match self.routing_keys.mkdir(str_name, self.uid, self.gid) {
            Ok(attr) => attr,
            _ => {
                return req.reply_error(libc::EEXIST);
            }
        };
        // out.attr().ino(stat.st_ino);
        // out.attr().mode(libc::S_IFDIR | 0x700 as u32);
        fill_attr(out.attr(), &stat);
        out.ino(stat.st_ino);
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        // self.fill_dir_attr(out.attr());
        info!("New directory has stat {:?}", StatWrap::from(stat));
        req.reply(out)
    }

    pub async fn rmdir(&self, req: &Request, op: op::Rmdir<'_>) -> io::Result<()> {
        debug!("Removing directory {}", op.name().to_string_lossy());
        let name = match op.name().to_str() {
            Some(name) => name,
            None => {return req.reply_error(libc::EINVAL);}
        };
        // We only have directories one level deep
        if op.parent() != self.routing_keys.root_ino() {
            error!("Directory too deep");
            return req.reply_error(libc::ENOTDIR);
        }
        let mut root = self.routing_keys.map.get_mut(&op.parent()).expect("Root inode does not exist");
        let ino = match root.lookup(name) {
            Some(ino) => ino,
            None => {return req.reply_error(libc::ENOENT); }
        };
        if let Some(dir) = self.routing_keys.map.get(&ino){
            if dir.num_children() != 0 {
                return req.reply_error(libc::ENOTEMPTY);
            }
        }
        root.remove_child(name);
        self.routing_keys.map.remove(&ino);

        req.reply(())
    }

    pub async fn mknod(&self, req: &Request, op: op::Mknod<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        let parent_ino = match self.routing_keys.map.entry(op.parent()) {
            Entry::Vacant(..) => {
                return req.reply_error(libc::ENOENT);
            }
            Entry::Occupied(entry) => entry.get().ino(),
        };
        let name = match op.name().to_str() {
            Some(name) => name,
            None => {
                return req.reply_error(libc::EINVAL);
            }
        };
        let attr = match self.routing_keys.mknod(name, op.mode(), parent_ino) {
            Ok(s) => s,
            Err(errno) => {
                return req.reply_error(errno);
            }
        };
        let mut out = EntryOut::default();
        out.ino(attr.st_ino);
        fill_attr(out.attr(), &attr);
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        req.reply(out)
    }

    pub async fn unlink(&self, req: &Request, op: op::Unlink<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;

        debug!("Unlinking {} in inode {}", op.name().to_string_lossy(), op.parent());
        // Remove the given name from the parent and return the inode being unlinked
        let unlinked_ino = match self.routing_keys.map.entry(op.parent()) {
            Entry::Occupied(mut entry) => {
                if let Some(name) = op.name().to_str() {
                    if let Some((_name, ino)) = entry.get_mut().remove_child(name) {
                        debug!("Parent now has {} children",
                               entry.get().num_children());
                        ino
                    } else {
                        return req.reply_error(libc::ENOENT);
                    }
                } else {
                    return req.reply_error(libc::EINVAL);
                }
            },
            Entry::Vacant(..) => {return req.reply_error(libc::ENOENT);}
        };

        // Now lookup the child in the inode table. It had better
        // exist or something is very wrong. Reduce the inode count
        // and return if it is now 0
        let remove = match self.routing_keys.map.entry(unlinked_ino) {
            Entry::Occupied(mut entry) => {
                let nlink = entry.get().attr().st_nlink.saturating_sub(1);
                entry.get_mut().attr_mut().st_nlink = nlink;
                nlink == 0
            },
            Entry::Vacant(..) => panic!("Parent inode {} held non-existant indoe with name {}",
                                         op.parent(), op.name().to_string_lossy())
        };

        if remove {
            debug!("Inode {} has link count 0. Removing", unlinked_ino);
            self.routing_keys.map.remove(&unlinked_ino);
        }

        req.reply(())
    }

    pub async fn open(&self, req: &Request, op: op::Open<'_>) -> io::Result<()> {
        info!("Opening new file handle for ino {}", op.ino());
        let mut node = self.routing_keys.map.get_mut(&op.ino()).unwrap();
        if (node.typ()) == libc::DT_DIR as u32 {
            return req.reply_error(libc::EISDIR);
        }
        let parent = self.routing_keys.map.get(&node.parent_ino).unwrap();
        // This is the only place we touch the rabbit connection.
        // Creating channels is not mutating, so we only need read
        // access
        let conn = self.connection.as_ref().read().await;
        let fh = self
            .file_handles
            .insert_new_fh(&conn, &self.exchange, &parent.name(), op.flags())
            .await;
        let mut out = OpenOut::default();
        out.fh(fh);
        out.nonseekable(true);
        node.atime_to_now(op.flags());
        req.reply(out)
    }

    pub async fn fsync(&self, req: &Request, op: op::Fsync<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        debug!("Syncing file {}", op.fh());
        if let Entry::Occupied(mut entry) = self.file_handles.entry(op.fh()) {
            match entry.get_mut().sync(true).await {
                Ok(..) => {
                    debug!("Fsync succeeded");
                    req.reply(())
                }
                Err(..) => {
                    error!("Error in fsync");
                    req.reply_error(libc::EIO)
                }
            }
        } else {
            req.reply_error(libc::ENOENT)
        }
    }

    pub async fn flush(&self, req: &Request, op: op::Flush<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        debug!("Flushing file handle");
        match self.file_handles.file_handles.entry(op.fh()) {
            Entry::Occupied(mut entry) => match entry.get_mut().sync(false).await {
                Ok(..) => {
                    debug!("File closed");
                }
                Err(..) => {
                    error!("File sync returned an error");
                    return req.reply_error(libc::EIO);
                }
            },
            Entry::Vacant(..) => {
                return req.reply_error(libc::ENOENT);
            }
        }
        debug!("Flush complete");
        req.reply(())
    }

    pub async fn release(&self, req: &Request, op: op::Release<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        info!("Releasing file handle");
        match self.file_handles.entry(op.fh()) {
            Entry::Occupied(mut entry) => match entry.get_mut().release().await {
                Ok(..) => {
                    debug!("File descriptor removed");
                }
                Err(..) => {
                    error!("File descriptor {} no longer exists", op.fh());
                    return req.reply_error(libc::EIO);
                }
            },
            Entry::Vacant(..) => {
                return req.reply_error(libc::ENOENT);
            }
        }
        self.file_handles.remove(op.fh());
        debug!("Flush complete");
        req.reply(())
    }

    pub async fn read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        match self.file_handles.entry(op.fh()) {
            Entry::Occupied(..) => {
                let data: &[u8] = &[];
                req.reply(data)
            }
            Entry::Vacant(..) => req.reply_error(libc::ENOENT),
        }
    }

    pub async fn write<T>(&self, req: &Request, op: op::Write<'_>, data: T) -> io::Result<()>
    where
        T: BufRead + Unpin,
    {
        use dashmap::mapref::entry::Entry;

        debug!(
            "Attempting write {} bytes to inode {} with fd {}",
            op.size(),
            op.ino(),
            op.fh()
        );

        let written = match self.file_handles.entry(op.fh()) {
            Entry::Vacant(..) => {
                error!("Unable to find file handle {}", op.fh());
                return req.reply_error(libc::ENOENT);
            }
            Entry::Occupied(mut entry) => {
                let file = entry.get_mut();
                debug!("Found file handle {}", file.fh);
                match file.write_buf(data, &WriteOptions::default()).await {
                    Ok(written) => written,
                    Err(err) => {
                        error!("No such file handle {}", op.fh());
                        return req.reply_error(err.raw_os_error().unwrap_or(libc::EIO));
                    }
                }
            }
        };
        debug!(
            "Write complete. Wrote {}/{} requested bytes",
            written,
            op.size()
        );
        if let Entry::Occupied(mut node) = self.routing_keys.map.entry(op.ino()) {
            node.get_mut().atime_to_now(op.flags());
        }
        // Setup the reply
        let mut out = WriteOut::default();
        out.size(written as u32);
        req.reply(out)
    }
}

fn fill_attr(attr: &mut FileAttr, st: &libc::stat) {
    attr.ino(st.st_ino);
    attr.size(st.st_size as u64);
    attr.mode(st.st_mode);
    attr.nlink(st.st_nlink as u32);
    attr.uid(st.st_uid);
    attr.gid(st.st_gid);
    attr.rdev(st.st_rdev as u32);
    attr.blksize(st.st_blksize as u32);
    attr.blocks(st.st_blocks as u64);
    attr.atime(Duration::new(st.st_atime as u64, st.st_atime_nsec as u32));
    attr.mtime(Duration::new(st.st_mtime as u64, st.st_mtime_nsec as u32));
    attr.ctime(Duration::new(st.st_ctime as u64, st.st_ctime_nsec as u32));
}

fn get_timestamp(time: &op::SetAttrTime) -> i64{
    match time {
        SetAttrTime::Timespec(dur) => dur.as_secs() as i64,
        SetAttrTime::Now =>{
            let now = std::time::SystemTime::now();
            now.duration_since(UNIX_EPOCH).expect("no such time").as_secs() as i64
        }
        &_ => 0
    }
}

fn set_attr(st: &mut libc::stat, attr: &op::Setattr) {
    if let Some(x) = attr.size() {st.st_size = x as i64};
    if let Some(x) = attr.mode() {st.st_mode =x};
    if let Some(x) = attr.uid() { st.st_uid = x};
    if let Some(x) = attr.gid() { st.st_gid = x};
    if let Some(x) = attr.atime().as_ref() { st.st_atime = get_timestamp(x)};
    if let Some(x) = attr.mtime().as_ref() {st.st_mtime = get_timestamp(x)};
}

struct StatWrap {
    stat: libc::stat,
}

impl From<libc::stat> for StatWrap {
    fn from(s: libc::stat) -> StatWrap {
        StatWrap { stat: s }
    }
}

impl fmt::Debug for StatWrap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stat = self.stat;
        f.debug_struct("stat_t")
            .field("st_ino", &stat.st_ino)
            .field("st_size", &stat.st_size)
            .field("st_nlink", &stat.st_nlink)
            .field("uid", &stat.st_uid)
            .field("gid", &stat.st_gid)
            .field("rdev", &stat.st_rdev)
            .field("st_blksize", &stat.st_blksize)
            .field("st_blocks", &stat.st_blocks)
            .finish()
    }
}

impl Drop for Rabbit {
    fn drop(&mut self) {
        info!("Shutting down filesystem");
        let conn = futures::executor::block_on(self.connection.write());
        info!("Got connection");
        let close = tokio::task::spawn(conn.close(0, "Normal Shutdown"));
        if let Err(..) = futures::executor::block_on(close).expect("Closing connection"){
            match conn.status().state() {
                lapin::ConnectionState::Closed => {},
                lapin::ConnectionState::Closing => {},
                lapin::ConnectionState::Error => {error!("Error closing connection");},
                _ => {panic!("Unable to close connection")}
            }
        }

        info!("Connection closed");
    }
}
