//! Wrapper that exposes [table::DirectoryTable] and [descriptor::FileHandle] as  Fuse filesytem

#![allow(unused_imports)]

use libc::{ptsname_r, stat};
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
use polyfuse::{op, reply::*, Request};
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
use tracing::{debug, error, info, warn, trace};
// use tracing_subscriber::fmt;
mod connection;
pub mod table;
use crate::cli;
mod descriptor;
use descriptor::FHno;
use descriptor::WriteError;
mod buffer;
pub mod dir_iter;
mod message;

mod options;
pub(crate) use options::*;


macro_rules! unwrap_or_return{
    ($result:expr, $request:ident) => {
        match $result {
            Ok(x) => x,
            Err(err) => {return $request.reply_error(err.raw_os_error());}
        }
    }
}

/// Default time to live for attributes returned to the kernel
const TTL: Duration = Duration::from_secs(1);

/// Main filesytem  handle. Representes  the connection to  the rabbit
/// server and the one-deep list of directories inside it.
///
/// None of its methods return errors themselves, rather they set the
/// error value on the request, which Fuse will eventually use to set
/// `errno` for the caller. Those error codes are documented as
/// Errors, despite no Rust `Err` ever being returned.
pub(crate) struct Rabbit {
    /// Open RabbitMQ connection
    connection: Arc<RwLock<lapin::Connection>>,

    /// [Self::write] will publish message to this exchnage
    exchange: String,

    /// [Self::write] will publish message to this routing key
    routing_keys: Arc<table::DirectoryTable>,

    /// Table of open file handles
    file_handles: descriptor::FileHandleTable,

    /// UID of the user who created the moutn
    uid: u32,

    /// GID of the user who created the moutn
    gid: u32,

    /// Time to live of metadata returned to the kernel
    ttl: Duration,

    /// Options that control the behavior of [Self::write]
    write_options: WriteOptions,
}

impl Rabbit {
    /// Create a new filesystem from the command-line arguments
    pub async fn new(args: &cli::Args) -> Rabbit {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

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
            routing_keys: table::DirectoryTable::new(uid, gid, 0o700),
            file_handles: descriptor::FileHandleTable::new(),
            write_options: args.options.clone(),
        }
    }

    /// Returns stats about the filesytem
    pub async fn statfs(&self, req: &Request, _op: op::Statfs<'_>) -> io::Result<()> {
        let mut out = StatfsOut::default();
        let stat = out.statfs();
        stat.files(self.routing_keys.map.len() as u64);
        stat.namelen(255);

        req.reply(out)
    }

    /// Lookup the inode of a file in a parent directory by name
    /// # Errors
    /// - ENOENT if the parent directory or target name does not exist
    /// - EINVAL if the file name is not valid (e.g. not UTF8)
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
            Some(entry) => entry,
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

        let ino = match parent.value().lookup(name) {
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

    /// Get the attrributes (as in stat(2)) of the inode
    ///
    /// # Errors
    /// - ENOENT if the inode does not exist
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
        debug!(
            "getattr for {}: {:?}",
            node.info().ino,
            debug::StatWrap::from(*node.attr())
        );
        req.reply(out)
    }

    /// Set the attributes of the inode
    ///
    /// # Errors
    /// - ENOENT if the inode does not exist
    pub async fn setattr(&self, req: &Request, op: op::Setattr<'_>) -> io::Result<()> {
        match self.routing_keys.map.entry(op.ino()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let mut out = AttrOut::default();
                let node = &mut entry.get_mut();
                set_attr(node.attr_mut(), &op);
                fill_attr(out.attr(), node.attr());
                out.ttl(self.ttl);
                req.reply(out)
            }
            dashmap::mapref::entry::Entry::Vacant(..) => {
                info!("File does not exist");
                req.reply_error(libc::ENOENT)
            }
        }
    }

    /// Read the contents (that is, the files '.' and '..') of a directory
    ///
    /// # Errors
    /// - ENOENT if the directory does not exist
    /// - EWOULDBLOCK if the directory exists, but is being accessed by another call
    pub async fn readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        info!("Reading directory {} with offset {}", op.ino(), op.offset());

        use dashmap::try_result::TryResult;

        let dir = unwrap_or_return!(self.routing_keys.get(op.ino()), req);
        debug!(
            "Looking for directory {} in parent {}",
            dir.ino(),
            dir.parent_ino
        );

        // Otherwise we are reading '.', so list all the directories.
        // There are no top level files.
        let mut out = ReaddirOut::new(op.size() as usize);

        for (i, (name, entry)) in dir.iter()
            .skip(op.offset() as usize)
            .enumerate()
        {
            debug!("Found directory entry {} in inode {}", name, op.ino());
            debug!("Adding dirent {}  {:?}", i, entry);
            let full = out.entry(
                name.as_ref(),
                entry.ino,
                entry.typ as u32,
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

    /// Create a new directory. Directories can only be created in the root
    ///
    /// # Errors
    /// - EINVAL if the filename is invalid
    /// - EEXIST if a directory of that name already exists
    pub async fn mkdir(&self, req: &Request, op: op::Mkdir<'_>) -> io::Result<()> {
        let parent_ino = op.parent();
        if parent_ino != self.routing_keys.root_ino() {
            error!("Can only create top-level directories");
            return req.reply_error(libc::EINVAL);
        }
        let name = op.name();
        info!("Creating directory {:?} in parent {}", name, parent_ino);
        let mut out = EntryOut::default();

        let stat = match self.routing_keys.mkdir(name, self.uid, self.gid) {
            Ok(attr) => attr,
            _ => {
                return req.reply_error(libc::EEXIST);
            }
        };
        fill_attr(out.attr(), &stat);
        out.ino(stat.st_ino);
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        // self.fill_dir_attr(out.attr());
        info!("New directory has stat {:?}", debug::StatWrap::from(stat));
        req.reply(out)
    }

    /// Remove an empty directory. The root may not be removed.
    ///
    /// # Errors
    /// - EINVAL if the filename is not valid
    /// - ENOTDIR the inode is a file and not a directory
    /// - ENOENT the directory does not exist
    /// Otherwise any error from [table::DirectoryTable::rmdir] is returned
    pub async fn rmdir(&self, req: &Request, op: op::Rmdir<'_>) -> io::Result<()> {
        debug!("Removing directory {}", op.name().to_string_lossy());

        // We only have directories one level deep
        if op.parent() != self.routing_keys.root_ino() {
            error!("Directory too deep");
            return req.reply_error(libc::ENOTDIR);
        }
        unwrap_or_return!(self.routing_keys.rmdir(op.parent(), op.name()), req);
        req.reply(())
    }

    /// Create a new regular file (node). Files may only be created in
    /// directories. There should be no files in the root.
    ///
    /// # Errors
    /// - EINVAL the filename is not valid
    /// Otherwise any error returned from [table::DirectoryTable::mknod] is returned
    pub async fn mknod(&self, req: &Request, op: op::Mknod<'_>) -> io::Result<()> {
        match self.routing_keys.mknod(op.name(), op.mode(), op.parent()) {
            Ok(attr) => {
                let mut out = EntryOut::default();
                out.ino(attr.st_ino);
                fill_attr(out.attr(), &attr);
                out.ttl_attr(self.ttl);
                out.ttl_entry(self.ttl);
                req.reply(out)
            }
            Err(err) => req.reply_error(err.raw_os_error()),
    }
}

    /// Reduce the link count of a file node
    ///
    /// # Errors
    /// - EINVAL the file name is not valid
    /// Otherwise errors from [table::DirectoryTable::unlink] are returned
    pub async fn unlink(&self, req: &Request, op: op::Unlink<'_>) -> io::Result<()> {
        if let Err(err) = self.routing_keys.unlink(op.parent(), op.name()) {
            req.reply_error(err.raw_os_error())
        } else {
            req.reply(())
        }
    }

    pub async fn rename(&self, req: &Request, op: op::Rename<'_>) -> io::Result<()> {
        let oldname = match op.name().to_str(){
            Some(name) => name,
            None => {return req.reply_error(libc::EINVAL);}
        };
        let newname = match op.newname().to_str(){
            Some(name) => name,
            None => {return req.reply_error(libc::EINVAL);}
        };
        debug!("Renameing {} -> {}", oldname, newname);
        let ino = match self.routing_keys.lookup(op.parent(), oldname) {
            Some(ino) => ino,
            None => {return req.reply_error(libc::ENOENT);}
        };
        let mut oldparent = match self.routing_keys.get_mut(op.parent()) {
            Ok(parent) => parent,
            Err(err) => {return req.reply_error(err.raw_os_error());},
        };
        oldparent.remove_child(oldname);

        let entry = match self.routing_keys.get(ino) {
            Ok(e) => e.info().clone(),
            Err(err) => {return req.reply_error(err.raw_os_error());},
        };

        let mut newparent = match self.routing_keys.get_mut(op.newparent()) {
            Ok(parent) => parent,
            Err(err) => {return req.reply_error(err.raw_os_error());},
        };
        newparent.insert_child(newname, &entry);

        req.reply(())
    }

    /// Create a new descriptor for a file
    ///
    /// # Errors
    /// - EISDIR if the inode points to a directory
    /// - ENOENT if the inode does not exist
    ///
    /// # Panics
    /// Will panic if a new AMQP channel can't be opened
    pub async fn open(&self, req: &Request, op: op::Open<'_>) -> io::Result<()> {
        info!("Opening new file handle for ino {}", op.ino());
        let parent_ino = {
            let mut node = match self.routing_keys.map.get_mut(&op.ino()) {
                None => return req.reply_error(libc::ENOENT),
                Some(node) => node,
            };
            if (node.typ()) == libc::DT_DIR {
                error!("Refusing to open; directory is not a file");
                return req.reply_error(libc::EISDIR);
            }
            node.atime_to_now(op.flags());
            trace!("Opening node in parent {}", node.parent_ino);
            node.parent_ino
        };
        // If somehow a file node exists with a parent, the filesytem
        // is corrupted, so it's okay to panic here.
        let routing_key = {
            let parent_ino = self.routing_keys.map.get(&parent_ino).unwrap().ino();
            let root = self.routing_keys.get(self.routing_keys.root_ino()).unwrap();
            root.get_child_name(parent_ino).unwrap()
        };
        trace!("Opening file bound to routing key {}", &routing_key);
        // This is the only place we touch the rabbit connection.
        // Creating channels is not mutating, so we only need read
        // access
        let conn = self.connection.as_ref().read().await;
        trace!("Creating new file handle");
        let fh = match self
            .file_handles
            .insert_new_fh(
                &conn,
                &self.exchange,
                &routing_key,
                op.flags(),
                &self.write_options,
            )
            .await {
                Ok(fh) => fh,
                Err(err) => { return req.reply_error(err.get_os_error().unwrap()); }
            };

        trace!("New file handle {}", fh);
        let mut out = OpenOut::default();
        out.fh(fh);
        out.nonseekable(true);
        req.reply(out)
    }

    /// Synchonrize the file descriptor
    ///
    /// This causes all buffered data in the file the to be written to
    /// the rabbit server. This includes partially formed lines.
    /// Depending on the options given, publishing partly formed lines
    /// may cause errors, which will be emitted as EIO.
    ///
    /// Additionally, this call blocks until all unconfirmed messages
    /// are either confirmed by the server or an error is returned.
    pub async fn fsync(&self, req: &Request, op: op::Fsync<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        debug!("Syncing file {}", op.fh());
        if let Entry::Occupied(mut entry) = self.file_handles.entry(op.fh()) {
            match entry.get_mut().sync(true).await {
                Ok(..) => req.reply(()),
                Err(..) => req.reply_error(libc::EIO),
            }
        } else {
            req.reply_error(libc::ENOENT)
        }
    }

    /// Issued by close(2) when the handle closes
    ///
    /// This causes *completed* lines to be published but not
    /// incomplete ones. The descriptor may still be held elsewhere
    /// and is still valid to write.
    ///
    /// As with [Self::fsync], this will block until previously published
    /// messages are confirmed or an error returned. As such it may
    /// return errors from previous [Self::write] calls
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

    /// Called when the kernel releases the file descriptor, after the last holder calls `close(2)`
    ///
    /// Blocks until the descriptor is fully flushed and all
    /// confirmations recieved. As such it may return errors from
    /// previous calls.  Attempting to use the file handle after release is an error
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

    /// Return empty data
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
                match file.write_buf(data).await {
                    Ok(written) => {
                        debug!("Wrote {} bytes", written);
                        written
                    }
                    Err(WriteError::ParsingError(sz)) => {
                        // On a parser error, if we published
                        // *anything* declare victory, otherwise raise
                        // a generic error
                        if sz.0 == 0 {
                            return req.reply_error(libc::EIO);
                        } else {
                            sz.0
                        }
                    }
                    Err(err) => {
                        error!("Write to fd {} failed", op.fh());
                        // Return the error code the descriptor gave
                        // us, or else a generic "IO error"
                        return req.reply_error(err.get_os_error().unwrap_or(libc::EIO));
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

/// Copy [polyfuse::reply::FileAttr] from `stat_t` structure for `stat(2)`
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

/// Convert the timestamp to a `i64`
fn get_timestamp(time: &op::SetAttrTime) -> i64 {
    match time {
        SetAttrTime::Timespec(dur) => dur.as_secs() as i64,
        SetAttrTime::Now => {
            let now = std::time::SystemTime::now();
            now.duration_since(UNIX_EPOCH)
                .expect("no such time")
                .as_secs() as i64
        }
        &_ => 0,
    }
}

/// Copy the contents of a kernel request into a `stat_t`
fn set_attr(st: &mut libc::stat, attr: &op::Setattr) {
    if let Some(x) = attr.size() {
        st.st_size = x as i64
    };
    if let Some(x) = attr.mode() {
        st.st_mode = x
    };
    if let Some(x) = attr.uid() {
        st.st_uid = x
    };
    if let Some(x) = attr.gid() {
        st.st_gid = x
    };
    if let Some(x) = attr.atime().as_ref() {
        st.st_atime = get_timestamp(x)
    };
    if let Some(x) = attr.mtime().as_ref() {
        st.st_mtime = get_timestamp(x)
    };
}

#[doc(hidden)]
mod debug{
    use std::fmt;

    pub (in super) struct StatWrap {
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
}

impl Drop for Rabbit {
    /// Close the RabbitMQ connection
    fn drop(&mut self) {
        info!("Shutting down filesystem");
        let conn = futures::executor::block_on(self.connection.write());
        info!("Got connection");
        let close = tokio::task::spawn(conn.close(0, "Normal Shutdown"));
        if let Err(..) = futures::executor::block_on(close).expect("Closing connection") {
            match conn.status().state() {
                lapin::ConnectionState::Closed => {}
                lapin::ConnectionState::Closing => {}
                lapin::ConnectionState::Error => {
                    error!("Error closing connection");
                }
                _ => {
                    panic!("Unable to close connection")
                }
            }
        }

        info!("Connection closed");
    }
}
