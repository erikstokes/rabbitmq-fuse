#![allow(unused_imports)]

use libc::stat;
use std::ops::Deref;
use std::{
    cell::RefCell,
    fmt,
    io::{self, BufRead, Cursor},
    mem,
    os::unix::prelude::*,
    rc::Rc,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut, WriteOut},
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

const TTL: Duration = Duration::from_secs(1);

/// Main filesytem  handle. Representes  the connection to  the rabbit
/// server and the one-deep list of directories inside it.
pub(crate) struct Rabbit {
    connection: lapin::Connection,
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
            connection: connection::get_connection(
                &args,
                ConnectionProperties::default().with_tokio(),
            )
            .await
            .unwrap(),
            uid,
            gid,
            ttl: TTL,
            exchange: args.exchange.to_string(),
            routing_keys: table::DirectoryTable::new(&root),
            file_handles: descriptor::FileHandleTable::new(),
        }
    }

    // fn fill_dir_attr(&self, attr: &mut FileAttr) {
    //     attr.ino(self.routing_keys.root_ino);
    //     attr.mode(libc::S_IFDIR as u32 | 0o555);
    //     attr.nlink(2);
    //     attr.uid(self.uid);
    //     attr.gid(self.gid);
    // }

    // fn fill_file_attr(&self, attr: &mut FileAttr) {
    //     attr.ino(HELLO_INO);
    //     attr.size(0);// files always appear empty
    //     attr.mode(libc::S_IFREG as u32 | 0o666);
    //     attr.nlink(1);
    //     attr.uid(self.uid);
    //     attr.gid(self.gid);
    // }

    pub async fn lookup(&self, req: &Request, op: op::Lookup<'_>) -> io::Result<()> {
        info!(
            "Doing lookup of {:?} in parent inode {}",
            op.name(),
            op.parent()
        );

        use dashmap::mapref::entry::Entry;

        match self.routing_keys.map.entry(op.parent()) {
            Entry::Vacant(..) => {
                error!("Parent directory does not exist");
                return req.reply_error(libc::ENOENT);
            }
            Entry::Occupied(entry) => {
                let parent = entry.get();
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

                let ino = match parent.lookup(&name.to_string()) {
                    Some(ino) => ino,
                    None => {
                        return req.reply_error(libc::ENOENT);
                    }
                };
                info!("Found inode {} for {}", ino, name);

                match self.routing_keys.map.entry(ino) {
                    Entry::Vacant(..) => {
                        error!("No such file {}", name);
                        return req.reply_error(libc::ENOENT);
                    }
                    Entry::Occupied(entry) => {
                        let dir = entry.get();
                        out.ino(dir.ino);
                        fill_attr(out.attr(), &dir.attr());
                        req.reply(out)
                    }
                }
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
        let dir = entry.get();
        fill_attr(out.attr(), &dir.attr());
        out.ttl(self.ttl);
        debug!("getattr for {}: {:?}", dir.name, StatWrap::from(dir.attr()));
        req.reply(out)
    }

    pub async fn read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        info!("Reading {} bytes from inode {}", op.size(), op.ino());
        use dashmap::mapref::entry::Entry;
        let entry = match self.routing_keys.map.entry(op.ino()) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(..) => return req.reply_error(libc::ENOENT),
        };

        if entry.get().typ == libc::DT_DIR as u32 {
            return req.reply_error(libc::EISDIR);
        }

        let data: &[u8] = &[]; // Files are always empty

        req.reply(data)
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
                return req.reply_error(libc::ENOENT);
            }
        };

        debug!(
            "Looking for directory {} in parent {}",
            dir.ino, dir.parent_ino
        );

        // Otherwise we are reading '.', so list all the directories.
        // There are no top level files.
        let mut out = ReaddirOut::new(op.size() as usize);

        for (i, entry) in self.routing_keys.iter_dir(&dir).skip(op.offset() as usize) {
            info!("Found directory entry {} in {}", entry.name, op.ino());
            let full = out.entry(
                entry.name.as_ref(),
                entry.ino,
                entry.typ,
                i + 1, // offset
            );
            if full {
                debug!("readdir request full");
                break;
            }
        }

        req.reply(out)
    }

    pub async fn mkdir(&mut self, req: &Request, op: op::Mkdir<'_>) -> io::Result<()> {
        let parent_ino = op.parent();
        if parent_ino != self.routing_keys.root_ino {
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
        let stat = match self.routing_keys.mkdir(str_name) {
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

    pub async fn mknod(&mut self, req: &Request, op: op::Mknod<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        let parent_ino = match self.routing_keys.map.entry(op.parent()) {
            Entry::Vacant(..) => {
                return req.reply_error(libc::ENOENT);
            }
            Entry::Occupied(entry) => entry.get().ino,
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

    pub async fn open(&self, req: &Request, op: op::Open<'_>) -> io::Result<()> {
        info!("Opening new file handle for ino {}", op.ino());
        let node = self.routing_keys.map.get(&op.ino()).unwrap();
        if node.typ == libc::DT_DIR as u32 {
            return req.reply_error(libc::EISDIR);
        }
        let parent = self.routing_keys.map.get(&node.parent_ino).unwrap();
        let fh = self
            .file_handles
            .insert_new_fh(&self.connection, &self.exchange, &parent.name, op.flags())
            .await;
        let mut out = OpenOut::default();
        out.fh(fh);
        out.nonseekable(true);
        req.reply(out)
    }

    pub async fn flush(&self, req: &Request, op: op::Flush<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        debug!("Flushing file handle");
        match self.file_handles.file_handles.entry(op.fh()) {
            Entry::Occupied(mut entry) => match entry.get_mut().sync().await {
                Ok(..) => {
                    debug!("File closed");
                }
                Err(..) => {
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
        match self.file_handles.file_handles.entry(op.fh()) {
            Entry::Occupied(mut entry) => match entry.get_mut().release().await {
                Ok(..) => {
                    debug!("File descriptor removed");
                }
                Err(..) => {
                    return req.reply_error(libc::EIO);
                }
            },
            Entry::Vacant(..) => {
                return req.reply_error(libc::ENOENT);
            }
        }
        self.file_handles.file_handles.remove(&op.fh());
        debug!("Flush complete");
        req.reply(())
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

        let written = match self.file_handles.file_handles.entry(op.fh()) {
            Entry::Vacant(..) => {
                error!("Unable to find file handle {}", op.fh());
                return req.reply_error(libc::ENOENT);
            }
            Entry::Occupied(mut entry) => {
                let file = entry.get_mut();
                debug!("Found file handle {}", file.fh);
                match file.write_buf(data).await {
                    Ok(written) => written,
                    Err(..) => {
                        error!("No such file handle {}", op.fh());
                        return req.reply_error(libc::EIO);
                    }
                }
            }
        };
        debug!(
            "Write complete. Wrote {}/{} requested bytes",
            written,
            op.size()
        );
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
