#![allow(unused_imports)]

use std::{io::{self, BufRead},
          os::unix::prelude::*,
          time::Duration,
          sync::atomic::{AtomicU64, Ordering},
          sync::Arc,
          mem,
          fmt,
          cell::RefCell,
          rc::Rc,
};
use libc::stat;
use std::ops::Deref;

use std::collections::hash_map::{Entry, HashMap, RandomState};
use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, ReaddirOut, WriteOut},
    Request,
};
use dashmap::DashMap;

use lapin::{BasicProperties,
            ConnectionProperties,
            // message::DeliveryResult,
            options::*,
            // publisher_confirm::Confirmation,
            types::ShortString,
};

use tokio_amqp::*;
// use pinky_swear::PinkySwear;
#[allow(unused_imports)] use tracing::{info, warn, error, debug};
// use tracing_subscriber::fmt;
mod connection;

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;
const HELLO_INO: u64 = 2;
const HELLO_FILENAME: &str = "hello";
type Ino = u64;



#[derive(Clone)]
struct DirEntry {
    name: String,
    ino: Ino,
    typ: u32,
    parent_ino: Ino,
    // Names of child entries and their inodes. This does /not/
    // contain the "." and ".." entries of directory.
    children: DashMap<String, Ino, RandomState>,
    attr: libc::stat,
}

// type RegularFile = Vec<u8>;

struct DirectoryTable {
    map: DashMap<Ino, DirEntry, RandomState>,
    next_ino: AtomicU64,
}

impl DirEntry {
    fn root(uid: u32, gid: u32, mode: u32) -> Self {
        let r = Self {
            name: ".".to_string(),
            ino: ROOT_INO,
            typ: libc::DT_DIR as u32,
            parent_ino: ROOT_INO,
            attr: {
                let mut attr = unsafe {mem::zeroed::<libc::stat>() };
                attr.st_ino = ROOT_INO;
                attr.st_nlink = 1; // that's right, 1 not 2. The second link will be made when the directory table assembles itself
                attr.st_mode = libc::S_IFDIR | mode;
                attr.st_gid = gid;
                attr.st_uid = uid;
                attr
            },
            children: DashMap::with_hasher(RandomState::new()),
        };
        r.children.insert(".".to_string(), r.ino);
        r
    }

}

/// Main filesytem  handle. Representes  the connection to  the rabbit
/// server and the one-deep list of directories inside it.
pub(crate)
struct Rabbit {
    connection: lapin::Connection,
    routing_keys: DirectoryTable,
    uid: u32,
    gid: u32,
    ttl: Duration,
}

impl DirectoryTable {
    fn new(root: &DirEntry) -> Self {
        let map = DashMap::with_hasher(RandomState::new());
        let dir_names = vec![".."];
        let mut tbl = Self {
            map,
            next_ino: AtomicU64::new(ROOT_INO + 1),
        };
        tbl.map.insert(ROOT_INO, root.clone() );
        for name in dir_names.iter() {
            // If we can't make the root directory, the world is
            // broken. Panic immediatly.
            tbl.mkdir(name, ROOT_INO).unwrap();
        }
        // tbl.mkdir("abc");
        tbl
    }

    /// Make a directory in the root. Note that subdirectories are not
    /// allowed and so no parent is passed into this
    fn mkdir(&mut self, name: &str, parent_ino: Ino) -> Result<libc::stat, libc::c_int> {
        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        info!("Creating directory {} with inode {}", name, ino);
        let attr = match self.map.entry(ino) {
            dashmap::mapref::entry::Entry::Occupied(..) => panic!("duplicate inode error"),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let mut attr  = unsafe { mem::zeroed::<libc::stat>() };
                attr.st_ino = ino;
                attr.st_nlink = if name != "." {2} else {0};
                attr.st_mode = libc::S_IFDIR | 0o700;
                attr.st_blocks = 8;
                attr.st_size = 4096;
                let dir = DirEntry {
                    name: name.to_string(),
                    ino,
                    parent_ino: ROOT_INO,
                    typ: libc::DT_DIR as u32,
                    attr,
                    children: DashMap::with_hasher(RandomState::new()),
                };
                info!("Directory {} has {} children", dir.name, dir.children.len());
                // Add the default child entries pointing to the itself and to its parent
                dir.children.insert(".".to_string(), dir.ino);
                dir.children.insert("..".to_string(), parent_ino);
                entry.insert(dir);
                attr
            }
        };
        self.map.entry(ROOT_INO).and_modify(|root| root.attr.st_nlink +=1);
        self.map.get_mut(&ROOT_INO).unwrap().children.insert(name.to_string(), ino);
        info!("Filesystem contains {} directories", self.map.len());
        Ok(attr)
    }

    fn iter_dir<'a>(&'a self, dir: &'a DirEntry) -> impl Iterator<Item = (u64, DirEntry) > + '_ {
        use dashmap::try_result::TryResult;
        use std::iter;



        debug!("Iterating over {} with {} children", dir.name, dir.children.len() );

        // DashMap interator iterates over the values of the map,
        // which are inodes in this case.
        dir.children.iter().filter_map(move |ino| {
            match self.map.try_get(&ino) {
                TryResult::Present(entry) => {
                    let mut child = entry.clone();
                    if  child.ino == dir.ino {
                        child.name = ".".to_string()
                    } else if child.ino == dir.parent_ino {
                        child.name = "..".to_string()
                    }
                    Some((ino.clone(), child ))
                },
                TryResult::Absent => None,
                TryResult::Locked => None,
            }
        })
    }
}

impl Rabbit {
    pub
    async fn new(addr: &str) -> Rabbit {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let root = DirEntry::root(uid, gid, 0o700);

        Rabbit {
            connection: connection::get_connection(addr,
                                                   ConnectionProperties::default().with_tokio()).await.unwrap(),
            uid,
            gid,
            ttl: TTL,
            routing_keys: DirectoryTable::new(&root),
        }
    }

    fn fill_dir_attr(&self, attr: &mut FileAttr) {
        attr.ino(ROOT_INO);
        attr.mode(libc::S_IFDIR as u32 | 0o555);
        attr.nlink(2);
        attr.uid(self.uid);
        attr.gid(self.gid);
    }

    fn fill_file_attr(&self, attr: &mut FileAttr) {
        attr.ino(HELLO_INO);
        attr.size(0);// files always appear empty
        attr.mode(libc::S_IFREG as u32 | 0o666);
        attr.nlink(1);
        attr.uid(self.uid);
        attr.gid(self.gid);
    }

    pub
    async fn lookup(&self, req: &Request, op: op::Lookup<'_>) -> io::Result<()> {

        info!("Doing lookup of {:?} in parent inode {}", op.name(), op.parent());

        use dashmap::mapref::entry::Entry;


        match self.routing_keys.map.entry(op.parent()) {
            Entry::Vacant(..) => {return req.reply_error(libc::ENOENT);},
            Entry::Occupied(entry) => {
                let parent = entry.get();
                let mut out = EntryOut::default();
                out.ttl_attr(self.ttl);
                out.ttl_entry(self.ttl);

                // The name is a [u8] (i.e. a `char*`), so we have to cast it to unicode
                let name = match op.name().to_str() {
                    Some(name) => name,
                    None => {return req.reply_error(libc::EINVAL);}
                };

                let ino = match parent.children.get(&name.to_string()) {
                    Some(ino) => *ino,
                    None => {return req.reply_error(libc::ENOENT);},
                };
                info!("Found inode {} for {}", ino, name);

                match self.routing_keys.map.entry(ino) {
                    Entry::Vacant(..) => { return req.reply_error(libc::ENOENT);},
                    Entry::Occupied(entry) => {
                        let dir = entry.get();
                        out.ino(dir.ino);
                        fill_attr(out.attr(), &dir.attr);
                        req.reply(out)
                   },
                }
            }
        }
    }

    pub
    async fn getattr(&self, req: &Request, op: op::Getattr<'_>) -> io::Result<()> {
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
        fill_attr(out.attr(), &dir.attr);
        out.ttl(self.ttl);
        debug!("getattr for {}: {:?}", dir.name, StatWrap::from(dir.attr));
        req.reply(out)
    }

    pub
    async fn read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        info!("Reading {} bytes from inode {}", op.size(), op.ino() );
        match op.ino() {
            HELLO_INO => (),
            ROOT_INO => return req.reply_error(libc::EISDIR),
            _ => return req.reply_error(libc::ENOENT),
        }

        let data: &[u8] = &[]; // Files are always empty

        req.reply(data)
    }

    fn dir_entries(&self) -> impl Iterator<Item = (u64, DirEntry)> + '_ {
        self.routing_keys.map.iter().filter_map(|ent| {
            // let i = ent.key();
            let i:u64 = *ent.key();
            let dir = ent.value();
            debug!("Yielding directory {}", dir.name);
            if dir.name != "." || dir.name!=".." {Some((i, dir.clone()))} else {None}
            // (i as u64, ent)
        })
    }

    pub
    async fn readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        info!("Reading directory {} with offset {}", op.ino(), op.offset());

        use dashmap::try_result::TryResult;

        let dir = match self.routing_keys.map.try_get(&op.ino()) {
            TryResult::Present(entry) => entry,
            TryResult::Absent => {return req.reply_error(libc::ENOENT);}
            TryResult::Locked => {return req.reply_error(libc::ENOENT);}
        };

        debug!("Looking for directory {} in parent {}", dir.ino, dir.parent_ino);


        // Otherwise we are reading '.', so list all the directories.
        // There are no top level files.
        let mut out = ReaddirOut::new(op.size() as usize);

        for (i, entry) in self.routing_keys.iter_dir(&dir).skip(op.offset() as usize) {
            info!("Found directory entry {} in {}", entry.name, op.ino() );
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

    pub
    async fn mkdir(&mut self, req: &Request, op: op::Mkdir<'_>) -> io::Result<()> {
        let parent_ino = op.parent();
        let name = op.name();
        info!("Creating directory {:?} in parent {}", name, parent_ino);
        let mut out = EntryOut::default();
        let str_name = match name.to_str() {
            Some(s) => s,
            None => { error!("Invalid filename"); return req.reply_error(libc::EINVAL); }
        };
        let stat = match self.routing_keys.mkdir(str_name, ROOT_INO) {
            Ok(attr) => attr,
            _ => {return req.reply_error(libc::EEXIST); }
        };
        // out.attr().ino(stat.st_ino);
        // out.attr().mode(libc::S_IFDIR | 0x700 as u32);
        fill_attr(out.attr(), &stat);
        out.ino(stat.st_ino);
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        // self.fill_dir_attr(out.attr());
        info!("New directory has stat {:?}", StatWrap::from(stat) );
        req.reply(out)
    }

    pub
    async fn write<T>(&self, req: &Request, op: op::Write<'_>, mut data: T) -> io::Result<()>
    where
        T: BufRead + Unpin,
    {

        info!("Attempting write to inode {:?}", op.ino() );

        let pub_opts = BasicPublishOptions{mandatory: true, immediate:false};
        let props = BasicProperties::default()
            .with_content_type(ShortString::from("utf8"));
        let channel = self.connection.create_channel().await.unwrap();

        let _offset = op.offset() as usize;
        let size = op.size() as usize;

        let mut content = vec!();
        content.resize(size, 0);
        data.read_to_end(&mut content);

        let routing_key = HELLO_FILENAME;

        info!(exchange="", routing_key=routing_key, "Publishing message");

        let confirm  = channel.basic_publish("",
                                             routing_key,
                                             pub_opts,
                                             content,
                                             props
        ).await;

        // Setup the reply
        let mut out = WriteOut::default();
        out.size(op.size());
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
        StatWrap{ stat:s }
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
