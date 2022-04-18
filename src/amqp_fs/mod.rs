
use std::{io::{self, BufRead},
          os::unix::prelude::*,
          time::Duration,
};
use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, ReaddirOut, WriteOut},
    Operation,
    Request,
};
use dashmap::DashMap;

use lapin::{BasicProperties,
            ConnectionProperties,
            message::DeliveryResult,
            options::*,
            publisher_confirm::Confirmation,
            types::{FieldTable, ShortString},
};

use tokio_amqp::*;
use pinky_swear::PinkySwear;
use tracing::{info, warn, error, instrument};

const TTL: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const ROOT_INO: u64 = 1;
const HELLO_INO: u64 = 2;
const HELLO_FILENAME: &str = "hello";

mod connection;

pub(crate)
struct Rabbit {
    connection: lapin::Connection,
    entries: Vec<DirEntry>,
    uid: u32,
    gid: u32,
}

struct DirEntry {
    name: &'static str,
    ino: u64,
    typ: u32,
}

type RegularFile = Vec<u8>;
// type Ino = u64;

// struct InodeTable {
//     map: DashMap<Ino, INode, RandomState>,
//     next_ino: AtomicU64,
// }

impl Rabbit {
    pub
    async fn new(addr: &str) -> Self {
        // Add 3 entries by default. The normal "." and ".." and on
        // test entry named 'hello'
        let mut entries = Vec::with_capacity(3);
        entries.push(DirEntry {
            name: ".",
            ino: ROOT_INO,
            typ: libc::DT_DIR as u32,
        });
        entries.push(DirEntry {
            name: "..",
            ino: ROOT_INO,
            typ: libc::DT_DIR as u32,
        });
        entries.push(DirEntry {
            name: HELLO_FILENAME,
            ino: HELLO_INO,
            typ: libc::DT_REG as u32,
        });

        Self {
            connection: connection::get_connection(addr,
                           ConnectionProperties::default().with_tokio()).await.unwrap(),
            entries,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }

    fn fill_root_attr(&self, attr: &mut FileAttr) {
        attr.ino(ROOT_INO);
        attr.mode(libc::S_IFDIR as u32 | 0o555);
        attr.nlink(2);
        attr.uid(self.uid);
        attr.gid(self.gid);
    }

    fn fill_routing_attr(&self, attr: &mut FileAttr) {
        attr.ino(HELLO_INO);
        attr.size(0);// files always appear empty
        attr.mode(libc::S_IFREG as u32 | 0o666);
        attr.nlink(1);
        attr.uid(self.uid);
        attr.gid(self.gid);
    }

    pub
    async fn lookup(&self, req: &Request, op: op::Lookup<'_>) -> io::Result<()> {

        info!("Doing lookup of {:?}", op);

        match op.parent() {
            ROOT_INO if op.name().as_bytes() == HELLO_FILENAME.as_bytes() => {
                info!("You are looking up 'hello'");
                let mut out = EntryOut::default();
                self.fill_routing_attr(out.attr());
                out.ino(HELLO_INO);
                out.ttl_attr(TTL);
                out.ttl_entry(TTL);
                req.reply(out)
            }
            _ => req.reply_error(libc::ENOENT),
        }
    }

    pub
    async fn getattr(&self, req: &Request, op: op::Getattr<'_>) -> io::Result<()> {
        let fill_attr = match op.ino() {
            ROOT_INO => Self::fill_root_attr,
            HELLO_INO => Self::fill_routing_attr,
            _ => return req.reply_error(libc::ENOENT),
        };

        let mut out = AttrOut::default();
        fill_attr(self, out.attr());
        out.ttl(TTL);

        req.reply(out)
    }

    pub
    async fn read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        match op.ino() {
            HELLO_INO => (),
            ROOT_INO => return req.reply_error(libc::EISDIR),
            _ => return req.reply_error(libc::ENOENT),
        }

        let data: &[u8] = &[]; // Files are always empty

        // let offset = op.offset() as usize;
        // if offset < HELLO_CONTENT.len() {
        //     let size = op.size() as usize;
        //     data = &HELLO_CONTENT[offset..];
        //     data = &data[..std::cmp::min(data.len(), size)];
        // }

        req.reply(data)
    }

    fn dir_entries(&self) -> impl Iterator<Item = (u64, &DirEntry)> + '_ {
        self.entries.iter().enumerate().map(|(i, ent)| {
            let offset = (i + 1) as u64;
            (offset, ent)
        })
    }

    pub
    async fn readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        if op.ino() != ROOT_INO {
            return req.reply_error(libc::ENOTDIR);
        }

        let mut out = ReaddirOut::new(op.size() as usize);

        for (i, entry) in self.dir_entries().skip(op.offset() as usize) {
            let full = out.entry(
                entry.name.as_ref(), //
                entry.ino,
                entry.typ,
                i + 1,
            );
            if full {
                break;
            }
        }

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
