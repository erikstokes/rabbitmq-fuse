use std::io::{self, BufRead, Cursor};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64,  Ordering};

use lapin::Promise;
use lapin::publisher_confirm::Confirmation;
#[allow(unused_imports)] use tracing::{info, warn, error, debug, trace};

use tokio::{sync::RwLock,
            io::AsyncWrite,
};

use lapin::{Channel, Connection,
            PromiseChain,
            publisher_confirm::PublisherConfirm,
            BasicProperties,
            options::*,
            types::ShortString};
use dashmap::DashMap;
use std::collections::hash_map::RandomState;

pub(crate)
type FHno = u64; /// File Handle number

pub(crate)
struct FileHandle {
    pub(crate) fh: FHno, /// File handle id
    channel: Channel,
    exchange: String,
    routing_key: String,
    line_buf: RwLock< Cursor< Vec<u8>> >,
    waiting_confirms: Vec< Mutex<PublisherConfirm  > >,
    flags: u32,
}

pub(crate)
struct FileHandleTable {
    pub(crate)
    file_handles: DashMap<FHno, FileHandle>,
    next_fh: AtomicU64,
}


impl FileHandle {
    pub(crate)
    async fn new(fh: FHno, connection: &Connection, exchange: &str, routing_key: &str, flags: u32) -> Self {
        debug!("Creating file handle {} for {}/{}", fh, exchange, routing_key);

        Self {
            fh,
            channel: connection.create_channel().await.unwrap(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            line_buf: RwLock::new(Cursor::new( vec!())),
            waiting_confirms: Vec::new(),
            flags,
        }
    }

    fn is_sync(&self) -> bool {
       ( self.flags & libc::O_SYNC as u32) != 0
    }

    /// Publish one line of input, returning a promnise for the publisher confirm
    async
    fn basic_publish(&self, line: &Vec<u8> ) -> PromiseChain<PublisherConfirm> {
        let pub_opts = BasicPublishOptions{mandatory: true, immediate:false};
        let props = BasicProperties::default()
            .with_content_type(ShortString::from("utf8"));
        debug!("Publishing {} bytes to exchange={} routing_key={}",
               line.len(), self.exchange, self.routing_key
        );
        self.channel.basic_publish(&self.exchange,
                                   &self.routing_key,
                                   pub_opts,
                                   line.clone(),
                                   props.clone())
    }

    /// Slit the internal buffer into lines and publish them
    async
    fn publish_lines(&mut self, is_sync: bool) -> usize {
        debug!("splitting into lines and publishing");
        let mut cur = self.line_buf.write().await;

        let mut line = vec!();
        let mut written = 0;
        while cur.read_until(b'\n', &mut line).expect("Unable to read input buffer") != 0 {
            written += line.len();
            if *line.last().unwrap() != b'\n' {
                debug!("Not publishing partial line");
                let pos = cur.position();
                cur.set_position( pos - line.len() as u64);
                break;
            }
            debug!("Found line with {} bytes", line.len());

            let confirm = self.basic_publish(&line.clone()).await;
            line.clear();
            if is_sync {
                info!("Sync enabled. Blocking for confirm");
                match confirm.wait() {
                    Ok(..) => {} // Everything is okay!
                    Err(..) => {return written;} // We at least wrote some stuff, right.. write?
                }
            } else {
                if let Ok(conf) = confirm.await {
                    self.waiting_confirms.push(Mutex::new(conf));
                } else {
                    return written;
                }
            }
        }
        written
    }

    pub
    async fn write_buf<T>(&mut self, mut buf: T) -> Result<usize, std::io::Error>
    where T: BufRead + Unpin {

        {
            // Read the input buffer into our internal line buffer and
            // then publish the results. The outer braces make sure
            // the lock on the buffer is dropped before we try to read
            // it
            let mut cur = self.line_buf.write().await;
            let read_bytes = buf.read_to_end( cur.get_mut() ).unwrap();
            debug!("Writing {} bytes into handle buffer", read_bytes);
        }
        let sync = self.is_sync();
        let written = self.publish_lines(sync).await;
        Ok(written)
    }

    fn wait_for_confirms(&self) -> Result<(), std::io::Error> {
        debug!("Waiting for pending confirms");
        let returned = self.channel.wait_for_confirms();
        debug!("Recieved returned messages");
        match returned.wait() {
            Ok(all_confs) => {
                if all_confs.is_empty(){
                    debug!("No returns. Everything okay");
                } else {
                    // Some messages were returned to us
                    error!("{} messages not confirmed", all_confs.len());
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "Messages were returned"));
                }
            },
            Err(..) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to get confirms"));
            },
        }
        for conf in &self.waiting_confirms {
            debug!("Waiting on confirm");
            if let Ok( ret ) = conf.lock().expect("conf lock").wait() {
                match ret {
                    Confirmation::Ack(_) => {debug!("Publish ACK recieved");},
                    Confirmation::Nack(_) => {
                        error!("Got back back for message");
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to get confirms"));
                    },
                    Confirmation::NotRequested => {},
                }
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to get confirms"));
            }
        }
        Ok(())
    }

    pub
    async fn sync(&mut self) -> Result<(), std::io::Error>{
        // let mut cur = self.line_buf.write().await;
        // TODO: Flush incomplete lines from buffer
        debug!("Closing descriptor {}", self.fh);
        debug!("Publishing buffered data");
        self.publish_lines(true).await;
        let out = self.wait_for_confirms();
        debug!("Buffer flush complete");
        out
    }

    pub
    async fn release(&mut self) -> Result<(), std::io::Error> {
        self.sync().await.ok();
        self.channel.close(0, "File handle closed").await.ok();
        debug!("Channel closed");
        Ok(())
    }
}

impl FileHandleTable {
    pub
    fn new() -> Self {
        Self{
            file_handles: DashMap::with_hasher(RandomState::new()),
            next_fh: AtomicU64::new(0),
        }
    }

    fn next_fh(&self) -> FHno {
        self.next_fh.fetch_add(1, Ordering::SeqCst)
    }

    /// Create a new open file handle and insert it into the table.
    /// Return the handle ID number for lookup later.
    pub
    async fn insert_new_fh(&self,
                     conn: &lapin::Connection,
                     exchange: &str,
                     routing_key: &str,
                     flags: u32) -> FHno {
        let fhno = self.next_fh();
        self.file_handles.insert(
            fhno,
            FileHandle::new(fhno, conn, &exchange, routing_key, flags).await
        );
        fhno
    }
}
