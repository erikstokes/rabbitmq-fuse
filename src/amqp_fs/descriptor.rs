// use std::borrow::BorrowMut;
use std::io::{self, Write, BufRead, BufWriter};
use core::borrow::BorrowMut;
use bytes::{BytesMut, BufMut};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64,  Ordering};

use lapin::Promise;
use tokio_util::codec::{AnyDelimiterCodec, Encoder, Decoder};
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
    line_buf: RwLock< AnyDelimiterCodec >,
    byte_buf: BytesMut,
    // waiting_confirms:  Vec<Mutex<PromiseChain<PublisherConfirm> > >,
    flags: u32,
}

pub(crate)
struct LinePublishOptions {
    /// Block after each line, waiting for the confirm
    sync: bool,

    /// Also publish partial lines, not ending in the delimiter
    allow_partial: bool,
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

        let out = Self {
            fh,
            channel: connection.create_channel().await.unwrap(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            line_buf: RwLock::new(AnyDelimiterCodec::new_with_max_length( b"\n".to_vec(),
                                                                          vec!(),
                                                                          (1<<27)*128)),
            byte_buf: BytesMut::new(),
            // waiting_confirms: Vec::new(),
            flags,
        };

        out.channel.confirm_select(ConfirmSelectOptions { nowait: false }).await.expect("Set confirm");
        out
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
    fn publish_lines(&mut self, opts: LinePublishOptions) -> usize {
        debug!("splitting into lines and publishing");
        let mut cur = self.line_buf.write().await;

        // let mut line = vec!();
        let mut written = 0;
        loop {
            let confirm = match  cur.decode(&mut self.byte_buf) {
                Ok( Some( line )) => {
                    if line.is_empty() {
                        continue;
                    }
                    let line_len = line.len() + 1; // +1 for the newline, which we consume but don't write
                    debug!("Found line with {} bytes", line_len);
                    written += line_len;
                    self.basic_publish(&line.to_vec()).await
                }
                // Incomplete frame, no newline yet
                Ok( None ) => {break;},
                Err(..) => {error!("Unable to parse input buffer"); break;}
            };

            if opts.sync {
                info!("Sync enabled. Blocking for confirm");
                match confirm.wait() {
                    Ok(..) => {} // Everything is okay!
                    Err(..) => {return written;} // We at least wrote some stuff, right.. write?
                }
            }
        }
        written
    }

    pub
    async fn write_buf<T>(&mut self, mut buf: T) -> Result<usize, std::io::Error>
    where T: BufRead + Unpin {

        // Read the input buffer into our internal line buffer and
        // then publish the results. The amount read is the amount
        // pushed into the internal buffer, not the amount published
        // since incomplete lines can be held for later writes
        let prev_len = self.byte_buf.len();
        self.byte_buf.extend_from_slice(buf.fill_buf().unwrap());
        let read_bytes = self.byte_buf.len() - prev_len;
        buf.consume(read_bytes);
        debug!("Writing {} bytes into handle buffer", read_bytes);

        let sync = self.is_sync();
        let written = self.publish_lines(LinePublishOptions{sync, allow_partial:false}).await;
        debug!("line publisher published {}/{} bytes. {} remain in buffer", written, read_bytes, self.byte_buf.len());
        Ok(read_bytes)
    }

    async fn wait_for_confirms(&self) -> Result<(), std::io::Error> {
        debug!("Waiting for pending confirms");
        let returned = self.channel.wait_for_confirms().await;
        debug!("Recieved returned messages");
        match returned {
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
        // for conf in &self.waiting_confirms {
        //     debug!("Waiting on confirm");
        //     if let Ok( mut ret ) = conf.lock().expect("conf lock").wait() {
        //         match ret.wait().expect("get conf") {
        //             Confirmation::Ack(_) => {debug!("Publish ACK recieved");},
        //             Confirmation::Nack(_) => {
        //                 error!("Got back NACK for message");
        //                 return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to get confirms"));
        //             },
        //             Confirmation::NotRequested => {debug!("Publish confirm not requested");},
        //         }
        //     } else {
        //         return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to get confirms"));
        //     }
        // }
        Ok(())
    }

    pub
    async fn sync(&mut self, allow_partial: bool) -> Result<(), std::io::Error>{
        // let mut cur = self.line_buf.write().await;
        // TODO: Flush incomplete lines from buffer
        debug!("Closing descriptor {}", self.fh);
        debug!("Publishing buffered data");
        self.publish_lines(LinePublishOptions{sync:true, allow_partial}).await;
        let out = self.wait_for_confirms().await;
        debug!("Buffer flush complete");
        out
    }

    pub
    async fn release(&mut self) -> Result<(), std::io::Error> {
        // Flush the last partial line before the file is dropped
        self.sync(false).await.ok();
        self.sync(true).await.ok();
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
