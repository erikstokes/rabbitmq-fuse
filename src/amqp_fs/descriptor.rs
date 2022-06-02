//! Tools for tracking open files and writing data itno them. The
//! mechanics of publishing to the rabbit server are managed here

use bytes::{BufMut, BytesMut};
use lapin::types::FieldTable;
use core::borrow::BorrowMut;
use std::io::{self, BufRead, BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lapin::publisher_confirm::Confirmation;
use lapin::Promise;
use tokio_util::codec::{AnyDelimiterCodec, Decoder, Encoder};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use tokio::{io::AsyncWrite, sync::RwLock};

use dashmap::DashMap;
use lapin::{
    message::BasicReturnMessage, options::*, publisher_confirm::PublisherConfirm,
    types::ShortString, BasicProperties, Channel, Connection, PromiseChain,
};
use std::collections::hash_map::RandomState;

use crate::amqp_fs::options::{PublishStyle, UnparsableStyle};

use super::options::{WriteOptions, LinePublishOptions};

use std::collections::{btree_map, BTreeMap};

use lapin::types::*;

use serde::{Deserialize, Serialize, Deserializer};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
// #[serde(remote="AMQPValue")]
pub enum MyAMQPValue {
    Boolean(Boolean),
    ShortShortInt(ShortShortInt),
    ShortShortUInt(ShortShortUInt),
    ShortInt(ShortInt),
    ShortUInt(ShortUInt),
    LongInt(LongInt),
    LongUInt(LongUInt),
    LongLongInt(LongLongInt),
    Float(Float),
    Double(Double),
    DecimalValue(DecimalValue),
    ShortString(ShortString),
    LongString(LongString),
    FieldArray(FieldArray),
    Timestamp(Timestamp),
    MyFieldTable(MyFieldTable),
    ByteArray(ByteArray),
    Void,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct MyFieldTable(BTreeMap<ShortString, MyAMQPValue>);

/// File Handle number
pub(crate) type FHno = u64;

/// Errors that can return from writing to the file.
pub enum WriteError {
    /// Header mode was specified, but we couldn't parse the line
    ParsingError(usize),

    /// RabbitMQ returned some error on publish. Could some from a
    /// previous publish but be returned by the current one
    RabbitError(lapin::Error, usize),

    /// The file's internal buffer filled without encountering a
    /// newline.
    ///
    /// Once emitted, no more writes will be possible. The
    /// file should be closed
    BufferFull(usize),

    /// A previous message failed the publisher confirm check.
    ///
    /// Either a NACK was returned, or the entire message was returned
    /// as unroutable
    ConfirmFailed(usize),
}


pub(in crate::amqp_fs) struct FileHandle {
    /// File handle id
    pub(crate) fh: FHno,
    /// RabbitMQ channel the file will publish to on write
    channel: Channel,

    // routing info
    exchange: String,
    routing_key: String,

    opts: WriteOptions,

    // Inner line buffer
    line_buf: RwLock<AnyDelimiterCodec>,
    byte_buf: BytesMut,

    can_write: bool,
    // waiting_confirms:  Vec<Mutex<PromiseChain<PublisherConfirm> > >,
    flags: u32, // open(2) flags
    num_writes: u64,
}

/// Table of open file descriptors
pub(in crate::amqp_fs) struct FileHandleTable {
    /// Mapping of inode numbers to file handle. Maybe accessed
    /// accross threads, but only one thread should hold a file handle
    /// at a time.
    pub(crate) file_handles: DashMap<FHno, FileHandle>,

    /// Atomically increment this to get the next handle number
    next_fh: AtomicU64,

    /// Max size (in bytes) of each file's internal buffer. When
    /// filled, the file can't be written to until the data is
    /// flushed.
    max_buf_size: usize,
}

impl FileHandle {

    /// Create a new file handle, which will publish to the given
    /// connection, using the exchange and routing_key
    ///
    /// Generally do not call this yourself. Instead use [FileHandleTable::insert_new_fh]
    /// # Panics
    /// Panics if the connection is unable to open the channel
    pub(crate) async fn new(
        fh: FHno,
        connection: &Connection,
        exchange: &str,
        routing_key: &str,
        flags: u32,
        opts: WriteOptions,
    ) -> Self {
        debug!(
            "Creating file handle {} for {}/{}",
            fh, exchange, routing_key
        );

        let out = Self {
            fh,
            channel: connection.create_channel().await.unwrap(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            line_buf: RwLock::new(AnyDelimiterCodec::new_with_max_length(
                b"\n".to_vec(),
                vec![],
                (1 << 27)*128*2,
            )),
            byte_buf: BytesMut::with_capacity(8000),
            opts,
            can_write: true,
            // waiting_confirms: Vec::new(),
            flags,
            num_writes: 0
        };

        debug!("File open sync={}", out.is_sync());

        out.channel
            .confirm_select(ConfirmSelectOptions { nowait: false })
            .await
            .expect("Set confirm");
        out
    }


    /// Returns true if each line will be confirmed as it is published
    fn is_sync(&self) -> bool {
        (self.flags & libc::O_SYNC as u32) != 0
    }

    /// Publish one line of input, returning a promnise for the publisher confirm.
    ///
    /// Returns the number of byte published, or any error returned by
    /// [lapin::Channel::basic_publish]. Note that the final newline is not
    /// publishied, so the return value may be one short of what you
    /// expect.
    async fn basic_publish(&self, line: &[u8], force_sync: bool) -> Result<usize, WriteError> {
        let pub_opts = BasicPublishOptions {
            mandatory: true,
            immediate: false,
        };
        use std::str;
        // let line = r#"{"a":1,"b":2}"#.as_bytes();
        debug!("publishing line {:?}", String::from_utf8_lossy(line));

        // let mut de = serde_json::Deserializer::from_slice(line);
        // let headers = FieldTable::deserialize(&mut de).unwrap();

        let headers = match &self.opts.line_opts.publish_in {
            PublishStyle::Header => {
                if let Ok(my_headers) = serde_json::from_slice::<MyFieldTable>(line){
                    trace!("my headers are {:?}", serde_json::to_string(&my_headers).unwrap());
                    let headers : FieldTable = my_headers.into();
                    headers
                } else {
                    error!("Failed to parse JSON line {}", String::from_utf8_lossy(line));
                    match &self.opts.line_opts.handle_unparsable {
                        UnparsableStyle::Skip => {
                            warn!("Skipping unparsable message, but reporting success");
                            return Ok(line.len()); // A LIE!
                        },
                        UnparsableStyle::Error => {
                            error!("Returning error for unparsed line");
                            return Err(WriteError::ParsingError(0));
                        },
                        UnparsableStyle::Key => {
                            let mut headers = FieldTable::default();
                            let val = AMQPValue::ByteArray(ByteArray::from(line));
                            // The CLI parser requires this field if
                            // the style is set to "key", so unwrap is
                            // safe
                            headers.insert(
                                self.opts.line_opts.parse_error_key
                                    .as_ref()
                                    .unwrap()
                                    .to_string()
                                    .into(), // Wow, that's a lot of conversions
                                val);
                            headers
                        }
                    }
                }
            },
            PublishStyle::Body => FieldTable::default()
        } ;

        trace!("headers are {:?}", headers);
        let props = BasicProperties::default()
            .with_content_type(ShortString::from("utf8"))
            .with_headers( headers )
            ;

        debug!(
            "Publishing {} bytes to exchange={} routing_key={}",
            line.len(),
            self.exchange,
            self.routing_key
        );
        match self.channel.basic_publish(
            &self.exchange,
            &self.routing_key,
            pub_opts,
            match &self.opts.line_opts.publish_in {
                PublishStyle::Header => Vec::<u8>::with_capacity(0),
                PublishStyle::Body => line.to_vec()
            },
            props,
        ).await {
            Ok(confirm)=>  {
                debug!("Publish succeeded. Sent {} bytes", line.len());
                if force_sync || self.is_sync() {
                    info!("Sync enabled. Blocking for confirm");
                    match confirm.await {
                        Ok(..) => Ok(line.len()), // Everything is okay!
                        Err(err) => Err(WriteError::RabbitError(err, 0)),     // We at least wrote some stuff, right.. write?
                    }
                } else {
                    Ok(line.len())
                }
            }
            Err(err) => Err(WriteError::RabbitError(err, 0))
        }
    }

    /// Split the internal buffer into lines and publish them. Returns
    /// the number of bytes published
    ///
    /// Only complete lines will be published, unless
    /// [LinePublishOptions::allow_partial] is true, in which case all
    /// buffered data will be published.
    async fn publish_lines(&mut self, allow_partial: bool, force_sync: bool) -> Result<usize, WriteError> {
        debug!("splitting into lines and publishing partial: {}, sync: {}",
               allow_partial, force_sync);
        let mut cur = self.line_buf.write().await;

        // let mut line = vec!();
        let mut written = 0;
        // partial lines can only occur at the end of the buffer, so
        // if we want to flush everything, just append a newline
        if allow_partial {
            self.byte_buf.extend_from_slice(b"\n");
        }
        loop {
            match cur.decode(&mut self.byte_buf) {
                // Found a complete line
                Ok(Some(line)) => {
                    if line.is_empty() {
                        continue;
                    }
                    match self.basic_publish(&line.to_vec(), force_sync).await {
                        Ok(len) => written += (len +1), // +1 for the newline
                        Err(mut err) => {
                            error!("basic publish did not succeed. Have written {}/{} bytes",
                                   written, line.len());
                            err.add_written(written);
                            return Err(err);
                        }

                    }
                }
                // Incomplete frame, no newline yet
                Ok(None) => {
                    break;
                    // if !allow_partial {
                    //     break;
                    // }
                    // // Should never fail since we know from above that the bytes exist
                    // if let Ok(Some(rest)) = cur.decode_eof(&mut self.byte_buf) {
                    //     match self.basic_publish(&rest.to_vec(), force_sync).await {
                    //         Ok(len) => written += (len+1);
                    //     } else {
                    //         break;
                    //     }
                    // } else {
                    //     break;
                    // }
                }
                Err(..) => {
                    error!("Unable to parse input buffer");
                    break;
                }
            };
        }

        Ok(written)
    }

    /// Write a buffer recieved from the kernel into the descriptor
    /// and return the number of bytes written
    ///
    /// Any complete lines (ending in \n) will be published
    /// immediatly. The rest of the data will be buffered. If the
    /// maxumim buffer size is excceded, this write will succed but
    /// future writes will will fail
    pub async fn write_buf<T>(&mut self, mut buf: T) -> Result<usize, WriteError>
    where
        T: BufRead + Unpin
    {
        debug!("Writing with options {:?}", self.opts);
        if !self.can_write {
            // return Err(std::io::Error::new(std::io::ErrorKind::WriteZero,
            //                                "Buffer full"));
            return Err(WriteError::BufferFull(0));
        }
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
        let result = self
            .publish_lines(sync, false).await;
        let pub_bytes = match result {
            Ok(written) => written,
            Err(ref err) => {
                error!("Line publish failed, but wrote {} bytes", err.written());
                err.written()
            }
        };
        debug!(
            "line publisher published {}/{} bytes. {} remain in buffer",
            pub_bytes,
            read_bytes,
            self.byte_buf.len()
        );
        self.byte_buf.reserve(pub_bytes);
        debug!("Buffer capacity {}", self.byte_buf.capacity());
        if self.opts.max_buffer_bytes >0 && self.byte_buf.len() > self.opts.max_buffer_bytes {
            self.can_write = false;
        }
        self.num_writes += 1;


        if self.num_writes % self.opts.max_unconfirmed == 0 {
            debug!("Wrote a lot, waiting for confirms");
            if let Err(err) = self.wait_for_confirms().await {
                return Err(err);
            }
        }
        match result {
            // We published some data with no errors and stored the
            // rest in a buffer, so we can report the entire amount
            // buffered as "written"
            Ok(_) => Ok(read_bytes),
            // If there was an error, we published some bytes and
            // maybe buffered the rest. Report only the published
            // bytes as "written" and remove the rest of the input
            // from the buffer
            Err(err) => {
                // The published bytes are already consumed from the
                // buffer, but the other new bytes we buffered might
                // be bad, so remove them from the buffer and make
                // people write them back. Since we know the buffer
                // didn't contain a complete line, we know we ate all
                // the old bytes and some new ones. Therefor the
                // buffer only contains newly written bytes, which we
                // don't want to keep.
                warn!("Truncating buffer");
                self.byte_buf.truncate(0);
                Err(err)
            }
        }
    }


    /// Wait until all requested publisher confirms have returned
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        debug!("Waiting for pending confirms");
        let returned = self.channel.wait_for_confirms().await;
        debug!("Recieved returned messages");
        match returned {
            Ok(all_confs) => {
                if all_confs.is_empty() {
                    debug!("No returns. Everything okay");
                } else {
                    // Some messages were returned to us
                    error!("{} messages not confirmed", all_confs.len());
                    for conf in all_confs {
                        conf.ack(BasicAckOptions::default())
                            .await
                            .expect("Return ack");
                    }
                    return Err(WriteError::ConfirmFailed(0));
                }
            }
            Err(err) => {
                return Err(WriteError::RabbitError(err, 0));
            }
        }

        Ok(())
    }

    /// Publish all complete buffered lines and, if `allow_partial` is
    /// true, incomplete lines as well
    pub async fn sync(&mut self, allow_partial: bool) -> Result<(), WriteError> {
        // let mut cur = self.line_buf.write().await;
        // TODO: Flush incomplete lines from buffer
        debug!("Syncing descriptor {}", self.fh);
        debug!("Publishing buffered data");
        if let Err(err) =  self.publish_lines(true, allow_partial).await {
            error!("Couldn't sync file buffer");
            return Err(err);
        }
        let out = self.wait_for_confirms().await;
        debug!("Buffer flush complete");
        out
    }


    /// Release the descriptor from the filesystem.
    ///
    /// The fully syncronizes the file, publishing all complete and
    /// incomplete lines, close the RabbitMQ channel and clears (but
    /// does not drop) the internal buffer.
    pub async fn release(&mut self) -> Result<(), std::io::Error> {
        // Flush the last partial line before the file is dropped
        self.sync(false).await.ok();
        self.sync(true).await.ok();
        self.channel.close(0, "File handle closed").await.ok();
        self.byte_buf.clear();
        self.byte_buf.truncate(0);
        debug!("Channel closed");
        Ok(())
    }
}

impl FileHandleTable {
    /// Create a new file table.  Created files will have the specificed maximum buffer size
    pub fn new(buffer_size: usize) -> Self {
        Self {
            file_handles: DashMap::with_hasher(RandomState::new()),
            next_fh: AtomicU64::new(0),
            max_buf_size: buffer_size,
        }
    }


    /// Get a valid handle number for a new file
    fn next_fh(&self) -> FHno {
        self.next_fh.fetch_add(1, Ordering::SeqCst)
    }

    /// Create a new open file handle with the givin flags and insert
    /// it into the table. Return the handle ID number for lookup
    /// later.
    ///
    /// Writing to the new file will publish messages on the given
    /// connection using `exchange` and `routing_key`.
    /// The file can be retrived later using [FileHandleTable::entry]
    pub async fn insert_new_fh(
        &self,
        conn: &lapin::Connection,
        exchange: &str,
        routing_key: &str,
        flags: u32,
        opts: &WriteOptions
    ) -> FHno {
        let fhno = self.next_fh();
        self.file_handles.insert(
            fhno,
            FileHandle::new(fhno, conn, exchange, routing_key, flags,
                            opts.clone()).await,
        );
        fhno
    }

    /// Get an open entry from the table, if it exits.
    ///
    /// Has the same sematics as [DashMap::entry]
    pub fn entry(&self, fh: FHno) -> dashmap::mapref::entry::Entry<FHno, FileHandle, RandomState> {
        self.file_handles.entry(fh)
    }

    /// Remove an entry from the file table.
    ///
    /// Note that this does not release the file.
    pub fn remove(&self, fh: FHno) {
        self.file_handles.remove(&fh);
    }
}
impl From<MyFieldTable> for FieldTable {
    fn from(tbl: MyFieldTable) -> Self {
        let mut out = FieldTable::default();
        for item in tbl.0.iter() {
            out.insert(item.0.clone(), item.1.clone().into())
        }
        out
    }
}



impl From<MyAMQPValue> for AMQPValue {
    fn from(val: MyAMQPValue) -> Self {
        match val {
            MyAMQPValue::Boolean(val) => AMQPValue::Boolean(val),
            MyAMQPValue::ShortShortInt(val) => AMQPValue::ShortShortInt(val),
            MyAMQPValue::ShortShortUInt(val) =>  AMQPValue::ShortShortUInt(val),
            MyAMQPValue::ShortInt(val)       =>  AMQPValue::ShortInt(val),
            MyAMQPValue::ShortUInt(val)      =>  AMQPValue::ShortUInt(val),
            MyAMQPValue::LongInt(val)        =>  AMQPValue::LongInt(val),
            MyAMQPValue::LongUInt(val)       =>  AMQPValue::LongUInt(val),
            MyAMQPValue::LongLongInt(val)    =>  AMQPValue::LongLongInt(val),
            MyAMQPValue::Float(val)          =>  AMQPValue::Float(val),
            MyAMQPValue::Double(val)         =>  AMQPValue::Double(val),
            MyAMQPValue::DecimalValue(val)   =>  AMQPValue::DecimalValue(val),
            MyAMQPValue::ShortString(val)    =>  AMQPValue::LongString(val.as_str().into()),
            MyAMQPValue::LongString(val)     =>  AMQPValue::LongString(val),
            MyAMQPValue::FieldArray(val)     =>  AMQPValue::FieldArray(val),
            MyAMQPValue::Timestamp(val)      =>  AMQPValue::Timestamp(val),
            MyAMQPValue::MyFieldTable(val)     =>  AMQPValue::FieldTable(val.into()),
            MyAMQPValue::ByteArray(val)      =>  AMQPValue::ByteArray(val),
            MyAMQPValue::Void                =>  AMQPValue::Void
        }
    }
}


impl WriteError {
    /// OS error code corresponding to the error, if there is one
    pub fn get_os_error(&self) -> Option<libc::c_int> {
        match self {
            Self::BufferFull(..) => Some(libc::ENOBUFS),
            Self::ConfirmFailed(..) => Some(libc::EIO),
            Self::ParsingError(..) => Some(libc::EIO),
            // There isn't an obvious error code for this, so let the
            // caller choose
            Self::RabbitError(..) => None,
        }
    }

    /// Number of bytes succesfully written before the error
    pub fn written(&self) -> usize {
        match self {
            Self::RabbitError(_err, size) => *size,
            Self::BufferFull(size) => *size,
            Self::ConfirmFailed(size) => *size,
            Self::ParsingError(size) => *size,
        }
    }

    /// Return the same error but reporting more data written
    pub fn add_written(&mut self, more: usize) -> &Self {
        match self {
            Self::RabbitError(_err, ref mut size) => *size += more,
            Self::BufferFull(ref mut size) => *size += more,
            Self::ConfirmFailed(ref mut size) => *size += more,
            Self::ParsingError(ref mut size) => *size += more
        }

        self

    }
}
