//! Tools for tracking open files and writing data itno them. The
//! mechanics of publishing to the rabbit server are managed here

use std::io::BufRead;
use std::sync::atomic::{AtomicU64, Ordering};

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use async_trait::async_trait;

use tokio::sync::RwLock;

use dashmap::DashMap;
use lapin::{
     options::*,
    types::ShortString, BasicProperties, Channel, Connection,
};
use std::collections::hash_map::RandomState;

use super::buffer::Buffer;
use super::message::Message;
use super::options::{WriteOptions, LinePublishOptions};
use super::publisher::{Publisher, rabbit::RabbitPublisher};


/// File Handle number
pub(crate) type FHno = u64;

/// An error that occurs during parsing the line into headers
#[derive(Debug)]
pub struct ParsingError(pub usize);

/// Errors that can return from writing to the file.
#[derive(Debug)]
pub enum WriteError {
    /// Header mode was specified, but we couldn't parse the line
    ParsingError(ParsingError),

    /// Unable to connect to the publising endpoint
    EndpointConnectionError,

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

    /// Unable to open the file within the timeout
    TimeoutError(usize),
}


/// An open file
pub(in crate) struct FileHandle<T: Publisher> {
    /// File handle id
    #[doc(hidden)]
    pub(crate) fh: FHno,

    // /// RabbitMQ channel the file will publish to on write
    // #[doc(hidden)]
    // channel: Channel,

    // /// The RabbitMQ exchange lines will be published to
    // exchange: String,

    // /// The routing key lines will will be published to
    // routing_key: String,

    publisher: T,

    /// Options applied to all writes that happend to this descriptor.
    ///
    /// Options are specific to the file descriptor and not the file
    opts: WriteOptions,

    // Inner line buffer
    #[doc(hidden)]
    buffer: RwLock<Buffer>,

    #[doc(hidden)]
    flags: u32, // open(2) flags

    #[doc(hidden)]
    num_writes: RwLock<u64>,
}


#[async_trait]
pub(crate) trait FileTable {

    type Publisher: Publisher;

    /// Create a new open file handle with the givin flags and insert
    /// it into the table. Return the handle ID number for lookup
    /// later.
    ///
    /// Writing to the new file will publish messages on the given
    /// connection using `exchange` and `routing_key`.
    /// The file can be retrived later using [FileHandleTable::entry]
    async fn insert_new_fh (
        &self,
        routing_key: &str,
        flags: u32,
        opts: &WriteOptions,
    ) -> Result<FHno, WriteError>;

    /// Get an open entry from the table, if it exits.
    ///
    /// Has the same sematics as [DashMap::entry]
    fn entry(&self, fh: FHno) -> dashmap::mapref::entry::Entry<FHno, FileHandle<Self::Publisher>, RandomState>;

    /// Remove an entry from the file table.
    ///
    /// Note that this does not release the file.
    fn remove(&self, fh: FHno);
}

impl<P: Publisher> FileHandle<P> {
    /// Create a new file handle, which will publish to the given
    /// connection, using the exchange and routing_key
    ///
    /// Generally do not call this yourself. Instead use [FileHandleTable::insert_new_fh]
    /// # Panics
    /// Panics if the connection is unable to open the channel
    pub(crate) fn new (fh: FHno,
                       publisher: P,
                       flags: u32,
                       opts: WriteOptions) -> Self {
        Self {buffer: RwLock::new(Buffer::new(8000, &opts)),
              fh,
              publisher,
              flags,
              opts,
              num_writes: RwLock::new(0),
        }
    }

    /// Returns true if each line will be confirmed as it is published
    fn is_sync(&self) -> bool {
        (self.flags & libc::O_SYNC as u32) != 0
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
        T: BufRead + Unpin + std::marker::Send,
    {
        debug!("Writing with options {:?}", self.opts);

        if self.buffer.read().await.is_full() {
            return Err(WriteError::BufferFull(0));
        }

        // Read the input buffer into our internal line buffer and
        // then publish the results. The amount read is the amount
        // pushed into the internal buffer, not the amount published
        // since incomplete lines can be held for later writes
        let read_bytes = self.buffer.write().await.extend(buf.fill_buf().unwrap());
        buf.consume(read_bytes);
        debug!("Writing {} bytes into handle buffer", read_bytes);

        let sync = self.is_sync();
        let result = self.publish_lines(sync, false).await;
        let pub_bytes = match result {
            Ok(written) => written,
            Err(ref err) => {
                error!("Line publish failed, but wrote {} bytes", err.written());
                err.written()
            }
        };
        debug!(
            "line publisher published {}/{} bytes",
            pub_bytes, read_bytes,
        );
        self.buffer.write().await.reserve(pub_bytes);
        *self.num_writes.get_mut() += 1;

        if *self.num_writes.read().await >= self.opts.max_unconfirmed {
            debug!("Wrote a lot, waiting for confirms");
            *self.num_writes.write().await = 0;
            self.publisher.wait_for_confirms().await?
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
                self.buffer.write().await.truncate(0);
                Err(err)
            }
        }
    }

    /// Split the internal buffer into lines and publish them. Returns
    /// the number of bytes published without error.
    ///
    /// Only complete lines will be published, unless `allow_partial`
    /// is true, in which case all buffered data will be published.
    async fn publish_lines(
        &self,
        allow_partial: bool,
        force_sync: bool,
    ) -> Result<usize, WriteError> {
        debug!(
            "splitting into lines and publishing partial: {}, sync: {}",
            allow_partial, force_sync
        );
        let mut cur = self.buffer.write().await;

        // let mut line = vec!();
        let mut written = 0;
        // partial lines can only occur at the end of the buffer, so
        // if we want to flush everything, just append a newline
        if allow_partial {
            cur.extend(b"\n");
        }
        loop {
            match cur.decode() {
                // Found a complete line
                Ok(Some(line)) => {
                    if line.is_empty() {
                        written += 1; // we 'wrote' a newline
                        continue;
                    }
                    match self.publisher.basic_publish(&line,
                                                       force_sync||self.is_sync(),
                                                       &self.opts.line_opts).await {
                        Ok(len) => written += len + 1, // +1 for the newline
                        Err(mut err) => {
                            error!(
                                "basic publish did not succeed. Have written {}/{} bytes",
                                written,
                                line.len()
                            );
                            err.add_written(written);
                            return Err(err);
                        }
                    }
                }
                // Incomplete frame, no newline yet
                Ok(None) => break,
                // This should never happen
                Err(..) => {
                    panic!("Unable to parse input buffer");
                }
            };
        }

        Ok(written)
    }


    /// Publish all complete buffered lines and, if `allow_partial` is
    /// true, incomplete lines as well. Wait for all publisher confirms to return
    pub async fn sync(&mut self, allow_partial: bool) -> Result<(), WriteError> {
        debug!("Syncing descriptor {}", self.fh);
        debug!("Publishing buffered data");
        if let Err(err) = self.publish_lines(true, allow_partial).await {
            error!("Couldn't sync file buffer");
            return Err(err);
        }
        let out = self.publisher.wait_for_confirms().await;
        *self.num_writes.write().await = 0;
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
        // self.channel.close(0, "File handle closed").await.ok();
        self.buffer.write().await.truncate(0);
        debug!("Channel closed");
        Ok(())
    }

    pub fn fh(&self) -> FHno {
        self.fh
    }

}

pub(crate) mod rabbit {

use std::sync::Arc;

use super::*;
use crate::amqp_fs::{connection, self};

/// Table of open file descriptors that publish to a RabbitMQ server
pub(crate) struct FileHandleTable {
    /// Open RabbitMQ connection
    connection: Arc<RwLock<connection::ConnectionPool>>,

    /// Files created from this table will publish to RabbitMQ on this exchange
    exchange: String,

    /// Mapping of inode numbers to file handle. Maybe accessed
    /// accross threads, but only one thread should hold a file handle
    /// at a time.
    #[doc(hidden)]
    pub(crate) file_handles: DashMap<FHno, FileHandle<RabbitPublisher> >,

    /// Atomically increment this to get the next handle number
    #[doc(hidden)]
    next_fh: AtomicU64,
}

impl FileHandleTable {
    /// Create a new file table.  Created files will have the specificed maximum buffer size
    pub fn new(mgr: connection::ConnectionManager, exchange: &str) -> Self {

        Self {
            connection: Arc::new(RwLock::new(
               connection::ConnectionPool::builder(mgr).build().unwrap()
            )),
            exchange: exchange.to_string(),
            file_handles: DashMap::with_hasher(RandomState::new()),
            next_fh: AtomicU64::new(0),
        }
    }

    /// Create a file table from command line arguments
    pub fn from_command_line(args: &crate::cli::Args) -> Self {

        let conn_props = lapin::ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);
        let connection_manager = amqp_fs::connection::ConnectionManager::from_command_line(&args, conn_props);
        FileHandleTable::new(connection_manager, &args.exchange)
    }

    /// Get a valid handle number for a new file
    fn next_fh(&self) -> FHno {
        self.next_fh.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl FileTable for FileHandleTable{
    type Publisher = RabbitPublisher;

    /// Create a new open file handle with the givin flags and insert
    /// it into the table. Return the handle ID number for lookup
    /// later.
    ///
    /// Writing to the new file will publish messages on the given
    /// connection using `exchange` and `routing_key`.
    /// The file can be retrived later using [FileHandleTable::entry]
    async fn insert_new_fh (
        &self,
        routing_key: &str,
        flags: u32,
        opts: &WriteOptions,
    ) -> Result<FHno, WriteError> {
        match self.connection.as_ref().read().await.get().await {
            Err(_) => {
                error!("No connection available");
                Err(WriteError::EndpointConnectionError)
            }
            Ok(conn) => {
                let fhno = self.next_fh();
                let publisher = RabbitPublisher::new(&conn, &self.exchange, routing_key, opts).await?;
                self.file_handles.insert(
                    fhno,
                    FileHandle::new(fhno, publisher, flags, opts.clone())
                );
                Ok(fhno)
            }
        }
    }

    /// Get an open entry from the table, if it exits.
    ///
    /// Has the same sematics as [DashMap::entry]
    fn entry(&self, fh: FHno) -> dashmap::mapref::entry::Entry<FHno, FileHandle<RabbitPublisher>, RandomState> {
        self.file_handles.entry(fh)
    }

    /// Remove an entry from the file table.
    ///
    /// Note that this does not release the file.
    fn remove(&self, fh: FHno) {
        self.file_handles.remove(&fh);
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
            Self::TimeoutError(..) => Some(libc::EIO),
            Self::EndpointConnectionError => Some(libc::EIO),
        }
    }

    /// Number of bytes succesfully written before the error
    pub fn written(&self) -> usize {
        match self {
            Self::RabbitError(_err, size) => *size,
            Self::BufferFull(size) => *size,
            Self::ConfirmFailed(size) => *size,
            Self::ParsingError(size) => size.0,
            Self::TimeoutError(size) => *size,
            Self::EndpointConnectionError => 0,
        }
    }

    /// Return the same error but reporting more data written
    pub fn add_written(&mut self, more: usize) -> &Self {
        match self {
            Self::RabbitError(_err, ref mut size) => *size += more,
            Self::BufferFull(ref mut size) => *size += more,
            Self::ConfirmFailed(ref mut size) => *size += more,
            Self::ParsingError(ref mut size) => size.0 += more,
            Self::TimeoutError (ref mut size)=> *size += more,
            Self::EndpointConnectionError => {},
        }

        self
    }
}

impl From<ParsingError> for WriteError {
    fn from(err: ParsingError) -> WriteError {
        WriteError::ParsingError(err)
    }
}
