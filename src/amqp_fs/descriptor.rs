//! Tools for tracking open files and writing data itno them. The
//! mechanics of publishing to the rabbit server are managed here
use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use tokio::sync::RwLock;

use thiserror::Error;

use dashmap::DashMap;

use std::collections::hash_map::RandomState;

use super::buffer::Buffer;
use super::options::WriteOptions;
use super::publisher::Endpoint;
use super::publisher::Publisher;

/// File Handle number
pub(crate) type FHno = u64;

/// An error that occurs during parsing the line into headers
#[derive(Debug)]
pub struct ParsingError(pub usize);

/// Errors that can return from writing to the file.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("Header mode was specified, but we couldn't parse the line")]
    ParsingError(ParsingError),

    #[error("Unable to connect to the publising endpoint")]
    EndpointConnectionError,

    #[error("RabbitMQ returned some error on publish. Could some from a previous publish but be returned by the current one")]
    RabbitError(lapin::Error, usize),

    #[error("IO error from the publishing backend. This error could result
    from a previous asynchronous publish but be returned by the
    current one")]
    IO {
        #[source]
        source: std::io::Error,
        // backtrace: Backtrace,
        size: usize,
    },

    /// The file's internal buffer filled without encountering a
    /// newline.
    ///
    /// Once emitted, no more writes will be possible. The
    /// file should be closed
    #[error("Internal buffer is full")]
    BufferFull(usize),

    /// A previous message failed the publisher confirm check.
    ///
    /// Either a NACK was returned, or the entire message was returned
    /// as unroutable
    #[error("Publish confirmation failed")]
    ConfirmFailed(usize),

    #[error("Unable to open the file within the timeout")]
    TimeoutError(usize),
}

/// An open file
pub(crate) struct FileHandle<Pub>
where
    Pub: Publisher,
{
    /// File handle id
    #[doc(hidden)]
    fh: FHno,

    // /// RabbitMQ channel the file will publish to on write
    // #[doc(hidden)]
    // channel: Channel,

    // /// The RabbitMQ exchange lines will be published to
    // exchange: String,

    // /// The routing key lines will will be published to
    // routing_key: String,
    /// The [`Publisher`] this file will write to
    publisher: Pub,

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

/// Table of open file descriptors that publish to a `RabbitMQ` server
#[derive(Default)]
pub(crate) struct FileTable<P: Publisher> {
    /// Mapping of inode numbers to file handle. Maybe accessed
    /// accross threads, but only one thread should hold a file handle
    /// at a time.
    #[doc(hidden)]
    pub(crate) file_handles: DashMap<FHno, FileHandle<P>>,

    /// Atomically increment this to get the next handle number
    #[doc(hidden)]
    next_fh: AtomicU64,
}

impl<P: Publisher> FileTable<P> {
    /// Create a new, empty file handle table
    pub fn new() -> Self {
        Self {
            file_handles: DashMap::new(),
            next_fh: AtomicU64::new(0),
        }
    }

    /// Get a valid handle number for a new file
    fn next_fh(&self) -> FHno {
        self.next_fh.fetch_add(1, Ordering::SeqCst)
    }

    /// Create a new open file handle with the given flags that will
    /// write to the given endpoint and insert it into the table.
    /// Return the handle ID number for lookup later.
    ///
    /// Writing to the new file will publish messages according to the
    /// given [Endpoint] The file can be retrived later using
    /// [`FileTable::entry`]
    pub async fn insert_new_fh(
        &self,
        endpoint: &dyn Endpoint<Publisher = P>,
        path: impl AsRef<Path>,
        flags: u32,
        opts: &WriteOptions,
    ) -> Result<FHno, WriteError> {
        debug!("creating new file descriptor for path");
        let fd = self.next_fh();
        let publisher = endpoint.open(path.as_ref(), flags).await?;
        let file = FileHandle::new(fd, publisher, flags, opts.clone());
        self.file_handles.insert(fd, file);
        Ok(fd)
    }

    /// Get an open entry from the table, if it exits.
    ///
    /// Has the same sematics as [`DashMap::entry`]
    pub fn entry(
        &self,
        fh: FHno,
    ) -> dashmap::mapref::entry::Entry<FHno, FileHandle<P>, RandomState> {
        self.file_handles.entry(fh)
    }

    /// Remove an entry from the file table.
    ///
    /// Note that this does not release the file.
    pub fn remove(&self, fh: FHno) {
        self.file_handles.remove(&fh);
    }
}

impl<Pub: Publisher> FileHandle<Pub> {
    /// Create a new file handle, which will publish to the given
    /// connection, using the exchange and `routing_key`
    ///
    /// Generally do not call this yourself. Instead use [`FileHandleTable::insert_new_fh`]
    /// # Panics
    /// Panics if the connection is unable to open the channel
    pub(crate) fn new(fh: FHno, publisher: Pub, flags: u32, opts: WriteOptions) -> Self {
        Self {
            buffer: RwLock::new(Buffer::new(8000, &opts)),
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
    /// immediately. The rest of the data will be buffered. If the
    /// maxumim buffer size is excceded, this write will succed but
    /// future writes will will fail
    pub async fn write_buf<T>(&mut self, mut buf: T) -> Result<usize, WriteError>
    where
        T: BufRead + Unpin + std::marker::Send,
    {
        debug!("Writing with options {:?} {:?} unconfirmed", self.opts, self.num_writes.read().await);

        if let Some(err) = self.publisher.pop_error() {
            error!("Error from previous write {:?}", err);
            return Err(err)
        }

        if *self.num_writes.read().await >= self.opts.max_unconfirmed {
            debug!("Wrote a lot, waiting for confirms");
            self.publisher.wait_for_confirms().await?;
            *self.num_writes.write().await = 0;
        }

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
                    match self
                        .publisher
                        .basic_publish(&line, force_sync || self.is_sync())
                        .await
                    {
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
    /// incomplete lines, close the `RabbitMQ` channel and clears (but
    /// does not drop) the internal buffer.
    pub async fn release(&mut self) -> Result<(), WriteError> {
        // Flush the last partial line before the file is dropped
        self.sync(false).await?;
        self.sync(true).await?;
        // self.channel.close(0, "File handle closed").await.ok();
        self.buffer.write().await.truncate(0);
        debug!("Channel closed");
        Ok(())
    }

    /// The file handle number
    pub fn fh(&self) -> FHno {
        self.fh
    }
}

impl WriteError {
    /// OS error code corresponding to the error, if there is one
    pub fn get_os_error(&self) -> Option<libc::c_int> {
        match self {
            Self::BufferFull(..) => Some(libc::ENOBUFS),
            Self::ParsingError(..)
                | Self::EndpointConnectionError
                | Self::ConfirmFailed(..)
                | Self::TimeoutError(..) => Some(libc::EIO),
            // There isn't an obvious error code for this, so let the
            // caller choose
            Self::RabbitError(..) => None,
            Self::IO{source:err,..} => Some(err.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    /// Number of bytes succesfully written before the error
    pub fn written(&self) -> usize {
        match self {
            Self::RabbitError(_err, size) => *size,
            Self::BufferFull(size) | Self::ConfirmFailed(size) | Self::TimeoutError(size) => *size,
            Self::ParsingError(size) => size.0,
            Self::EndpointConnectionError => 0,
            Self::IO{size, ..} => *size,
        }
    }

    /// Return the same error but reporting more data written
    pub fn add_written(&mut self, more: usize) -> &Self {
        match self {
            Self::RabbitError(_err, ref mut size) => *size += more,
            Self::BufferFull(ref mut size)
                | Self::IO{ref mut size, ..}
                | Self::TimeoutError(ref mut size)
                | Self::ConfirmFailed(ref mut size) => *size += more,
            Self::ParsingError(ref mut err) => err.0 += more,
            Self::EndpointConnectionError => {}
        }

        self
    }
}

impl From<ParsingError> for WriteError {
    fn from(err: ParsingError) -> WriteError {
        WriteError::ParsingError(err)
    }
}

impl From<std::io::Error> for WriteError {
    fn from(err: std::io::Error) -> WriteError {
        WriteError::IO{source:err, size:0}
    }
}

impl From<std::io::ErrorKind> for WriteError {
    fn from(err: std::io::ErrorKind) -> WriteError {
        std::io::Error::from(err).into()
    }
}
