//! Tools for tracking open files and writing data itno them. The
//! mechanics of publishing to the rabbit server are managed here
use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio_util::codec::AnyDelimiterCodecError;
#[allow(unused_imports)]
use tracing::{debug, error, info, instrument, trace, warn};

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

/// Types of errors that can occur during file writes
#[derive(Debug, Error)]
pub enum WriteErrorKind {
    /// Paring error for AMQP headers. Generally this means we failed
    /// to parse the json input, but can also be raised by failing to
    /// emit it as AMQP
    #[error("Header mode was specified, but we couldn't parse the line")]
    ParsingError,

    /// The enpoint failed to connect the the RabbitMQ server, or
    /// failed to open a channel
    #[error("Unable to connect to the publising endpoint")]
    EndpointConnectionError,

    /// Errors returned by the endpoint
    #[error("An endpoint returned some error on publish. Could some from a previous publish but be returned by the current one")]
    EndpointError {
        /// The source error
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// IO errors returned by publishing
    #[error(
        "IO error from the publishing backend. This error could result
    from a previous asynchronous publish but be returned by the
    current one"
    )]
    IO {
        /// The source IO error
        #[source]
        source: std::io::Error,
        // backtrace: Backtrace,
    },

    /// The file's internal buffer filled without encountering a
    /// newline.
    ///
    /// Once emitted, no more writes will be possible. The
    /// file should be closed
    #[error("Internal buffer is full")]
    BufferFull,

    /// A previous message failed the publisher confirm check.
    ///
    /// Either a NACK was returned, or the entire message was returned
    /// as unroutable
    #[error("Publish confirmation failed")]
    ConfirmFailed,

    /// Failed to open the endpoint connection with the timeout
    #[error("Unable to open the file within the timeout")]
    TimeoutError,
}

impl WriteErrorKind {
    /// Add a size and convert to a [`WriteError`]
    pub fn into_error(self, size: usize) -> WriteError {
        WriteError { kind: self, size }
    }
}

/// Errors that can return from writing to the file. Errors that can
/// occur during publishing have a `usize` memeber containing the
/// number of bytes written before the error
#[derive(Debug, Error)]
pub struct WriteError {
    /// The type of error that occured
    pub kind: WriteErrorKind,
    /// The number of bytes that were written successfully before the
    /// error occured
    pub size: usize,
}

// Display is required to implement Error
impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "kind: {}, size: {}", self.kind, self.size)
    }
}

/// An open file
#[derive(Debug)]
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
    #[instrument(skip(self))]
    pub async fn insert_new_fh<Opts: clap::Args>(
        &self,
        endpoint: &dyn Endpoint<Publisher = P, Options = Opts>,
        path: impl AsRef<Path> + std::fmt::Debug,
        flags: u32,
        opts: &WriteOptions,
    ) -> Result<FHno, WriteError> {
        debug!(path=?path, "creating new file descriptor for path");
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
    #[instrument(skip(self))]
    pub fn remove(&self, fh: FHno) {
        self.file_handles.remove(&fh);
    }
}

impl<Pub: Publisher> FileHandle<Pub> {
    /// Create a new file handle, which will publish to the given
    /// connection, using the exchange and `routing_key`
    ///
    /// Generally do not call this yourself. Instead use [`FileTable::insert_new_fh`]
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
    // #[instrument(skip(buf))]
    pub async fn write_buf<T>(&mut self, mut buf: T) -> Result<usize, WriteError>
    where
        T: BufRead + Unpin + std::marker::Send,
    {
        debug!(
            "Writing with options {:?} {:?} unconfirmed",
            self.opts,
            self.num_writes.read().await
        );

        if let Some(err) = self.publisher.pop_error() {
            error!("Error from previous write {:?}", err);
            return Err(err);
        }

        if *self.num_writes.read().await >= self.opts.max_unconfirmed {
            debug!("Wrote a lot, waiting for confirms");
            self.publisher.wait_for_confirms().await?;
            *self.num_writes.write().await = 0;
        }

        if self.buffer.read().await.is_full() {
            return Err(WriteErrorKind::BufferFull.into_error(0));
        }

        // Read the input buffer into our internal line buffer and
        // then publish the results. The amount read is the amount
        // pushed into the internal buffer, not the amount published
        // since incomplete lines can be held for later writes
        let read_bytes = self.buffer.write().await.extend(buf.fill_buf()?);
        buf.consume(read_bytes);

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
    // #[instrument(skip(self))]
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

                    #[cfg(feature = "prometheus_metrics")]
                    crate::MESSAGE_COUNTER.inc();

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
                Err(AnyDelimiterCodecError::MaxChunkLengthExceeded) => {
                    warn!("Internal buffer is full");
                    return Err(WriteErrorKind::BufferFull.into_error(written));
                }
                Err(e) => {
                    // This should never happen, but we don't panic
                    // because it's in a spawned task, so tokio
                    // doesn't propagate the error
                    error!(error=?e, "An error occured parsing the file buffer");
                    return Err(WriteErrorKind::ParsingError.into_error(written));
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

        if let Some(err) = self.publisher.pop_error() {
            error!(error = ?err, "Error from previous write");
            return Err(err);
        }

        if let Err(err) = self.publish_lines(true, allow_partial).await {
            error!("Couldn't sync file buffer");
            return Err(err);
        }
        let out = self.publisher.wait_for_confirms().await;
        debug!(confirm=?out, "Got confirms");
        *self.num_writes.write().await = 0;
        debug!("Buffer flush complete {:?}", &out);
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
        match &self.kind {
            WriteErrorKind::BufferFull => Some(libc::ENOBUFS),
            WriteErrorKind::ParsingError
            | WriteErrorKind::EndpointConnectionError
            | WriteErrorKind::ConfirmFailed
            | WriteErrorKind::TimeoutError => Some(libc::EIO),
            // There isn't an obvious error code for this, so let the
            // caller choose
            WriteErrorKind::EndpointError { .. } => None,
            WriteErrorKind::IO { source: err, .. } => Some(err.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    /// Number of bytes succesfully written before the error
    pub fn written(&self) -> usize {
        self.size
    }

    /// Return the same error but reporting more data written
    pub fn add_written(&mut self, more: usize) -> &WriteError {
        self.size += more;
        self
    }
}

impl From<ParsingError> for WriteError {
    fn from(err: ParsingError) -> WriteError {
        WriteError {
            kind: WriteErrorKind::ParsingError,
            size: err.0,
        }
    }
}

impl From<std::io::Error> for WriteError {
    fn from(err: std::io::Error) -> WriteError {
        Self {
            kind: WriteErrorKind::IO { source: err },
            size: 0,
        }
    }
}

impl From<std::io::ErrorKind> for WriteError {
    fn from(err: std::io::ErrorKind) -> WriteError {
        std::io::Error::from(err).into()
    }
}
