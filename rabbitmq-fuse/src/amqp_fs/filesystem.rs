use async_trait::async_trait;
use polyfuse::op::SetAttrTime;
use std::time::UNIX_EPOCH;
use std::{io::BufRead, sync::Arc, time::Duration};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut, StatfsOut, WriteOut},
};

// use pinky_swear::PinkySwear;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use crate::amqp_fs::descriptor::WriteErrorKind;

use super::descriptor::WriteError;
// use tracing_subscriber::fmt;
use super::descriptor::FileTable;
pub(crate) use super::options::*;
use super::publisher::Endpoint;
use super::table;
use miette::Diagnostic;

/// Error emited by filesystem calls. Errors can can from the inode
/// table, kernel IO, [`super::publisher::Endpoint`] writes or
/// internal to the filesystem. Internal errors will be created via
/// [`Error::raw_os_error`] from a C stdlib error code
#[derive(Error, Diagnostic, Debug)]
pub enum Error {
    /// An error from the inode table
    #[error(transparent)]
    Table(#[from] super::table::Error),

    /// An IO error from a filesystem operation
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// An endpoint write failure
    #[error(transparent)]
    EndpointWrite(#[from] super::descriptor::WriteError),
    /// Error decoding the request from the kernel
    #[error(transparent)]
    Decode(#[from] op::DecodeError),
}

impl Error {
    /// Create a new error from a system error code
    fn from_raw_os_error(code: i32) -> Error {
        std::io::Error::from_raw_os_error(code).into()
    }

    /// Get the system error code corespoinding to the error. If there
    /// is no corresponding code, returns `None`.
    fn raw_os_error(&self) -> Option<i32> {
        match self {
            Error::Table(e) => Some(e.raw_os_error()),
            Error::IO(e) => e.raw_os_error(),
            Error::EndpointWrite(e) => e.get_os_error(),
            Error::Decode(_) => Some(libc::EIO),
        }
    }
}

/// The result type of filesystem operations.
type Result<T> = ::std::result::Result<T, Error>;

/// Default time to live for attributes returned to the kernel
const TTL: Duration = Duration::from_secs(1);

/// Main filesytem  handle. Representes  the connection to  the rabbit
/// server and the one-deep list of directories inside it.
///
/// None of its methods return errors themselves, rather they set the
/// error value on the request, which Fuse will eventually use to set
/// `errno` for the caller. Those error codes are documented as
/// Errors, despite no Rust `Err` ever being returned.
pub(crate) struct Filesystem<E: Endpoint> {
    /// Table of directories and files
    file_table: Arc<table::DirectoryTable>,

    /// The endpoint that calls to `write(2)` will push data to.
    endpoint: E,

    /// Table of open file handles
    file_handles: FileTable<E::Publisher>,

    /// UID of the user who created the mount
    uid: u32,

    /// GID of the user who created the mount
    gid: u32,

    /// Time to live of metadata returned to the kernel
    ttl: Duration,

    /// Options that control the behavior of [Self::write]
    write_options: WriteOptions,
}

/// Things that may be mounted as filesystems
#[async_trait]
pub(crate) trait Mountable {
    /// Begin processing filesystem requests emitted by `session. This
    /// will loop over all requests until either the session is closed
    /// or the `CancellationToken` is cancelled
    async fn run(
        self: Box<Self>,
        session: crate::session::AsyncSession,
        cancel: CancellationToken,
    ) -> Result<()>;
}

// all these methods need to be async to fit the api, but some of them
// (e.g. rm) don't actually do anything async
#[allow(clippy::unused_async)]
impl<E> Filesystem<E>
where
    E: Endpoint,
{
    /// Create a new filesystem that will write to the given endpoint
    pub fn new(endpoint: E, write_options: WriteOptions) -> Self {
        #![allow(clippy::similar_names)]
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        Filesystem {
            uid,
            gid,
            ttl: TTL,
            endpoint,
            file_table: table::DirectoryTable::new(uid, gid, 0o700),
            file_handles: FileTable::new(),
            write_options,
        }
    }

    /// Returns stats about the filesytem
    pub async fn statfs(&self, _op: op::Statfs<'_>) -> Result<StatfsOut> {
        let mut out = StatfsOut::default();
        let stat = out.statfs();
        stat.files(self.file_table.map.len() as u64);
        stat.namelen(255);

        Ok(out)
    }

    /// Lookup the inode of a file in a parent directory by name
    /// # Errors
    /// - ENOENT if the parent directory or target name does not exist
    /// - EINVAL if the file name is not valid (e.g. not UTF8)
    pub async fn lookup(&self, op: op::Lookup<'_>) -> Result<EntryOut> {
        info!(
            "Doing lookup of {:?} in parent inode {}",
            op.name(),
            op.parent()
        );

        let mut out = EntryOut::default();
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        // The name is a [u8] (i.e. a `char*`), so we have to cast it to unicode
        let name = match op.name().to_str() {
            Some(name) => Ok(name),
            None => Err(super::table::Error::InvalidName),
        }?;

        let ino = self
            .file_table
            .lookup(op.parent(), name)
            .ok_or(super::table::Error::NotExist)?;
        info!("Found inode {} for {}", ino, name);

        let dir = self.file_table.get(ino)?;
        out.ino(dir.info().ino);
        fill_attr(out.attr(), dir.attr());
        // req.reply(out)?;
        Ok(out)
    }

    /// Get the attrributes (as in stat(2)) of the inode
    ///
    /// # Errors
    /// - ENOENT if the inode does not exist
    /// # Panic
    /// Will panic if attributes are too large or too small
    pub async fn getattr(&self, op: op::Getattr<'_>) -> Result<AttrOut> {
        info!("Getting attributes of {}", op.ino());

        let node = self.file_table.get(op.ino())?;

        // let fill_attr = Self::fill_dir_attr;

        let mut out = AttrOut::default();
        fill_attr(out.attr(), node.attr());
        out.ttl(self.ttl);
        debug!(
            "getattr for {}: {:?}",
            node.info().ino,
            debug::StatWrap::from(*node.attr())
        );
        Ok(out)
    }

    /// Set the attributes of the inode
    ///
    /// # Errors
    /// - ENOENT if the inode does not exist
    pub async fn setattr(&self, op: op::Setattr<'_>) -> Result<AttrOut> {
        info!("Setting file attributes {:?}", op);
        let mut node = self.file_table.get_mut(op.ino())?;
        let mut out = AttrOut::default();
        set_attr(node.attr_mut(), &op);
        fill_attr(out.attr(), node.attr());
        out.ttl(self.ttl);
        Ok(out)
    }

    /// Read the contents (that is, the files '.' and '..') of a directory
    ///
    /// # Errors
    /// - ENOENT if the directory does not exist
    /// - EWOULDBLOCK if the directory exists, but is being accessed by another call
    pub async fn readdir(&self, op: op::Readdir<'_>) -> Result<ReaddirOut> {
        info!("Reading directory {} with offset {}", op.ino(), op.offset());

        let dir = self.file_table.get(op.ino())?;
        debug!(
            "Looking for directory {} in parent {}",
            dir.info().ino,
            dir.parent_ino
        );

        // Otherwise we are reading '.', so list all the directories.
        // There are no top level files.
        let mut out = ReaddirOut::new(op.size() as usize);

        for (i, (name, entry)) in dir
            .iter()
            .skip(op.offset().try_into().expect("Directory offset too large"))
            .enumerate()
        {
            debug!("Found directory entry {} in inode {}", name, op.ino());
            debug!("Adding dirent {}  {:?}", i, entry);
            let full = out.entry(
                name.as_ref(),
                entry.ino,
                entry.typ.into(),
                i as u64 + 1, // offset
            );
            if full {
                debug!("readdir request full");
                break;
            }
        }

        debug!("Returning readdir reply");
        Ok(out)
    }

    /// Create a new directory. Directories can only be created in the root
    ///
    /// # Errors
    /// - EINVAL if the filename is invalid
    /// - EEXIST if a directory of that name already exists
    pub async fn mkdir(&self, op: op::Mkdir<'_>) -> Result<EntryOut> {
        let parent_ino = op.parent();
        if parent_ino != self.file_table.root_ino() {
            error!("Can only create top-level directories");
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL).into());
        }
        let name = op.name();
        info!("Creating directory {:?} in parent {}", name, parent_ino);
        let mut out = EntryOut::default();

        let stat = self.file_table.mkdir(name, self.uid, self.gid)?;
        fill_attr(out.attr(), &stat);
        out.ino(stat.st_ino);
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        // self.fill_dir_attr(out.attr());
        info!("New directory has stat {:?}", debug::StatWrap::from(stat));
        Ok(out)
    }

    /// Remove an empty directory. The root may not be removed.
    ///
    /// # Errors
    /// - EINVAL if the filename is not valid
    /// - ENOTDIR the inode is a file and not a directory
    /// - ENOENT the directory does not exist
    /// Otherwise any error from [`table::DirectoryTable::rmdir`] is returned
    pub async fn rmdir(&self, op: op::Rmdir<'_>) -> Result<()> {
        debug!("Removing directory {}", op.name().to_string_lossy());

        // We only have directories one level deep
        if op.parent() != self.file_table.root_ino() {
            panic!("Directory too deep");
            // return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR).into());
        }
        self.file_table.rmdir(op.parent(), op.name())?;

        Ok(())
    }

    /// Create a new regular file (node). Files may only be created in
    /// directories. There should be no files in the root.
    ///
    /// # Errors
    /// - EINVAL the filename is not valid
    /// Otherwise any error returned from [`table::DirectoryTable::mknod`] is returned
    pub async fn mknod(&self, op: op::Mknod<'_>) -> Result<EntryOut> {
        let attr = self.file_table.mknod(op.name(), op.mode(), op.parent())?;
        let mut out = EntryOut::default();
        out.ino(attr.st_ino);
        fill_attr(out.attr(), &attr);
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        Ok(out)
    }

    /// Reduce the link count of a file node
    ///
    /// # Errors
    /// - EINVAL the file name is not valid
    /// Otherwise errors from [`table::DirectoryTable::unlink`] are returned
    pub async fn unlink(&self, op: op::Unlink<'_>) -> Result<()> {
        self.file_table.unlink(op.parent(), op.name())?;
        Ok(())
    }

    /// Rename the given indode
    ///
    /// # Errors
    /// - ENOENT the source does not exist
    /// - EINVAL the source or target do not have valid names
    pub async fn rename(&self, op: op::Rename<'_>) -> Result<()> {
        let oldname = op
            .name()
            .to_str()
            .ok_or(std::io::Error::from_raw_os_error(libc::ENOENT))?;
        let newname = op
            .newname()
            .to_str()
            .ok_or(std::io::Error::from_raw_os_error(libc::EINVAL))?;
        debug!("Renameing {} -> {}", oldname, newname);
        let ino = self
            .file_table
            .lookup(op.parent(), oldname)
            .ok_or(std::io::Error::from_raw_os_error(libc::ENOENT))?;
        debug!("Getting unique ref to old parent");
        {
            let mut oldparent = self.file_table.get_mut(op.parent())?;
            oldparent.remove_child(oldname);
        }

        let entry = self.file_table.get(ino)?;

        debug!("Getting unique ref to new parent");
        {
            let mut newparent = self.file_table.get_mut(op.newparent())?;
            newparent.insert_child(newname, entry.info());
        };

        Ok(())
    }

    /// Create a new descriptor for a file and call
    /// [`crate::amqp_fs::publisher::Endpoint::open`] to create a new
    /// [`crate::amqp_fs::publisher::Publisher`]
    ///
    /// # Errors
    /// - EISDIR if the inode points to a directory
    /// - ENOENT if the inode does not exist
    pub async fn open(&self, op: op::Open<'_>) -> Result<OpenOut> {
        info!("Opening new file handle for ino {}", op.ino());
        {
            // Check that the node is in fact a normal file and not a
            // directory, and update the metadata. Scope this to
            // ensure the lock is dropped when we try to get the path,
            // which require touching the table again
            let mut node = self.file_table.get_mut(op.ino())?;
            if (node.typ()) == libc::DT_DIR {
                error!("Refusing to open; directory is not a file");
                return Err(std::io::Error::from_raw_os_error(libc::EISDIR).into());
            }
            node.atime_to_now(op.flags());
            trace!("Opening node in parent {}", node.parent_ino);
            // node.parent_ino
        };
        let path = self.file_table.real_path(op.ino())?;
        trace!("Opening file bound to routing key {:?}", &path);
        let fh = {
            trace!("Creating new file handle");
            let opener = self.file_handles.insert_new_fh(
                &self.endpoint,
                &path,
                op.flags(),
                &self.write_options,
            );
            if self.write_options.open_timeout_ms > 0 {
                tokio::time::timeout(
                    tokio::time::Duration::from_millis(self.write_options.open_timeout_ms),
                    opener,
                )
                .await
                .unwrap_or(Err(WriteErrorKind::TimeoutError.into_error(0)))
            } else {
                opener.await
            }?
        };

        trace!("New file handle {}", fh);
        let mut out = OpenOut::default();
        out.fh(fh);
        out.nonseekable(true);
        Ok(out)
    }

    /// Synchonrize the file descriptor
    ///
    /// This causes all buffered data in the file the to be written to
    /// the rabbit server. This may includes partially formed lines.
    /// Depending on the options given, publishing partly formed lines
    /// may cause errors, which will be emitted as EIO.
    ///
    /// This behavior is controlled by the [`SyncStyle`] value of
    /// [`WriteOptions::line_opts.fsync`]. If publishing partial lines
    /// are allowed, this is likely to error, so consider the setting
    /// of [`WriteOptions::line_opts.handle_unparsable`] as well.
    ///
    /// Additionally, this call blocks until all unconfirmed messages
    /// are either confirmed by the server or an error is returned.
    ///
    /// # Errors
    ///
    /// Can return all the errors from [`Endpoint::Publisher`]'s
    /// [`crate::amqp_fs::publisher::Publisher::basic_publish`] as
    /// well as ENOENT if the file has stopped existing, or EIO of the
    /// publishing of the remaining buffer fails
    pub async fn fsync(&self, op: op::Fsync<'_>) -> Result<()> {
        use dashmap::mapref::entry::Entry;
        let allow_partial = self.write_options.fsync.allow_partial();
        debug!("Syncing file {} allow_partial: {}", op.fh(), allow_partial);
        if let Entry::Occupied(mut entry) = self.file_handles.entry(op.fh()) {
            match entry.get_mut().sync(allow_partial).await {
                Ok(..) => Ok(()),
                Err(..) => Err(std::io::Error::from_raw_os_error(libc::EIO).into()),
            }
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT).into())
        }
    }

    /// Issued by close(2) when the handle closes
    ///
    /// This causes *completed* lines to be published but not
    /// incomplete ones. The descriptor may still be held elsewhere
    /// and is still valid to write.
    ///
    /// As with [`Self::fsync`], this will block until previously published
    /// messages are confirmed or an error returned. As such it may
    /// return errors from previous [`Self::write`] calls
    pub async fn flush(&self, op: op::Flush<'_>) -> Result<()> {
        use dashmap::mapref::entry::Entry;
        debug!("Flushing file handle");
        if let Entry::Occupied(mut entry) = self.file_handles.entry(op.fh()) {
            if entry.get_mut().sync(false).await.is_ok() {
                debug!("File closed");
            } else {
                error!("File sync returned an error");
                return Err(Error::from_raw_os_error(libc::EIO));
            }
        } else {
            return Err(Error::from_raw_os_error(libc::ENOENT));
        }
        debug!("Flush complete");
        Ok(())
    }

    /// Called when the kernel releases the file descriptor, after the last holder calls `close(2)`
    ///
    /// Blocks until the descriptor is fully flushed and all
    /// confirmations recieved. As such it may return errors from
    /// previous calls.  Attempting to use the file handle after release is an error
    pub async fn release(&self, op: op::Release<'_>) -> Result<()> {
        use dashmap::mapref::entry::Entry;
        info!("Releasing file handle");
        match self.file_handles.entry(op.fh()) {
            Entry::Occupied(mut entry) => {
                if entry.get_mut().release().await.is_ok() {
                    debug!("File descriptor removed");
                } else {
                    error!("File descriptor {} produced an error on release", op.fh());
                    return Err(Error::from_raw_os_error(libc::EIO));
                }
            }
            Entry::Vacant(..) => {
                return Err(Error::from_raw_os_error(libc::ENOENT));
            }
        }
        self.file_handles.remove(op.fh());
        debug!("Flush complete");
        Ok(())
    }

    /// Return empty data
    pub async fn read(&self, op: op::Read<'_>) -> Result<&[u8]> {
        use dashmap::mapref::entry::Entry;
        match self.file_handles.entry(op.fh()) {
            Entry::Occupied(..) => {
                let data: &[u8] = &[];
                Ok(data)
            }
            Entry::Vacant(..) => Err(Error::from_raw_os_error(libc::ENOENT)),
        }
    }

    /// Write data to the filesystems endpoint.
    ///
    /// Will reply with the number of bytes actually written.
    /// "Written" here means pushed into the `Endpoint`'s `Publisher`,
    /// where they may be cached for some amount of time, depending on
    /// the flags the file was opened with. In general data will be
    /// kept until a complete "message" is assembled.
    ///
    /// May return errors for previous calls to write, in which case
    /// you should assume that any data written after the last call to
    /// `fsync` was not published.
    ///
    /// # Errors
    /// - EBADF The file descriptor does not point to a on open endpoint publisher
    /// - EIO This or a previous write failed
    /// - ENOBUF Too much data was written without a delimiter
    pub async fn write<T>(&self, op: op::Write<'_>, data: T) -> Result<WriteOut>
    where
        T: BufRead + Unpin + std::marker::Send,
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
                return Err(Error::from_raw_os_error(libc::EBADF));
            }
            Entry::Occupied(mut entry) => {
                let file = entry.get_mut();
                debug!("Found file handle {}", file.fh());
                match file.write_buf(data).await {
                    Ok(written) => {
                        debug!("Wrote {} bytes", written);
                        written
                    }
                    Err(
                        e @ WriteError {
                            kind: WriteErrorKind::ParsingError,
                            size: sz,
                        },
                    ) => {
                        // On a parser error, if we published
                        // *anything* declare victory, otherwise raise
                        // a generic error
                        if sz == 0 {
                            return Err(e.into());
                        }
                        sz
                    }
                    Err(err) => {
                        error!("Write to fd {} failed", op.fh());
                        // Return the error code the descriptor gave
                        // us, or else a generic "IO error"
                        return Err(err.into());
                    }
                }
            }
        };
        debug!(
            "Write complete. Wrote {}/{} requested bytes",
            written,
            op.size()
        );
        if let Entry::Occupied(mut node) = self.file_table.map.entry(op.ino()) {
            node.get_mut().atime_to_now(op.flags());
        }
        // Setup the reply
        let mut out = WriteOut::default();
        // From man 2 write: On Linux, write() (and similar system
        // calls) will transfer at most 0x7ffff000 (2,147,479,552)
        // bytes, returning the number of bytes actually transferred.
        // (This is true on both 32-bit and 64-bit systems.). So, the
        // size was always 32-bits and thus the amount we wrote also
        // 32-bits, thus this cast is always fine (on linux). On
        // non-linux, does polyfuse even work? If not, we're
        // truncating the write here and the caller might think that
        // they didn't write as much data as they thought they did
        out.size(written.try_into().unwrap());
        Ok(out)
    }
}

#[async_trait]
impl<E> Mountable for Filesystem<E>
where
    E: Endpoint + 'static,
{
    async fn run(
        self: Box<Self>,
        session: crate::session::AsyncSession,
        cancel: CancellationToken,
    ) -> Result<()> {
        use polyfuse::Operation;
        // Move out of the box and into an arc so we can hand clones
        // out to the various tasks
        let fs = Arc::new(*self);
        while !cancel.is_cancelled() {
            let req = tokio::select! {
                req = session.next_request() => req,
                _ = cancel.cancelled() => break,
            }?;
            let req = match req {
                Some(req) => req,
                None => break, // kernel closed the connection, so we jump out
            };
            debug!(request = ?req.operation(), "Got request");
            let fs = fs.clone();
            let _task: tokio::task::JoinHandle<Result<()>> = tokio::task::spawn(async move {
                let result = match req.operation()? {
                    Operation::Lookup(op) => {
                        fs.lookup(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Getattr(op) => {
                        fs.getattr(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Setattr(op) => {
                        fs.setattr(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Read(op) => fs.read(op).await.and_then(|out| Ok(req.reply(out)?)),
                    Operation::Readdir(op) => {
                        fs.readdir(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Write(op, data) => {
                        fs.write(op, data).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Mkdir(op) => fs.mkdir(op).await.and_then(|out| Ok(req.reply(out)?)),
                    Operation::Rmdir(op) => fs.rmdir(op).await.and_then(|out| Ok(req.reply(out)?)),
                    Operation::Mknod(op) => fs.mknod(op).await.and_then(|out| Ok(req.reply(out)?)),
                    Operation::Unlink(op) => {
                        fs.unlink(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Rename(op) => {
                        fs.rename(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Open(op) => fs.open(op).await.and_then(|out| Ok(req.reply(out)?)),
                    Operation::Flush(op) => fs.flush(op).await.and_then(|out| Ok(req.reply(out)?)),
                    Operation::Release(op) => {
                        fs.release(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    Operation::Fsync(op) => fs.fsync(op).await.and_then(|out| Ok(req.reply(out)?)),
                    Operation::Statfs(op) => {
                        fs.statfs(op).await.and_then(|out| Ok(req.reply(out)?))
                    }
                    _ => {
                        error!("Unhandled op code in request {:?}", req.operation());
                        Err(Error::from_raw_os_error(libc::ENOSYS))
                    }
                };
                if let Err(e) = result {
                    let code = e.raw_os_error().unwrap_or(libc::EIO);
                    req.reply_error(code)?;
                }
                Ok(())
            });
        }

        // consume the file handles, forcibly closing each
        info!("Closing all open files");
        for mut item in fs.file_handles.file_handles.iter_mut() {
            // Ignore errors that happen here
            let _ = item.value_mut().release().await;
        }
        fs.file_handles.file_handles.clear();
        info!("{} files left", fs.file_handles.file_handles.len());

        fs.file_table.clear();

        Ok(())
    }
}

/// Copy [`polyfuse::reply::FileAttr`] from `stat_t` structure for `stat(2)`
fn fill_attr(attr: &mut FileAttr, st: &libc::stat) {
    attr.ino(st.st_ino);
    attr.mode(st.st_mode);
    attr.uid(st.st_uid);
    attr.gid(st.st_gid);
    // The kernel says these can be u64, but polyfuse says u32. I
    // guess panic if you have 2^32 links?
    attr.nlink(st.st_nlink.try_into().unwrap());
    attr.rdev(st.st_rdev.try_into().unwrap());
    // These disagree about signed vs unsigned. Why are they signed? I
    // guess truncate at 0 if they happen to be negative?
    attr.size(st.st_size.try_into().unwrap_or(0));
    attr.blksize(st.st_blksize.try_into().unwrap_or(0));
    attr.blocks(st.st_blocks.try_into().unwrap_or(0));
    attr.atime(Duration::new(
        st.st_atime.try_into().unwrap_or(0),
        st.st_atime_nsec.try_into().unwrap_or(0),
    ));
    attr.mtime(Duration::new(
        st.st_mtime.try_into().unwrap_or(0),
        st.st_mtime_nsec.try_into().unwrap_or(0),
    ));
    attr.ctime(Duration::new(
        st.st_ctime.try_into().unwrap_or(0),
        st.st_ctime_nsec.try_into().unwrap_or(0),
    ));
}

/// Convert the timestamp to a pair `i64`, (secs, nsecs). For
/// assigning to `stat_t`.
fn get_timestamp(time: &op::SetAttrTime) -> (i64, i64) {
    /// Convert duration to `(secs, nsecs)`, returing `(i64::MAX, 0) if the
    /// conversion fails
    fn duration_to_pair(dur: Duration) -> (i64, i64) {
        let secs = dur.as_secs().try_into().unwrap_or(i64::MAX);
        let nsecs = dur.subsec_nanos().into();
        (secs, nsecs)
    }
    match time {
        SetAttrTime::Timespec(dur) => duration_to_pair(*dur),
        SetAttrTime::Now => {
            let now = std::time::SystemTime::now();
            let dur = now.duration_since(UNIX_EPOCH).expect("no such time");
            duration_to_pair(dur)
        }
        &_ => (0, 0),
    }
}

/// Copy the contents of a kernel request into a `stat_t`
fn set_attr(st: &mut libc::stat, attr: &op::Setattr) {
    if let Some(x) = attr.size() {
        // The size should actually always be 0. If it somehow isn't
        // probably things are fataly wrong
        st.st_size = x.try_into().expect("Too large file size");
    };
    if let Some(x) = attr.mode() {
        st.st_mode = x;
    };
    if let Some(x) = attr.uid() {
        st.st_uid = x;
    };
    if let Some(x) = attr.gid() {
        st.st_gid = x;
    };
    if let Some(x) = attr.atime().as_ref() {
        let (s, ns) = get_timestamp(x);
        st.st_atime = s;
        st.st_atime_nsec = ns;
    };
    if let Some(x) = attr.mtime().as_ref() {
        let (s, ns) = get_timestamp(x);
        st.st_mtime = s;
        st.st_mtime_nsec = ns;
    };
}

#[doc(hidden)]
mod debug {
    use std::fmt;

    pub(super) struct StatWrap {
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

#[cfg(test)]
mod test {
    use crate::{amqp_fs::publisher::StreamCommand, cli::EndpointCommand};

    use super::*;
    use std::sync::Arc;

    use super::{Filesystem, WriteOptions};
    use eyre::Result;
    use tempfile::TempDir;

    /// Spawn a thread running a mount and return the mountpoint and
    /// the join handle for the thread
    async fn get_mount(
        ep: impl EndpointCommand,
    ) -> Result<(TempDir, CancellationToken, tokio::task::JoinHandle<()>)> {
        let mount_dir = TempDir::with_prefix("fusegate")?;
        let fuse_conf = polyfuse::KernelConfig::default();

        let session = crate::session::AsyncSession::mount(
            mount_dir.path().to_str().unwrap().into(),
            fuse_conf,
        )
        .await?;
        let fs = ep.get_mount(&WriteOptions::default()).unwrap();
        let cancel = CancellationToken::new();
        let stop = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                fs.run(session, cancel).await.unwrap();
            })
        };
        Ok((mount_dir, cancel, stop))
    }

    #[test]
    fn create() -> Result<()> {
        let endpoint = crate::amqp_fs::publisher::StdOut { logfile: None };
        let _fs = Filesystem::new(endpoint, WriteOptions::default());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_mount() -> Result<()> {
        let ep = StreamCommand::stdout();
        let (_mount_dir, cancel, stop) = get_mount(ep).await?;
        cancel.cancel();
        stop.await?;
        Ok(())
    }
}
