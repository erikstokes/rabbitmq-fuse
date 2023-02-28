use async_trait::async_trait;
use polyfuse::op::SetAttrTime;
use std::time::UNIX_EPOCH;
use std::{
    io::{self, BufRead},
    sync::Arc,
    time::Duration,
};

use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut, StatfsOut, WriteOut},
    Request,
};

// use pinky_swear::PinkySwear;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use super::descriptor::WriteError;
// use tracing_subscriber::fmt;
use super::descriptor::FileTable;
pub(crate) use super::options::*;
use super::publisher::Endpoint;
use super::table;

/// Unwrap the value or return an error code in the reply
macro_rules! unwrap_or_return {
    ($result:expr, $request:ident) => {
        match $result {
            Ok(x) => x,
            Err(err) => {
                return $request.reply_error(err.raw_os_error());
            }
        }
    };
}

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
    routing_keys: Arc<table::DirectoryTable>,

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

    #[doc(hidden)]
    /// Is the file system running?
    is_running: Arc<std::sync::atomic::AtomicBool>,
}

/// Things that may be mounted as filesystems
#[async_trait]
pub(crate) trait Mountable {
    /// Stop processing syscall requests
    fn stop(&self);

    /// Is the mount currently running?
    fn is_running(&self) -> bool;

    /// Begin processing filesystem requests emitted by [session]
    async fn run(self: Arc<Self>, session: crate::session::AsyncSession) -> anyhow::Result<()>;
}

// all these methods need to be async to fit the api, but some of them
// (e.g. rm) don't actually do anything async
#[allow(clippy::unused_async)]
impl<E> Filesystem<E>
    where E: Endpoint
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
            routing_keys: table::DirectoryTable::new(uid, gid, 0o700),
            file_handles: FileTable::new(),
            write_options,
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Returns stats about the filesytem
    pub async fn statfs(&self, req: &Request, _op: op::Statfs<'_>) -> io::Result<()> {
        let mut out = StatfsOut::default();
        let stat = out.statfs();
        stat.files(self.routing_keys.map.len() as u64);
        stat.namelen(255);

        req.reply(out)
    }

    /// Lookup the inode of a file in a parent directory by name
    /// # Errors
    /// - ENOENT if the parent directory or target name does not exist
    /// - EINVAL if the file name is not valid (e.g. not UTF8)
    pub async fn lookup(&self, req: &Request, op: op::Lookup<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
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
            Some(name) => name,
            None => {
                return req.reply_error(libc::EINVAL);
            }
        };

        let ino = match self.routing_keys.lookup(op.parent(), name) {
            Some(ino) => ino,
            None => {
                return req.reply_error(libc::ENOENT);
            }
        };
        info!("Found inode {} for {}", ino, name);

        match self.routing_keys.map.entry(ino) {
            Entry::Vacant(..) => {
                error!("No such file {}", name);
                req.reply_error(libc::ENOENT)
            }
            Entry::Occupied(entry) => {
                let dir = entry.get();
                out.ino(dir.info().ino);
                fill_attr(out.attr(), dir.attr());
                req.reply(out)
            }
        }
    }

    /// Get the attrributes (as in stat(2)) of the inode
    ///
    /// # Errors
    /// - ENOENT if the inode does not exist
    /// # Panic
    /// Will panic if attributes are too large or too small
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
        let node = entry.get();
        fill_attr(out.attr(), node.attr());
        out.ttl(self.ttl);
        debug!(
            "getattr for {}: {:?}",
            node.info().ino,
            debug::StatWrap::from(*node.attr())
        );
        req.reply(out)
    }

    /// Set the attributes of the inode
    ///
    /// # Errors
    /// - ENOENT if the inode does not exist
    pub async fn setattr(&self, req: &Request, op: op::Setattr<'_>) -> io::Result<()> {
        match self.routing_keys.map.entry(op.ino()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let mut out = AttrOut::default();
                let node = entry.get_mut();
                set_attr(node.attr_mut(), &op);
                fill_attr(out.attr(), node.attr());
                out.ttl(self.ttl);
                req.reply(out)
            }
            dashmap::mapref::entry::Entry::Vacant(..) => {
                info!("File does not exist");
                req.reply_error(libc::ENOENT)
            }
        }
    }

    /// Read the contents (that is, the files '.' and '..') of a directory
    ///
    /// # Errors
    /// - ENOENT if the directory does not exist
    /// - EWOULDBLOCK if the directory exists, but is being accessed by another call
    pub async fn readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        info!("Reading directory {} with offset {}", op.ino(), op.offset());

        let dir = unwrap_or_return!(self.routing_keys.get(op.ino()), req);
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
        req.reply(out)
    }

    /// Create a new directory. Directories can only be created in the root
    ///
    /// # Errors
    /// - EINVAL if the filename is invalid
    /// - EEXIST if a directory of that name already exists
    pub async fn mkdir(&self, req: &Request, op: op::Mkdir<'_>) -> io::Result<()> {
        let parent_ino = op.parent();
        if parent_ino != self.routing_keys.root_ino() {
            error!("Can only create top-level directories");
            return req.reply_error(libc::EINVAL);
        }
        let name = op.name();
        info!("Creating directory {:?} in parent {}", name, parent_ino);
        let mut out = EntryOut::default();

        let stat = match self.routing_keys.mkdir(name, self.uid, self.gid) {
            Ok(attr) => attr,
            _ => {
                return req.reply_error(libc::EEXIST);
            }
        };
        fill_attr(out.attr(), &stat);
        out.ino(stat.st_ino);
        out.ttl_attr(self.ttl);
        out.ttl_entry(self.ttl);
        // self.fill_dir_attr(out.attr());
        info!("New directory has stat {:?}", debug::StatWrap::from(stat));
        req.reply(out)
    }

    /// Remove an empty directory. The root may not be removed.
    ///
    /// # Errors
    /// - EINVAL if the filename is not valid
    /// - ENOTDIR the inode is a file and not a directory
    /// - ENOENT the directory does not exist
    /// Otherwise any error from [`table::DirectoryTable::rmdir`] is returned
    pub async fn rmdir(&self, req: &Request, op: op::Rmdir<'_>) -> io::Result<()> {
        debug!("Removing directory {}", op.name().to_string_lossy());

        // We only have directories one level deep
        if op.parent() != self.routing_keys.root_ino() {
            error!("Directory too deep");
            return req.reply_error(libc::ENOTDIR);
        }
        unwrap_or_return!(self.routing_keys.rmdir(op.parent(), op.name()), req);
        req.reply(())
    }

    /// Create a new regular file (node). Files may only be created in
    /// directories. There should be no files in the root.
    ///
    /// # Errors
    /// - EINVAL the filename is not valid
    /// Otherwise any error returned from [`table::DirectoryTable::mknod`] is returned
    pub async fn mknod(&self, req: &Request, op: op::Mknod<'_>) -> io::Result<()> {
        match self.routing_keys.mknod(op.name(), op.mode(), op.parent()) {
            Ok(attr) => {
                let mut out = EntryOut::default();
                out.ino(attr.st_ino);
                fill_attr(out.attr(), &attr);
                out.ttl_attr(self.ttl);
                out.ttl_entry(self.ttl);
                req.reply(out)
            }
            Err(err) => req.reply_error(err.raw_os_error()),
        }
    }

    /// Reduce the link count of a file node
    ///
    /// # Errors
    /// - EINVAL the file name is not valid
    /// Otherwise errors from [`table::DirectoryTable::unlink`] are returned
    pub async fn unlink(&self, req: &Request, op: op::Unlink<'_>) -> io::Result<()> {
        if let Err(err) = self.routing_keys.unlink(op.parent(), op.name()) {
            req.reply_error(err.raw_os_error())
        } else {
            req.reply(())
        }
    }

    /// Rename the given indode
    ///
    /// # Errors
    /// - ENOENT the source does not exist
    /// - EINVAL the source or target do not have valid names
    pub async fn rename(&self, req: &Request, op: op::Rename<'_>) -> io::Result<()> {
        let oldname = match op.name().to_str() {
            Some(name) => name,
            None => {
                return req.reply_error(libc::EINVAL);
            }
        };
        let newname = match op.newname().to_str() {
            Some(name) => name,
            None => {
                return req.reply_error(libc::EINVAL);
            }
        };
        debug!("Renameing {} -> {}", oldname, newname);
        let ino = match self.routing_keys.lookup(op.parent(), oldname) {
            Some(ino) => ino,
            None => {
                return req.reply_error(libc::ENOENT);
            }
        };
        let mut oldparent = match self.routing_keys.get_mut(op.parent()) {
            Ok(parent) => parent,
            Err(err) => {
                return req.reply_error(err.raw_os_error());
            }
        };
        oldparent.remove_child(oldname);

        let entry = match self.routing_keys.get(ino) {
            Ok(e) => e.info().clone(),
            Err(err) => {
                return req.reply_error(err.raw_os_error());
            }
        };

        let mut newparent = match self.routing_keys.get_mut(op.newparent()) {
            Ok(parent) => parent,
            Err(err) => {
                return req.reply_error(err.raw_os_error());
            }
        };
        newparent.insert_child(newname, &entry);

        req.reply(())
    }

    /// Create a new descriptor for a file
    ///
    /// # Errors
    /// - EISDIR if the inode points to a directory
    /// - ENOENT if the inode does not exist
    ///
    /// # Panics
    /// Will panic if a new AMQP channel can't be opened
    pub async fn open(&self, req: &Request, op: op::Open<'_>) -> io::Result<()> {
        info!("Opening new file handle for ino {}", op.ino());
        {
            // Check that the node is in fact a normal file and not a
            // directory, and update the metadata. Scope this to
            // ensure the lock is dropped when we try to get the path,
            // which require touching the table again
            let mut node = match self.routing_keys.map.get_mut(&op.ino()) {
                None => return req.reply_error(libc::ENOENT),
                Some(node) => node,
            };
            if (node.typ()) == libc::DT_DIR {
                error!("Refusing to open; directory is not a file");
                return req.reply_error(libc::EISDIR);
            }
            node.atime_to_now(op.flags());
            trace!("Opening node in parent {}", node.parent_ino);
            // node.parent_ino
        };
        let path = unwrap_or_return!(self.routing_keys.real_path(op.ino()), req);
        trace!("Opening file bound to routing key {:?}", &path);
        let fh = {
            trace!("Creating new file handle");
            let opener = self.file_handles.insert_new_fh(
                &self.endpoint,
                &path,
                op.flags(),
                &self.write_options,
            );
            match if self.write_options.open_timeout_ms > 0 {
                tokio::time::timeout(
                    tokio::time::Duration::from_millis(self.write_options.open_timeout_ms),
                    opener,
                )
                .await
                .unwrap_or(Err(WriteError::TimeoutError(0)))
            } else {
                opener.await
            } {
                Ok(fh) => fh,
                Err(err) => {
                    return req.reply_error(err.get_os_error().unwrap());
                }
            }
        };

        trace!("New file handle {}", fh);
        let mut out = OpenOut::default();
        out.fh(fh);
        out.nonseekable(true);
        req.reply(out)
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
    /// Can return all the errors from [`Rabbit::write`] as well as ENOENT if
    /// the file has stop existing, or EIO of the publishing of the
    /// remaining buffer fails
    pub async fn fsync(&self, req: &Request, op: op::Fsync<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        let allow_partial = self.write_options.fsync.allow_partial();
        debug!("Syncing file {} allow_partial: {}", op.fh(), allow_partial);
        if let Entry::Occupied(mut entry) = self.file_handles.entry(op.fh()) {
            match entry.get_mut().sync(allow_partial).await {
                Ok(..) => req.reply(()),
                Err(..) => req.reply_error(libc::EIO),
            }
        } else {
            req.reply_error(libc::ENOENT)
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
    pub async fn flush(&self, req: &Request, op: op::Flush<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        debug!("Flushing file handle");
        if let Entry::Occupied(mut entry) = self.file_handles.entry(op.fh()) {
            if let Ok(..) = entry.get_mut().sync(false).await {
                debug!("File closed");
            } else {
                error!("File sync returned an error");
                return req.reply_error(libc::EIO);
            }
        } else {
            return req.reply_error(libc::ENOENT);
        }
        debug!("Flush complete");
        req.reply(())
    }

    /// Called when the kernel releases the file descriptor, after the last holder calls `close(2)`
    ///
    /// Blocks until the descriptor is fully flushed and all
    /// confirmations recieved. As such it may return errors from
    /// previous calls.  Attempting to use the file handle after release is an error
    pub async fn release(&self, req: &Request, op: op::Release<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        info!("Releasing file handle");
        match self.file_handles.entry(op.fh()) {
            Entry::Occupied(mut entry) => {
                if let Ok(..) = entry.get_mut().release().await {
                    debug!("File descriptor removed");
                } else {
                    error!("File descriptor {} no longer exists", op.fh());
                    return req.reply_error(libc::EIO);
                }
            }
            Entry::Vacant(..) => {
                return req.reply_error(libc::ENOENT);
            }
        }
        self.file_handles.remove(op.fh());
        debug!("Flush complete");
        req.reply(())
    }

    /// Return empty data
    pub async fn read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        use dashmap::mapref::entry::Entry;
        match self.file_handles.entry(op.fh()) {
            Entry::Occupied(..) => {
                let data: &[u8] = &[];
                req.reply(data)
            }
            Entry::Vacant(..) => req.reply_error(libc::ENOENT),
        }
    }

    /// Write data to the filesystems endpoint.
    ///
    /// Will reply with the number of bytes actually written.
    /// "Written" here means pushed into the [Endpoint] [Publisher],
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
    pub async fn write<T>(&self, req: &Request, op: op::Write<'_>, data: T) -> io::Result<()>
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
                return req.reply_error(libc::EBADF);
            }
            Entry::Occupied(mut entry) => {
                let file = entry.get_mut();
                debug!("Found file handle {}", file.fh());
                match file.write_buf(data).await {
                    Ok(written) => {
                        debug!("Wrote {} bytes", written);
                        written
                    }
                    Err(WriteError::ParsingError(sz)) => {
                        // On a parser error, if we published
                        // *anything* declare victory, otherwise raise
                        // a generic error
                        if sz.0 == 0 {
                            return req.reply_error(libc::EIO);
                        }
                        sz.0
                    }
                    Err(err) => {
                        error!("Write to fd {} failed", op.fh());
                        // Return the error code the descriptor gave
                        // us, or else a generic "IO error"
                        return req.reply_error(err.get_os_error().unwrap_or(libc::EIO));
                    }
                }
            }
        };
        debug!("Write complete. Wrote {}/{} requested bytes", written, op.size());
        if let Entry::Occupied(mut node) = self.routing_keys.map.entry(op.ino()) {
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
        // truncating the write here and the caller might thing that
        // they didn't write as much data as they thought they did
        out.size(written.try_into().unwrap());
        req.reply(out)
    }
}

#[async_trait]
impl<E> Mountable for Filesystem<E>
where
    E: Endpoint + 'static,
{
    fn is_running(&self) -> bool {
        self.is_running.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn stop(&self) {
        self.is_running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    async fn run(self: Arc<Self>, session: crate::session::AsyncSession) -> anyhow::Result<()> {
        use polyfuse::Operation;
        self.is_running
            .store(true, std::sync::atomic::Ordering::Relaxed);
        while let Some(req) = session.next_request().await? {
            let fs = self.clone();
            let _task: tokio::task::JoinHandle<anyhow::Result<()>> =
                tokio::task::spawn(async move {
                    match req.operation()? {
                        Operation::Lookup(op) => fs.lookup(&req, op).await?,
                        Operation::Getattr(op) => fs.getattr(&req, op).await?,
                        Operation::Setattr(op) => fs.setattr(&req, op).await?,
                        Operation::Read(op) => fs.read(&req, op).await?,
                        Operation::Readdir(op) => fs.readdir(&req, op).await?,
                        Operation::Write(op, data) => fs.write(&req, op, data).await?,
                        Operation::Mkdir(op) => fs.mkdir(&req, op).await?,
                        Operation::Rmdir(op) => fs.rmdir(&req, op).await?,
                        Operation::Mknod(op) => fs.mknod(&req, op).await?,
                        Operation::Unlink(op) => fs.unlink(&req, op).await?,
                        Operation::Rename(op) => fs.rename(&req, op).await?,
                        Operation::Open(op) => fs.open(&req, op).await?,
                        Operation::Flush(op) => fs.flush(&req, op).await?,
                        Operation::Release(op) => fs.release(&req, op).await?,
                        Operation::Fsync(op) => fs.fsync(&req, op).await?,
                        Operation::Statfs(op) => fs.statfs(&req, op).await?,
                        _ => {
                            error!("Unhandled op code in request {:?}", req.operation());
                            req.reply_error(libc::ENOSYS)?;
                        }
                    }

                    Ok(())
                });

            if !self.is_running() {
                info!("Leaving fuse loop");
                break;
            }
        }
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
    attr.atime(Duration::new(st.st_atime.try_into().unwrap_or(0),
                             st.st_atime_nsec.try_into().unwrap_or(0)));
    attr.mtime(Duration::new(st.st_mtime.try_into().unwrap_or(0),
                             st.st_mtime_nsec.try_into().unwrap_or(0)));
    attr.ctime(Duration::new(st.st_ctime.try_into().unwrap_or(0),
                             st.st_ctime_nsec.try_into().unwrap_or(0)));
}

/// Convert the timestamp to a `i64`
fn get_timestamp(time: &op::SetAttrTime) -> i64 {
    match time {
        SetAttrTime::Timespec(dur) => dur.as_secs().try_into().unwrap_or(i64::MAX),
        SetAttrTime::Now => {
            let now = std::time::SystemTime::now();
            now.duration_since(UNIX_EPOCH)
                .expect("no such time")
                .as_secs().try_into().unwrap_or(i64::MAX)
        }
        &_ => 0,
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
        st.st_atime = get_timestamp(x);
    };
    if let Some(x) = attr.mtime().as_ref() {
        st.st_mtime = get_timestamp(x);
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
    use super::{Filesystem, WriteOptions};
    use anyhow::Result;

    #[test]
    fn create() -> Result<()> {
        let endpoint = crate::amqp_fs::publisher::StdOut{};
        let fs = Filesystem::new(endpoint, WriteOptions::default());
        Ok(())
    }
}
