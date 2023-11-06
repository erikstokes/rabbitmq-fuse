/// An asynchronous FUSE session that emits filesytem requests from the kernel
use polyfuse::{KernelConfig, Request, Session};
use std::{io, path::PathBuf};
use tokio::io::{unix, Interest};

/// Async FUSE session
pub(crate) struct AsyncSession {
    #[doc(hidden)]
    inner: unix::AsyncFd<Session>,
}

impl AsyncSession {
    /// Mount the given path and begin listening for requests
    pub async fn mount(mountpoint: PathBuf, config: KernelConfig) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let session = Session::mount(mountpoint, config)?;
            Ok(Self {
                inner: unix::AsyncFd::with_interest(session, Interest::READABLE)?,
            })
        })
        .await
        .expect("join error")
    }

    /// Get the next request from the kernel. If the `Result` is
    /// `None`, the mount is closed and no further requests will
    /// appear
    pub async fn next_request(&self) -> io::Result<Option<Request>> {
        use futures::{future::poll_fn, ready, task::Poll};

        poll_fn(|cx| {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;
            match self.inner.get_ref().next_request() {
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    guard.clear_ready();
                    Poll::Pending
                }
                res => {
                    guard.retain_ready();
                    Poll::Ready(res)
                }
            }
        })
        .await
    }
}
