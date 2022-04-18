
use std::{io, path::PathBuf};
use tokio:: io::{unix, Interest};
use polyfuse::{KernelConfig,  Request, Session};

pub(crate)
struct AsyncSession {
    inner: unix::AsyncFd<Session>,
}

impl AsyncSession {
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
