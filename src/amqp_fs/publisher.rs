use std::{cell::RefCell, path::Path};

use anyhow;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use futures::lock::Mutex;

use super::descriptor::WriteError;

/// Trait that allows parsing and publishing the results of a buffer
/// to a given endpoint. A publisher is "per client" object. Each open
/// file will recieve its own publsiher, which will handle buffering
/// etc.
#[async_trait]
pub(crate) trait Publisher: Send + Sync + std::fmt::Debug {
    /// Wait until all message to published to the endpoint have been
    /// confirmed. Should return `Ok` if all in-flight messages have
    /// been confrimed, otherwise an error. What exactly "confirmed"
    /// means depends on the endpoint.
    async fn wait_for_confirms(&self) -> Result<(), WriteError>;

    /// Non-blocking poll to see if an error arrived asynchronously.
    /// This should reset the error status
    fn pop_error(&self) -> Option<WriteError> {
        None
    }

    /// Add an asynchronous error to be looked at later
    fn push_error(&self, _err: WriteError) {}

    /// Publish one line to the endpoint. This must be implement for
    /// each endpoint type. Publications are not promised to actually
    /// occur, only be scheduled to occur.
    /// [Publisher::wait_for_confirms] should be called to ensure the
    /// publication happened.
    ///
    /// If `force_sync` is given, block until the confirmation is
    /// recieved. It is still necessary to call `wait_for_confirms`
    /// even when passing `force_sync`
    async fn basic_publish(&self, line: &[u8], force_sync: bool) -> Result<usize, WriteError>;
}

/// Thing that writes can be published to. This is a
/// once-per-filesystem object whose main function to to create a new
/// [`Publisher`] on each call to `open`
#[enum_dispatch(EndpointCommands)]
#[async_trait]
pub(crate) trait Endpoint: Send + Sync + std::fmt::Debug {
    /// The [`Publisher`] type the `Endpoint` will write to
    type Publisher: Publisher;

    /// The options used to create the endpoint
    type Options: clap::Args;

    // /// Construct an endpoint from command-line arguments
    // fn from_command_line(args: &Self::Options) -> anyhow::Result<Self>
    // where
    //     Self: Sized;

    /// Return a new file handle that allows writing to the endpoint using the endpoint publisher
    async fn open(&self, path: &Path, flags: u32) -> Result<Self::Publisher, WriteError>;
}

/// Simple publisher that writes lines to a given stream
#[derive(Debug)]
pub struct StreamPubliser<S: std::io::Write> {
    /// The stream to publish to
    stream: Mutex<RefCell<S>>,
}

/// Endpoint that writes to stdout
#[derive(Debug)]
pub struct StdOut {}

#[derive(clap::Args, Debug)]
pub struct StreamCommand {}

impl crate::cli::EndpointCommand for StreamCommand {
    type Endpoint = StdOut;

    fn as_endpoint(&self) -> anyhow::Result<StdOut>
    where
        Self: Sized,
    {
        Ok(StdOut {})
    }
}

#[async_trait]
impl Endpoint for StdOut {
    type Publisher = StreamPubliser<std::io::Stdout>;
    type Options = StreamCommand;

    async fn open(&self, _path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        Ok(Self::Publisher::new(std::io::stdout()))
    }
}

impl<S: std::io::Write> StreamPubliser<S> {
    /// Create a stream publsiher from the given stream
    fn new(stream: S) -> Self {
        Self {
            stream: Mutex::new(RefCell::new(stream)),
        }
    }
}

#[async_trait]
impl<S> Publisher for StreamPubliser<S>
where
    S: std::io::Write + Send + Sync + std::fmt::Debug,
{
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        self.stream.lock().await.borrow_mut().flush()?;
        Ok(())
    }

    fn pop_error(&self) -> Option<WriteError> {
        None
    }

    fn push_error(&self, _err: WriteError) {}

    async fn basic_publish(&self, line: &[u8], _force_sync: bool) -> Result<usize, WriteError> {
        use std::borrow::BorrowMut;
        let written = self
            .stream
            .lock()
            .await
            .borrow_mut()
            .get_mut()
            .write(line)?;
        Ok(written)
    }
}
