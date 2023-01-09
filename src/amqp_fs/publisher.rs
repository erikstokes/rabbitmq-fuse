
use std::{path::Path, cell::RefCell,};

use async_trait::async_trait;
use futures::lock::Mutex;

use super::descriptor::WriteError;

/// Trait that allows parsing and publishing the results of a buffer
/// to a given endpoint
#[async_trait]
pub(crate) trait Publisher: Send+Sync {

     /// Wait until all message to published to the endpoint have been
     /// confirmed. Should return `Ok` if all in-flight messages have
     /// been confrimed, otherwise an error. What exactly "confirmed"
     /// means depends on the endpoint.
    async fn wait_for_confirms(&self) -> Result<(), WriteError>;

    /// Publish one line to the endpoint. This must be implement for
    /// each endpoint type. Publications are not promised to actually
    /// occur, only be scheduled to occur.
    /// [Publisher::wait_for_confirms] should be called to ensure the
    /// publication happened
    async fn basic_publish(&self, line: &[u8], force_sync: bool) -> Result<usize, WriteError>;
}

/// Thing that writes can be published to
#[async_trait]
pub(crate) trait Endpoint: Send+Sync {

    type Publisher: Publisher;

    /// Construct an endpoint from command-line arguments
    fn from_command_line(args: &crate::cli::Args) -> Self where Self: Sized;

    /// Return a new file handle that allows writing to the endpoint using the endpoint publisher
    async fn open(&self, path: &Path, flags: u32) -> Result<Self::Publisher, WriteError>;
}

pub struct StreamPubliser<S: std::io::Write> {
    stream: Mutex<RefCell<S>>
}

pub struct StdOut{}

#[async_trait]
impl Endpoint for StdOut {
    type Publisher = StreamPubliser<std::io::Stdout>;

    fn from_command_line(_args: &crate::cli::Args) ->Self
        where Self:Sized {
        Self{}
    }

    async fn open(&self, _path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        Ok(Self::Publisher::new(std::io::stdout()))
    }

}

impl<S: std::io::Write> StreamPubliser<S> {
    fn new(stream: S) -> Self {
        Self { stream: Mutex::new(RefCell::new(stream)) }
    }
}

#[async_trait]
impl<S> Publisher for StreamPubliser<S> where S: std::io::Write + Send + Sync {
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        self.stream
            .lock()
            .await
            .borrow_mut()
            .flush()?;
        Ok(())
    }

    async fn basic_publish(&self, line: &[u8], _force_sync: bool) -> Result<usize, WriteError> {
        use std::borrow::BorrowMut;
        let written = self.stream
            .lock()
            .await
            .borrow_mut()
            .get_mut()
            .write(line)?;
        Ok(written)
    }
}
