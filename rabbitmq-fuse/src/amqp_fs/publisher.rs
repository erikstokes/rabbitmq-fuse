use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use std::sync::Mutex;

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

    /// Publish one line to the endpoint. This must be implemented for
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
pub struct StreamPubliser {
    /// The stream to publish to
    stream: Arc<Mutex<dyn std::io::Write + Sync + Send>>,
}

impl std::fmt::Debug for StreamPubliser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamPubliser").finish_non_exhaustive()
    }
}

/// Endpoint that writes to stdout or a given file path
#[derive(Debug)]
pub struct StdOut {
    /// Path to redirct writes to. Each publisher will get an open
    /// file to this path. If `None`, writes will go to `stdout`.
    pub(crate) logfile: Option<std::path::PathBuf>,
}

/// Command that creates and `Endpoint` publishing data to `stdout`
#[derive(clap::Args, Debug)]
pub struct StreamCommand {
    /// All writes will redirect to this file
    #[clap(long)]
    logfile: Option<std::path::PathBuf>,
}

// These are only used by internal tests. The public interface is the
// clap generated CLI
impl StreamCommand {
    #[cfg(test)]
    /// Create a new stream command for the given path
    pub(crate) fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            logfile: Some(path.into()),
        }
    }

    #[cfg(test)]
    pub(crate) fn stdout() -> Self {
        Self { logfile: None }
    }
}

impl crate::cli::EndpointCommand for StreamCommand {
    type Endpoint = StdOut;

    fn as_endpoint(&self) -> miette::Result<StdOut>
    where
        Self: Sized,
    {
        Ok(StdOut {
            logfile: self.logfile.clone(),
        })
    }
}

#[async_trait]
impl Endpoint for StdOut {
    type Publisher = StreamPubliser;
    type Options = StreamCommand;

    async fn open(&self, _path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        match &self.logfile {
            Some(path) => {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                Ok(Self::Publisher::new(file))
            }
            None => Ok(Self::Publisher::new(std::io::stdout())),
        }
    }
}

impl StreamPubliser {
    /// Create a stream publsiher from the given stream
    fn new<S: std::io::Write + Sync + Send + 'static>(stream: S) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

#[async_trait]
impl Publisher for StreamPubliser {
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        self.stream.lock().unwrap().flush()?;
        Ok(())
    }

    async fn basic_publish(&self, line: &[u8], _force_sync: bool) -> Result<usize, WriteError> {
        let mut handle = self.stream.lock().unwrap();
        let mut written = handle.write(line)?;
        written += handle.write(b"\n")?;
        Ok(written)
    }
}
