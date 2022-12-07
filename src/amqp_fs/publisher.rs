
use std::path::Path;

use async_trait::async_trait;

use super::{descriptor::WriteError, options::{LinePublishOptions, WriteOptions}};

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
    async fn basic_publish(&self, line: &[u8], force_sync: bool, line_opts: &LinePublishOptions) -> Result<usize, WriteError>;
}

/// Thing that writes can be published to
#[async_trait]
pub(crate) trait Endpoint: Send+Sync {

    type Publisher: Publisher;

    /// Construct an endpoint from command-line arguments
    fn from_command_line(args: &crate::cli::Args) -> Self where Self: Sized;

    /// Return a new file handle that allows writing to the endpoint using the endpoint publisher
    async fn open(&self, path: &Path, flags: u32, opts: &WriteOptions) -> Result<Self::Publisher, WriteError>;
}
