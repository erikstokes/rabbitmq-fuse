use async_trait::async_trait;

use crate::amqp_fs::publisher::Endpoint;
use crate::amqp_fs::publisher::Publisher;
use crate::cli::EndpointCommand;

use super::lapin::endpoint::RabbitExchnage;
use super::lapin::endpoint::RabbitPublisher;
use super::RabbitCommand;

/// Special endpoint for message that will be recieved by amqp-node
#[derive(Debug)]
pub struct NodeEndpoint(RabbitExchnage);

#[derive(Debug)]
pub struct NodePublisher(RabbitPublisher);

#[async_trait]
impl Publisher for NodePublisher {
    async fn wait_for_confirms(&self) -> Result<(), crate::amqp_fs::descriptor::WriteError> {
        self.0.wait_for_confirms().await
    }

    async fn basic_publish(
        &self,
        line: &[u8],
        force_sync: bool,
    ) -> Result<usize, crate::amqp_fs::descriptor::WriteError> {
        self.0.basic_publish(line, force_sync).await
    }
}

#[async_trait]
impl Endpoint for NodeEndpoint {
    type Publisher = NodePublisher;

    type Options = Command;

    async fn open(
        &self,
        path: &std::path::Path,
        flags: u32,
    ) -> Result<Self::Publisher, crate::amqp_fs::descriptor::WriteError> {
        self.0.open(path, flags).await.map(NodePublisher)
    }
}

/// Newtype wrapper to create a second rabbit command with the same
/// arguments
#[derive(Debug, clap::Args)]
pub struct Command {
    #[clap(flatten)]
    args: RabbitCommand,
}

impl EndpointCommand for Command {
    type Endpoint = NodeEndpoint;

    fn as_endpoint(&self) -> miette::Result<NodeEndpoint> {
        self.args.as_endpoint().map(NodeEndpoint)
    }
}
