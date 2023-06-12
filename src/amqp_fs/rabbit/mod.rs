use clap;
use async_trait::async_trait;

#[allow(unused_imports)]
use tracing::{debug, error, info, Level};

use crate::cli::TlsArgs;

use super::{publisher::{Endpoint, Publisher}, descriptor::WriteError};

mod message;

/// Options that control the `RabbitMQ` publisher and endpoint
pub mod options;

#[cfg(feature = "lapin_endpoint")]
/// Rabbit connections provided by [lapin](https://docs.rs/lapin/latest/lapin/index.html)
pub mod lapin;

#[cfg(feature = "amqprs_endpoint")]
/// Rabbit connections provided by [amqprs](https://docs.rs/amqprs/latest/amqprs/)
pub mod amqprs;


#[derive(clap::ValueEnum, Copy, Clone, Debug)]
pub enum RabbitBackend {
    #[cfg(feature = "lapin_endpoint")]
    Lapin,
    #[cfg(feature = "amqprs_endpoint")]
    Amqprs,
}

#[derive(Debug)]
pub enum RabbitExchange {
    #[cfg(feature = "lapin_endpoint")]
    Lapin(lapin::RabbitExchnage),
    #[cfg(feature = "amqprs_endpoint")]
    Amqprs(amqprs::AmqpRsExchange),
}

#[derive(Debug)]
pub(crate) enum RabbitPublisher {
    #[cfg(feature = "lapin_endpoint")]
    Lapin(lapin::RabbitPublisher),
    #[cfg(feature = "amqprs_endpoint")]
    Amqprs(amqprs:: AmqpRsPublisher)
}

#[async_trait]
impl Publisher for RabbitPublisher {
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        match self {
            #[cfg(feature = "lapin_endpoint")]
            Self::Lapin(publisher) => publisher.wait_for_confirms().await,
            #[cfg(feature = "amqprs_endpoint")]
            Self::Amqprs(publisher) => publisher.wait_for_confirms().await,
        }
    }

    async fn basic_publish(&self, line: &[u8], force_sync: bool) -> Result<usize, WriteError> {
        match self {
            #[cfg(feature = "lapin_endpoint")]
            Self::Lapin(publisher) => publisher.basic_publish(line, force_sync).await,
            #[cfg(feature = "amqprs_endpoint")]
            Self::Amqprs(publisher) => publisher.basic_publish(line, force_sync).await,
        }
    }
}

#[async_trait]
impl Endpoint for RabbitExchange {
    type Publisher = RabbitPublisher;
    type Options = RabbitCommand;

    fn from_command_line(args: &RabbitCommand) -> anyhow::Result<Self>
    where
        Self: Sized {
        info!(backend=?args.backend, "Creating rabbit endpoint");
        match args.backend {
            #[cfg(feature = "lapin_endpoint")]
            RabbitBackend::Lapin => Ok(Self::Lapin(lapin::RabbitExchnage::from_command_line(args)?)),
            #[cfg(feature = "amqprs_endpoint")]
            RabbitBackend::Amqprs => Ok(Self::Amqprs(amqprs::AmqpRsExchange::from_command_line(args)?)),
        }
    }

    async fn open(&self, path: &std::path::Path, flags: u32) -> Result<Self::Publisher, WriteError> {
        match self {
            #[cfg(feature = "lapin_endpoint")]
            Self::Lapin(ep) => Ok(RabbitPublisher::Lapin(ep.open(path, flags).await?)),
            #[cfg(feature = "amqprs_endpoint")]
            Self::Amqprs(ep) => Ok(RabbitPublisher::Amqprs(ep.open(path, flags).await?)),
        }
    }
}

#[derive(clap::Args, Debug)]
pub struct RabbitCommand {

    /// URL of the rabbitmq server
    #[arg(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f"))]
    pub rabbit_addr: String,

    #[arg(long, value_enum, default_value="lapin")]
    pub backend: RabbitBackend,

    /// Exchange to bind directories in the mount point to
    #[clap(short, long, default_value_t = String::from(""))]
    pub exchange: String,

    #[command(flatten)]
    pub options: options::RabbitMessageOptions,

    /// Options to control TLS connections
    #[command(flatten)]
    pub(crate) tls_options: TlsArgs,

}


impl RabbitCommand {
    /// Parse the enpoint url string to a [`url::Url`]
    pub fn endpoint_url(&self) -> anyhow::Result<url::Url> {
        Ok(url::Url::parse(&self.rabbit_addr)?)
    }
}

impl crate::cli::EndpointCommand for RabbitCommand {
    type Endpoint = RabbitExchange;

    // fn get_mount(&self) ->  std::sync::Arc<dyn super::Mountable + Send + Sync> {
    //     todo!()
    // }
}
