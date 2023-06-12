use enum_dispatch::enum_dispatch;
use async_trait::async_trait;

#[allow(unused_imports)]
use tracing::{debug, error, info, Level};


use self::options::RabbitBackend;

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

    fn from_command_line(args: &crate::cli::Args) -> anyhow::Result<Self>
    where
        Self: Sized {
        info!(backend=?args.rabbit_options.backend, "Creating rabbit endpoint");
        match args.rabbit_options.backend {
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
