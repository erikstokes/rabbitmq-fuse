use std::sync::Arc;

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


/// Selector for the backend rabbit library. This is used to form
/// connections and publish messages
#[derive(clap::ValueEnum, Copy, Clone, Debug)]
pub enum RabbitBackend {
    /// Backend using the [lapin](https://docs.rs/lapin/latest/lapin/index.html) library
    #[cfg(feature = "lapin_endpoint")]
    Lapin,
    #[cfg(feature = "amqprs_endpoint")]
    /// Backend using [ampqrs](https://docs.rs/amqprs/latest/amqprs/)
    Amqprs,
}

/// Dummy struct to abstract over the various rabbit backend types
#[derive(Debug)]
pub enum RabbitExchange {
    /// Endpoint using the [lapin](https://docs.rs/lapin/latest/lapin/index.html) library
    #[cfg(feature = "lapin_endpoint")]
    Lapin(lapin::RabbitExchnage),
    #[cfg(feature = "amqprs_endpoint")]
    /// Endpoint using [ampqrs](https://docs.rs/amqprs/latest/amqprs/)
    Amqprs(amqprs::AmqpRsExchange),
}

/// Dummy struct to abstract over the various rabbit backend types
#[derive(Debug)]
pub(crate) struct RabbitPublisher {}

#[async_trait]
impl Publisher for RabbitPublisher {
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        unreachable!()
    }

    async fn basic_publish(&self, _line: &[u8], _force_sync: bool) -> Result<usize, WriteError> {
        unreachable!()
    }
}

#[async_trait]
impl Endpoint for RabbitExchange {
    type Publisher = RabbitPublisher;
    type Options = RabbitCommand;

    async fn open(&self, _path: &std::path::Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        unreachable!()
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

    /// Options to controll the message published
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

    fn get_mount(&self, write: &super::options::WriteOptions) ->  anyhow::Result<std::sync::Arc<dyn super::Mountable + Send + Sync + 'static>> {
        // Unwrap the enum and return a filesystem using the inner type
        let fs: Arc<dyn super::Mountable + Send + Sync + 'static> = match self.backend {
            #[cfg(feature = "lapin_endpoint")]
            RabbitBackend::Lapin => {
                match self.as_endpoint()? {
                    RabbitExchange::Lapin(ep) =>  std::sync::Arc::new(super::Filesystem::new(ep, write.clone())),
                    _ => unreachable!(),
                }

            },
            #[cfg(feature = "amqprs_endpoint")]
            RabbitBackend::Amqprs => {
                  match self.as_endpoint()? {
                    RabbitExchange::Amqprs(ep) =>  std::sync::Arc::new(super::Filesystem::new(ep, write.clone())),
                    _ => unreachable!(),
                }
            }
        };
        Ok(fs)
    }

    fn as_endpoint(&self) -> anyhow::Result<Self::Endpoint> {
        Ok(match self.backend {
            #[cfg(feature = "lapin_endpoint")]
            RabbitBackend::Lapin => {
                RabbitExchange::Lapin(lapin::RabbitExchnage::from_command(self)?)
            },
            #[cfg(feature = "amqprs_endpoint")]
            RabbitBackend::Amqprs => {
                RabbitExchange::Amqprs(amqprs::AmqpRsExchange::from_command(self)?)
            }
        })
    }
}
