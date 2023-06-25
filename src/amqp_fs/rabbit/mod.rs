


use clap;

#[allow(unused_imports)]
use tracing::{debug, error, info, Level};

use crate::cli::TlsArgs;



mod message;

/// Options that control the `RabbitMQ` publisher and endpoint
pub mod options;

#[cfg(feature = "lapin_endpoint")]
/// Rabbit connections provided by [lapin](https://docs.rs/lapin/latest/lapin/index.html)
pub mod lapin;

#[cfg(feature = "amqprs_endpoint")]
/// Rabbit connections provided by [amqprs](https://docs.rs/amqprs/latest/amqprs/)
pub mod amqprs;

#[derive(clap::Args, Debug)]
pub struct RabbitCommand {
    /// URL of the rabbitmq server
    #[arg(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f"))]
    pub rabbit_addr: String,

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
