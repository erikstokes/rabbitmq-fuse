/// Module grouping endpoints that publish data to RabbitMQ. Two
/// endpoints are supported, [lapin](https://github.com/amqp-rs/lapin)
/// and [amqprs](https://github.com/gftea/amqprs)
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

/// Publish messages in a restricted format that amqp-node can consume
pub mod amqp_node;

/// [`crate::cli::EndpointCommand`] implmentation providing the command
/// line options for the RabbitMQ publishers
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
