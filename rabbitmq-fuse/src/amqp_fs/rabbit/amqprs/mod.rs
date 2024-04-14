/// amqprs based [`crate::amqp_fs::publisher::Endpoint`] and
/// [`crate::amqp_fs::publisher::Publisher`] implementations
pub mod endpoint;

/// on_delivery_confirmation callback handling
pub mod returns;

pub use command::Command;
/// amqprs based Endpoint;
pub use endpoint::AmqpRsExchange;

/// Connection manager
pub mod connection;

/// Subcommand and command-line arguments
pub mod command;

/// Structs to control AMQP authentication
mod auth;
