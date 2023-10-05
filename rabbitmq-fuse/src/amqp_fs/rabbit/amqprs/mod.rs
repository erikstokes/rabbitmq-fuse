/// amqprs based [`crate::publisher::Endpoint`] and
/// [`crate::publisher::Publisher`] implementations
pub mod endpoint;

/// on_delivery_confirmation callback handling
pub mod returns;

pub use command::Command;
/// amqprs based Endpoint;
pub use endpoint::AmqpRsExchange;
pub use endpoint::AmqpRsPublisher;

/// Connection manager
pub mod connection;

/// Subcommand and command-line arguments
pub mod command;
