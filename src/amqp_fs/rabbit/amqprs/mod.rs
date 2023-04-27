/// amqprs based [`crate::publisher::Endpoint`] and
/// [`crate::publisher::Publisher`] implementations
pub mod endpoint;

/// on_delivery_confirmation callback handling
pub mod returns;

/// amqprs based Endpoint;
pub use endpoint::AmqpRsExchange;
