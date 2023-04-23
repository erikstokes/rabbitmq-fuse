mod message;
/// Options that controll the `RabbitMQ` publisher and endpoint
pub mod options;

pub mod lapin;

#[cfg(feature = "amqprs_endpoint")]
pub mod amqprs;
