use miette::IntoDiagnostic;

use crate::cli::EndpointCommand;

#[derive(Debug, clap::ValueEnum, Clone)]
enum ConfirmLevel {
    /// Confirm the message when the lead server acknowledges it
    Lead,
    /// Confirm the message when all replicates acknowledge it
    All,
}

#[derive(clap::Args, Debug)]
pub struct KafkaCommand {
    /// URL of the Kafka bootstrap server
    #[clap(long, default_value = "localhost:9092")]
    pub kafka_url: String,

    #[clap(long, value_enum, default_value = "lead")]
    pub ack_level: ConfirmLevel,
}

impl EndpointCommand for KafkaCommand {
    type Endpoint = super::endpoint::KafkaEndpoint;

    fn as_endpoint(&self) -> miette::Result<Self::Endpoint> {
        Ok(Self::Endpoint::new(&self.kafka_url).into_diagnostic()?)
    }
}
