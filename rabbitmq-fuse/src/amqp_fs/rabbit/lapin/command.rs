use crate::{
    amqp_fs::rabbit::{lapin::RabbitExchnage, options},
    cli::EndpointCommand,
};
use miette::{IntoDiagnostic, WrapErr};

/// [`crate::cli::EndpointCommand`] implmentation providing the command
/// line options for the RabbitMQ publishers
#[derive(clap::Args, Debug)]
pub struct RabbitCommand {
    /// RabbitMQ connection options
    #[command(flatten)]
    rabbit_args: lapin_pool::ConnectionArgs,

    /// Exchange to bind directories in the mount point to
    #[clap(short, long, default_value_t = String::from(""))]
    pub exchange: String,

    /// Options to control the message published
    #[command(flatten)]
    pub options: options::RabbitMessageOptions,
}

impl EndpointCommand for RabbitCommand {
    type Endpoint = RabbitExchnage;

    fn as_endpoint(&self) -> miette::Result<RabbitExchnage> {
        let opener = self.rabbit_args.connection_opener()?;

        let endpoint =
            Self::Endpoint::new(opener, &self.exchange, self.options.clone()).into_diagnostic()?;

        if self.options.immediate_connection {
            futures::executor::block_on(async { endpoint.test_connection().await }).with_context(
                || format!("Failed to connect with arguments {:?}", self.rabbit_args),
            )?;
        }
        Ok(endpoint)
    }
}
