use crate::{
    amqp_fs::rabbit::{
        lapin::{connection::Opener, RabbitExchnage},
        RabbitCommand,
    },
    cli::EndpointCommand,
};

impl EndpointCommand for RabbitCommand {
    type Endpoint = RabbitExchnage;

    fn as_endpoint(&self) -> anyhow::Result<RabbitExchnage> {
        let conn_props = lapin::ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);
        let connection_manager = Opener::from_command_line(self, conn_props)?;
        let endpoint =
            Self::Endpoint::new(connection_manager, &self.exchange, self.options.clone())?;

        if self.options.immediate_connection {
            futures::executor::block_on(async { endpoint.test_connection().await })?;
        }
        Ok(endpoint)
    }
}
