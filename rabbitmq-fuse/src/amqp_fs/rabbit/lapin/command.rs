use crate::{
    amqp_fs::rabbit::{lapin::RabbitExchnage, options::AuthMethod, RabbitCommand},
    cli::EndpointCommand,
};
use lapin_pool::lapin;
use miette::{IntoDiagnostic, WrapErr};

impl EndpointCommand for RabbitCommand {
    type Endpoint = RabbitExchnage;

    fn as_endpoint(&self) -> miette::Result<RabbitExchnage> {
        let conn_props = lapin::ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);
        let builder =
            lapin_pool::ConnectionBuilder::new(&self.rabbit_addr).with_properties(conn_props);
        let builder = if let Some(ref pem) = self.tls_options.ca_cert {
            builder.with_ca_pem(pem)
        } else {
            builder
        };
        let builder = if let Some(ref p12) = self.tls_options.key {
            let builder = builder.with_p12(p12);
            if let Some(ref passwd) = self.tls_options.password {
                builder.key_password(passwd)
            } else {
                builder
            }
        } else {
            builder
        };

        let opener = match self.options.amqp_auth {
            Some(AuthMethod::Plain) => builder
                .plain_auth(&self.options.plain_auth.amqp_user)
                .with_password(
                    &self
                        .options
                        .plain_auth
                        .password()
                        .into_diagnostic()?
                        .unwrap_or("guest".to_string()),
                )
                .opener()?,
            Some(AuthMethod::External) => builder.external_auth().password_prompt().opener()?,
            None => builder.opener()?,
        };

        let endpoint =
            Self::Endpoint::new(opener, &self.exchange, self.options.clone()).into_diagnostic()?;

        if self.options.immediate_connection {
            futures::executor::block_on(async { endpoint.test_connection().await })
                .with_context(|| format!("Failed to connect to {}", self.rabbit_addr))?;
        }
        Ok(endpoint)
    }
}
