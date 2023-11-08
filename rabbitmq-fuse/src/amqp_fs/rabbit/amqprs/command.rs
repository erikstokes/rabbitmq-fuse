use amqprs::tls::TlsAdaptor;

use crate::{amqp_fs::rabbit::RabbitCommand, cli::EndpointCommand};

use super::{connection::ConnectionPool, AmqpRsExchange};

use miette::{IntoDiagnostic, Result};

/// Newtype wrapper to create a second rabbit command with the same
/// arguments
#[derive(Debug, clap::Args)]
pub struct Command {
    #[clap(flatten)]
    args: RabbitCommand,
}

impl EndpointCommand for Command {
    type Endpoint = AmqpRsExchange;

    fn as_endpoint(&self) -> Result<AmqpRsExchange> {
        let args = &self.args;
        let tls = TlsAdaptor::without_client_auth(
            Some(args.tls_options.ca_cert.as_ref().unwrap().as_ref()),
            "localhost".to_string(),
        )
        .into_diagnostic()?;
        let credentials =
            if let Some(super::super::options::AuthMethod::Plain) = args.options.amqp_auth {
                let plain = &args.options.plain_auth;
                plain.try_into().into_diagnostic()?
            } else {
                miette::bail!("Only plain authentication is supported");
            };
        let opener = super::connection::Opener::new(
            &args.rabbit_addr.parse().into_diagnostic()?,
            credentials,
            tls,
        );
        let pool = ConnectionPool::builder(opener).build().into_diagnostic()?;
        Ok(Self::Endpoint::new(
            pool,
            &args.exchange,
            args.options.clone(),
        ))
    }
}
