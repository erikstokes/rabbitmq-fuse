use amqprs::tls::TlsAdaptor;

use crate::{
    amqp_fs::rabbit::options::{self, AmqpPlainAuth, AuthMethod},
    cli::{EndpointCommand, TlsArgs},
};

use super::{connection::ConnectionPool, AmqpRsExchange};

use miette::{IntoDiagnostic, Result};

/// [`crate::cli::EndpointCommand`] implmentation providing the command
/// line options for the RabbitMQ publishers
#[derive(clap::Args, Debug)]
pub struct Command {
    /// URL of the rabbitmq server
    #[arg(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f"))]
    pub rabbit_addr: String,

    /// Exchange to bind directories in the mount point to
    #[clap(short, long, default_value_t = String::from(""))]
    pub exchange: String,

    /// Authentication method for RabbitMQ server. If not given, the
    /// method will be taken from the URL parameters
    #[arg(long)]
    pub amqp_auth: Option<AuthMethod>,

    /// Username password for plain authentication
    #[command(flatten)]
    pub plain_auth: AmqpPlainAuth,

    /// Options to controll the message published
    #[command(flatten)]
    pub options: options::RabbitMessageOptions,

    /// Options to control TLS connections
    #[command(flatten)]
    pub(crate) tls_options: TlsArgs,
}

// /// Newtype wrapper to create a second rabbit command with the same
// /// arguments
// #[derive(Debug, clap::Args)]
// pub struct Command {
//     #[clap(flatten)]
//     args: RabbitCommand,
// }

impl EndpointCommand for Command {
    type Endpoint = AmqpRsExchange;

    fn as_endpoint(&self) -> Result<AmqpRsExchange> {
        let args = &self;
        let tls = TlsAdaptor::without_client_auth(
            Some(args.tls_options.ca_cert.as_ref().unwrap().as_ref()),
            "localhost".to_string(),
        )
        .into_diagnostic()?;
        let credentials = if let Some(super::super::options::AuthMethod::Plain) = args.amqp_auth {
            let plain = &args.plain_auth;
            plain.try_into().into_diagnostic()?
        } else {
            miette::bail!("Only plain authentication is supported with the 'amqprs' endpoint. Consider using the default 'rabbit' endpoint");
        };
        let opener = super::connection::Opener::new(
            &args.rabbit_addr.parse().into_diagnostic()?,
            credentials,
            tls,
        );
        let pool = ConnectionPool::builder(opener).build().into_diagnostic()?;
        // if self.args.options.immediate_connection {
        //     tracing::info!("Requested immediate connection test");
        //     let _: Result<()> = futures::executor::block_on(async {
        //         tracing::debug!("Opening test connection");
        //         let conn = pool.get().await.into_diagnostic().with_context(|| {
        //             format!("Failed to get connection to {}", self.args.rabbit_addr)
        //         })?;
        //         panic!("here");
        //         miette::ensure!(
        //             conn.open_channel(None).await.is_ok(),
        //             "Failed to get connection to {}",
        //             self.args.rabbit_addr
        //         );
        //         Ok(())
        //     });
        // }

        Ok(Self::Endpoint::new(
            pool,
            &args.exchange,
            args.options.clone(),
        ))
    }
}
