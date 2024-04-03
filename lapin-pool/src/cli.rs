use crate::{
    builder::Result,
    options::{AmqpPlainAuth, AuthMethod, TlsArgs},
    Opener,
};

#[derive(Clone, Debug, clap::ValueEnum)]
enum AuthKind {
    Plain,
    External,
}

/// Clap derive command-line arguments to make rabbit connections.
///
/// Add this struct to a `clap::Parser` struct to generate the command
/// line options needed to form RabbitMQ connections.
///
/// # Example
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()>{
/// use clap::Parser;
///
/// #[derive(Debug, clap::Parser)]
/// /// Clap derive command-line arguments to make rabbit connections
/// struct Args {
///     /// Generates all the options needed to create connection openers
///     #[command(flatten)]
///     rabbit: lapin_pool::ConnectionArgs
///     // Any other clap configuration goes here
/// }
///
/// let args = Args::parse();
/// let opener = args.rabbit.connection_opener()?;
/// # Ok(()) }
/// ```
///
/// This will generate a command line help like
///
/// ```text
/// Clap derive command-line arguments to make rabbit connections
///
/// Usage: example [OPTIONS]
///
/// Options:
///   -r, --rabbit-addr <RABBIT_ADDR>
///           URL of the rabbitmq server [default: amqp://127.0.0.1:5672/%2f]
///       --key <KEY>
///           P12 formatted key
///       --ca-cert <CA_CERT>
///           PEM formatted CA certificate chain
///       --password <PASSWORD>
///           Password for key, if encrypted
///       --amqp-auth <AMQP_AUTH>
///           Authentication method for RabbitMQ server. If not given, the method will be taken from the URL parameters [possible values: plain, external]
///       --amqp-password <AMQP_PASSWORD>
///           Password for RabbitMQ server. Required if --amqp-auth is set to 'plain'
///       --amqp-password-file <AMQP_PASSWORD_FILE>
///           Plain text file containing the password. A single trailing newline will be removed
///       --amqp-user <AMQP_USER>
///           Username for RabbitMQ server. Required if --amqp-auth is set to 'plain' [default: guest]
///   -h, --help
///           Print help
/// ```
#[derive(Clone, Debug, clap::Args)]
pub struct ConnectionArgs {
    #[arg(short, long, default_value_t = String::from("amqp://127.0.0.1:5672/%2f"))]
    /// URL of the rabbitmq server
    rabbit_addr: String,

    #[command(flatten)]
    /// Options to control TLS connections
    tls_options: TlsArgs,

    /// Authentication method for RabbitMQ server. If not given, the
    /// method will be taken from the URL parameters
    #[arg(long)]
    amqp_auth: Option<AuthKind>,

    #[command(flatten)]
    /// Username password for plain authentication
    plain_auth: AmqpPlainAuth,
}

impl ConnectionArgs {
    /// The authentication method to use. If not given on the
    /// command-line, return None and use whatever is encoded in the
    /// URL
    fn auth(&self) -> Option<AuthMethod> {
        self.amqp_auth.as_ref().map(|kind| match kind {
            AuthKind::Plain => AuthMethod::Plain(self.plain_auth.clone()),
            AuthKind::External => AuthMethod::External,
        })
    }

    /// Convert the given command line arguments into an [`Opener`]
    pub fn connection_opener(&self) -> Result<Opener> {
        let conn_props = lapin::ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);
        let builder = crate::ConnectionBuilder::new(&self.rabbit_addr).with_properties(conn_props);
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

        let opener = match self.auth() {
            Some(AuthMethod::Plain(plain_auth)) => builder
                .plain_auth(&plain_auth.amqp_user)
                .with_password(&plain_auth.password()?.unwrap_or("guest".to_string()))
                .opener()?,
            Some(AuthMethod::External) => builder.external_auth().password_prompt().opener()?,
            None => builder.opener()?,
        };
        Ok(opener)
    }
}
