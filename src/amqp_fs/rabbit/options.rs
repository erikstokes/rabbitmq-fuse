/// Where in the AMQP message to place the written line
#[derive(Clone, Debug, clap::ArgEnum)]
pub enum PublishStyle {
    /// The line is published in the message's body
    Body,

    /// The line is parsed and published in the messages header
    Header,
}

/// How to handle lines that can't be parse. Only used if
/// [`LinePublishOptions::publish_in`] is [`PublishStyle::Header`]
#[derive(Clone, Debug, clap::ArgEnum)]
pub enum UnparsableStyle {
    /// Failing to parse will return an error
    Error,
    /// Silently skip the message, allowed to return success
    Skip,
    /// Write the raw bytes to the key specified in [LinePublishOptions::parse_error_key]
    Key,
}

/// Server authentication method
#[derive(Copy, Clone, Debug, clap::ArgEnum)]
pub enum AuthMethod {
    /// Plain username/password authentication
    Plain,
    /// External certificate based authentication
    External,
}

/// Username/password data for AMQP PLAIN auth method
#[derive(clap::Parser, Clone, Debug, Default)]
#[clap(group=clap::ArgGroup::new("amqp-plain-auth").multiple(false))]
pub struct AmqpPlainAuth {
    /// Password for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    #[clap(long, group = "amqp-plain-auth")]
    amqp_password: Option<String>,

    /// Plain text file containing the password. A single trailing newline will be removed
    #[clap(long, group = "amqp-plain-auth", conflicts_with = "amqp-password")]
    amqp_password_file: Option<std::path::PathBuf>,

    /// Username for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    #[clap(long, default_value = "guest", required_if_eq("amqp-auth", "plain"))]
    pub amqp_user: String,
}

/// Options that control how data is published per line
#[derive(clap::Args, Clone, Debug)]
pub struct RabbitMessageOptions {
    /// Decode lines and publish them in the message headers instead of the body
    #[clap(long, default_value = "body", arg_enum)]
    pub publish_in: PublishStyle,

    /// If [LinePublishOptions::publish_in] is [PublishStyle::Header],
    /// unparsable data will be stored in this single header key as
    /// raw bytes.  Otherwise this is ignored
    #[clap(long, required_if_eq("handle-unparsable", "key"))]
    pub parse_error_key: Option<String>,

    /// How to handle unparsable lines
    #[clap(long, default_value = "error", arg_enum)]
    pub handle_unparsable: UnparsableStyle,

    /// Authentication method for RabbitMQ server
    #[clap(long, arg_enum)]
    pub amqp_auth: Option<AuthMethod>,

    /// Username password for plain authentication
    #[clap(flatten)]
    pub plain_auth: AmqpPlainAuth,

    /// Immediatly open a RabbitMQ connection on mount
    #[clap(long)]
    pub immediate_connection: bool,
}

impl AmqpPlainAuth {
    /// Return the password for PLAIN auth, or None if no password is
    /// given. Returns an io error if the password file is given but
    /// can't be read
    pub fn password(&self) -> std::io::Result<Option<String>> {
        let pass = if let Some(pfile) = &self.amqp_password_file {
            let p = std::fs::read_to_string(pfile)?;
            match p.strip_suffix('\n') {
                Some(p) => Some(p.to_string()),
                None => Some(p.to_string()),
            }
        } else {
            self.amqp_password.clone()
        };
        Ok(pass)
    }
}

impl Default for RabbitMessageOptions {
    fn default() -> Self {
        Self {
            publish_in: PublishStyle::Body,
            parse_error_key: None,
            handle_unparsable: UnparsableStyle::Error,
            amqp_auth: None,
            plain_auth: AmqpPlainAuth::default(),
            immediate_connection: false,
        }
    }
}
