
/// Where in the AMQP message to place the written line
#[derive(Clone, Debug, clap::ArgEnum)]
pub enum PublishStyle {
    /// The line is published in the message's body
    Body,

    /// The line is parsed and published in the messages header
    Header,
}

/// How to handle lines that can't be parse. Only used if
/// [LinePublishOptions::publish_in] is [PublishStyle::Header]
#[derive(Clone, Debug, clap::ArgEnum)]
pub enum UnparsableStyle {
    /// Failing to parse will return an error
    Error,
    /// Silently skip the message, allowed to return success
    Skip,
    /// Write the raw bytes to the key specified in [LinePublishOptions::parse_error_key]
    Key,
}

#[derive(Copy, Clone, Debug, clap::ArgEnum)]
pub enum AuthMethod {
    Plain,
    External,
}

/// Options that control how data is published per line
#[derive(clap::Args)]
#[derive(Clone, Debug)]
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
    pub amqp_auth:  Option<AuthMethod>,

    /// Username for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    #[clap(long, required_if_eq("amqp-auth", "plain"))]
    pub amqp_user: Option<String>,

    /// Password for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    #[clap(long, required_if_eq("amqp-auth", "plain"))]
    pub amqp_password: Option<String>,
}

impl Default for RabbitMessageOptions {
    fn default() -> Self {
        Self {
            publish_in: PublishStyle::Body,
            parse_error_key: None,
            handle_unparsable: UnparsableStyle::Error,
            amqp_auth: None,
            amqp_user: None,
            amqp_password: None,
        }
    }
}

impl From<AuthMethod> for Option<lapin::auth::SASLMechanism> {
    fn from(val: AuthMethod) -> Option<lapin::auth::SASLMechanism> {
        Some(
            match val {
                AuthMethod::Plain => lapin::auth::SASLMechanism::Plain,
                AuthMethod::External => lapin::auth::SASLMechanism::External,
            }
        )
    }
}
