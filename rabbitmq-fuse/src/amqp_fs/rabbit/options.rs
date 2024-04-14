/// Where in the AMQP message to place the written line
#[derive(Clone, Debug, clap::ValueEnum)]
pub enum PublishStyle {
    /// The line is published in the message's body
    Body,

    /// The line is parsed and published in the messages header
    Header,
}

/// How to handle lines that can't be parse. Only used if
/// [`RabbitMessageOptions::publish_in`] is [`PublishStyle::Header`]
#[derive(Clone, Debug, clap::ValueEnum)]
pub enum UnparsableStyle {
    /// Failing to parse will return an error
    Error,
    /// Silently skip the message, allowed to return success
    Skip,
    /// Write the raw bytes to the key specified in [RabbitMessageOptions::parse_error_key]
    Key,
}

/// Options that control how data is published per line
#[derive(clap::Args, Clone, Debug)]
pub struct RabbitMessageOptions {
    /// Decode lines and publish them in the message headers instead of the body
    #[arg(long, default_value = "body", value_enum)]
    pub publish_in: PublishStyle,

    /// If [RabbitMessageOptions::publish_in] is [PublishStyle::Header],
    /// unparsable data will be stored in this single header key as
    /// raw bytes.  Otherwise this is ignored
    #[arg(long, required_if_eq("handle_unparsable", "key"))]
    pub parse_error_key: Option<String>,

    /// How to handle unparsable lines
    #[arg(long, default_value = "error", value_enum)]
    pub handle_unparsable: UnparsableStyle,

    /// Immediatly open a RabbitMQ connection on mount
    #[arg(long)]
    pub immediate_connection: bool,
}

impl Default for RabbitMessageOptions {
    fn default() -> Self {
        Self {
            publish_in: PublishStyle::Body,
            parse_error_key: None,
            handle_unparsable: UnparsableStyle::Error,
            immediate_connection: false,
        }
    }
}
