//! Options controling how buffered lines are published to the
//! RabbitMQ server

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

/// Options the control how data is published per line
#[derive(Clone, Debug, clap::Args)]
pub(crate) struct LinePublishOptions {
    /// For debugging. Block after each line, waiting for the confirm. This is global
    /// for all writes and is equivalent to opening every file with
    /// `O_SYNC | O_DIRECT`
    #[clap(long)]
    pub sync: bool,

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
}

/// Options the control writting globally for an open file descriptor
#[derive(Clone, Debug, clap::Args)]
pub(crate) struct WriteOptions {
    /// Number of unconfirmed messages to publsih before syncing
    #[clap(long, default_value_t = 10_000)]
    pub max_unconfirmed: u64,

    /// Size of open file's internal buffer. 0 means unlimited
    #[clap(long, default_value_t = 16777216)]
    pub max_buffer_bytes: usize,

    /// Options to control how lines are published
    #[clap(flatten)]
    pub line_opts: LinePublishOptions,
}

impl std::str::FromStr for UnparsableStyle {
    type Err = clap::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "key" => Ok(UnparsableStyle::Key),
            "error" => Ok(UnparsableStyle::Error),
            "skip" => Ok(UnparsableStyle::Skip),
            _ => Err(clap::Error::raw(
                clap::ErrorKind::InvalidValue,
                "Unknown value".to_string(),
            )),
        }
    }
}

impl Default for LinePublishOptions {
    fn default() -> Self {
        Self {
            sync: false,
            publish_in: PublishStyle::Body,
            parse_error_key: None,
            handle_unparsable: UnparsableStyle::Error,
        }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 16777216,
            max_unconfirmed: 10_000,
            line_opts: LinePublishOptions::default(),
        }
    }
}
