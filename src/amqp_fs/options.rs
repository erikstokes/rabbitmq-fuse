//! Options controling how buffered lines are published to the
//! Endpoint


/// Select the behavior of `fsync(3)` when there is partial data in
/// the buffer. This can happen, for example, if multiple threads are
/// writing to the same descriptor and one calls `fsync`. Specify if
/// you want the partially accumlated lines to be published, or to
/// remain in the internal buffer waiting on future writes.
#[derive(Clone, Debug, clap::ArgEnum)]
pub enum SyncStyle {
    /// Always publish all data, including incomplete lines
    AllowPartialLines,
    /// Only publish full lines, leaving partial data in the internal
    /// buffers
    CompleteLinesOnly,
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

    /// Time in miliseconds to wait for files to open
    #[clap(long, default_value_t=0)]
    pub open_timeout_ms: u64,

    /// Whether to publish incomplete data on fsync calls
    #[clap(long, default_value="allow-partial-lines", arg_enum)]
    pub fsync: SyncStyle,

    /// For debugging. Block after each line, waiting for the confirm. This is global
    /// for all writes and is equivalent to opening every file with
    /// `O_SYNC | O_DIRECT`
    #[clap(long)]
    pub sync: bool,

}

impl std::str::FromStr for SyncStyle {
    type Err = clap::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "allow-partial-lines" => Ok(SyncStyle::AllowPartialLines),
            "complete-lines-only" => Ok(SyncStyle::CompleteLinesOnly),
            _ => Err(clap::Error::raw(
                clap::ErrorKind::InvalidValue,
                "Unknown value".to_string(),
            )),
        }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            sync: false,
            max_buffer_bytes: 16777216,
            max_unconfirmed: 10_000,
            // line_opts: LinePublishOptions::default(),
            open_timeout_ms: 0,
            fsync: SyncStyle::AllowPartialLines,
        }
    }
}

impl SyncStyle {
    pub fn allow_partial(&self) -> bool {
        match self {
            SyncStyle::AllowPartialLines => true,
            SyncStyle::CompleteLinesOnly => false
        }
    }
}
