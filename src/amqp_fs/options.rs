/// Options controling how buffered lines are published to the
/// RabbitMQ server



/// Options the control how data is published per line
pub(crate) struct LinePublishOptions {
    /// Block after each line, waiting for the confirm. This is global
    /// for all writes and is equivalent to opening the file with
    /// `O_SYNC | O_DIRECT`
    pub sync: bool,

    /// Decode lines and publish them in the message headers instead of the body
    pub in_headers: bool,

    /// If [LinePublishOptions::in_headers] is true, unparsable data will be stored in this
    /// single header key as raw bytes
    pub parse_error_key: Option<String>

}

/// Options the control writting globally for an open file descriptor
pub(crate) struct WriteOptions {
    pub max_unconfirmed: u64,
    pub max_buffer_bytes: usize,
    pub line_opts: LinePublishOptions,
}

impl Default for LinePublishOptions {
    fn default() -> Self {
        Self {
            sync: false,
            in_headers: false,
            parse_error_key: None
        }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self{
            max_buffer_bytes: 16777216,
            max_unconfirmed: 10_000,
            line_opts: LinePublishOptions::default(),
        }
    }
}
