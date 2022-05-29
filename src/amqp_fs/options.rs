/// Options controling how buffered lines are published to the
/// RabbitMQ server


pub(crate) struct LinePublishOptions {
    /// Block after each line, waiting for the confirm
    pub sync: bool,

    /// Also publish partial lines, not ending in the delimiter
    pub allow_partial: bool,
}

pub(crate) struct WriteOptions {
    pub max_unconfirmed: u64
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self{
            max_unconfirmed: 10_000,
        }
    }
}
