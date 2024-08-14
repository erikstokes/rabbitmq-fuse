#[cfg(feature = "prometheus_metrics")]
use opentelemetry::{metrics::Counter, KeyValue};

#[derive(Debug, Clone)]
/// A set of metrics for counting RabbitMQ messages sent, confirmed and rejected
pub(super) enum Metrics {
    None,
    #[cfg(feature = "prometheus_metrics")]
    #[doc(hidden)]
    Some {
        messages_sent: Counter<u64>,
        _confirms_recieved: Counter<u64>,
        reject_recieved: Counter<u64>,
        labels: Arc<Vec<KeyValue>>,
    },
}

impl Metrics {
    #[cfg(feature = "prometheus_metrics")]
    /// A new set of RabbitMQ related messages from the given meter.
    /// Tha passed labels will be attached to every metric set.
    pub fn new(
        meter: &opentelemetry::metrics::Meter,
        labels: std::sync::Arc<Vec<KeyValue>>,
    ) -> Self {
        Self::Some {
            messages_sent: meter
                .u64_counter("rabbit_messages_sent")
                .with_description(
                    "Number of messages sent to RabbitMQ, regardless of it the publish succeeded",
                )
                .init(),
            _confirms_recieved: meter
                .u64_counter("rabbit_confirms_received")
                .with_description("Number of publisher confirms recieved")
                .init(),
            reject_recieved: meter
                .u64_counter("rabbit_reject_received")
                .with_description("Number of publisher rejections recieved")
                .init(),

            labels,
        }
    }

    /// New empty metrics where all operatations are NOOPs
    pub fn none() -> Self {
        Self::None
    }

    /// Increment the sent counter
    #[allow(unused_variables)]
    pub fn add_sent(&self, value: u64) {
        #[cfg(feature = "prometheus_metrics")]
        if let Self::Some {
            messages_sent,
            labels,
            ..
        } = self
        {
            messages_sent.add(value, labels)
        }
    }

    /// Increment the confirmation recieved counter
    #[allow(unused_variables)]
    pub fn add_rejct(&self, value: u64) {
        #[cfg(feature = "prometheus_metrics")]
        if let Self::Some {
            reject_recieved,
            labels,
            ..
        } = self
        {
            reject_recieved.add(value, labels)
        }
    }
}
