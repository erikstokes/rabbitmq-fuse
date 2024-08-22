#[cfg(feature = "prometheus_metrics")]
use crate::telemetry::{Counter, Family, Labels, Registry};

#[cfg(feature = "prometheus_metrics")]
use std::sync::Arc;

#[derive(Debug, Clone)]
/// A set of metrics for counting RabbitMQ messages sent, confirmed and rejected
pub(super) enum Metrics {
    None,
    #[cfg(feature = "prometheus_metrics")]
    #[doc(hidden)]
    Some {
        messages_sent: Family<Labels, Counter<u64>>,
        _confirms_recieved: Family<Labels, Counter<u64>>,
        reject_recieved: Family<Labels, Counter<u64>>,
        labels: Labels,
    },
}

impl Metrics {
    #[cfg(feature = "prometheus_metrics")]
    /// A new set of RabbitMQ related messages from the given meter.
    /// Tha passed labels will be attached to every metric set.
    pub fn new(registry: &mut Registry, labels: Labels) -> Self {
        let messages_sent: Family<Labels, Counter<u64>> = Default::default();
        let _confirms_recieved: Family<Labels, Counter<u64>> = Default::default();
        let reject_recieved: Family<Labels, Counter<u64>> = Default::default();
        registry.register(
            "rabbit_messages_sent",
            "Number of messages sent to RabbitMQ, regardless of it the publish succeeded",
            messages_sent.clone(),
        );
        registry.register(
            "rabbit_confirms_received",
            "Number of publisher confirms received",
            _confirms_recieved.clone(),
        );
        registry.register(
            "rabbit_reject_recieved",
            "Number of publisher rejections received",
            reject_recieved.clone(),
        );
        Self::Some {
            messages_sent,
            _confirms_recieved,
            reject_recieved,
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
            messages_sent.get_or_create(labels).inc_by(value);
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
            reject_recieved.get_or_create(labels).inc_by(value);
        }
    }
}
