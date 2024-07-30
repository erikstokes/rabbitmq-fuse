#[cfg(feature = "prometheus_metrics")]
use crate::telemetry::OTelMetrics;

use enum_dispatch::enum_dispatch;

/// Service name to inject into all exported metrics
pub(crate) const SERVICE_NAME: &str = "fusegate";

/// Metrics collector. Will observe filesystem operations and record them in an appropriate
#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum EndpointMetrics {
    NoMetrics,
    #[cfg(feature = "prometheus_metrics")]
    OTelMetrics,
}

impl EndpointMetrics {
    /// A shim that doesn't actually collect any metrics
    pub fn no_metrics() -> Self {
        NoMetrics {}.into()
    }
}

#[enum_dispatch(EndpointMetrics)]
pub trait Metrics {
    fn observe_line(&self, line: &[u8]);
}

/// Shim that doesn't collect any matrics
#[derive(Debug, Clone)]
pub struct NoMetrics {}

impl Metrics for NoMetrics {
    fn observe_line(&self, _line: &[u8]) {}
}
