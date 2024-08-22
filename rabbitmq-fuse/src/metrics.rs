#[cfg(feature = "prometheus_metrics")]
use crate::telemetry::PromMetrics;

use enum_dispatch::enum_dispatch;

#[cfg(feature = "prometheus_metrics")]
/// Service name to inject into all exported metrics
pub const SERVICE_NAME: &str = "fusegate";

/// Metrics collector. Will observe filesystem operations and record them in an appropriate
#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum EndpointMetrics {
    NoMetrics,
    #[cfg(feature = "prometheus_metrics")]
    PromMetrics,
}

impl EndpointMetrics {
    /// A shim that doesn't actually collect any metrics
    pub fn no_metrics() -> Self {
        NoMetrics {}.into()
    }
}

/// Record metrics about various kinds of filesystem operations
#[enum_dispatch(EndpointMetrics)]
#[allow(unused_variables)]
pub trait Metrics {
    /// Record metrics about a published line
    fn observe_line(&self, line: &[u8]) {}
    /// Record metrics that an error in a filesystem task occured (e.g. a panic)
    fn observe_error(&self) {}
    /// Record that the filesystem was synchronized to the endpoint
    fn observe_sync(&self) {}
    /// Record that a filesystem operation was requested
    fn observe_op<T>(&self, op: &polyfuse::Operation<T>) {}
}

/// Shim that doesn't collect any matrics
#[derive(Debug, Clone)]
pub struct NoMetrics {}

impl Metrics for NoMetrics {}
