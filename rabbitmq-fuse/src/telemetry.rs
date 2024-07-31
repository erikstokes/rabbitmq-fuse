use std::{sync::Arc, time::SystemTime};

// use opentelemetry_sdk::WithExportConfig;
// use opentelemetry_sdk::{trace, Resource};
// use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use opentelemetry::{
    metrics::{Counter, Histogram},
    KeyValue,
};
use opentelemetry_sdk::metrics::{self, SdkMeterProvider};

use axum::{extract::State, routing::get, Router};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio_util::sync::CancellationToken;

use tracing::info;

use crate::metrics::EndpointMetrics;
use prometheus::{Encoder, Registry, TextEncoder};

/// The type of the spawned metrics serving HTTP server.
type ServerTask = tokio::task::JoinHandle<std::io::Result<()>>;

#[derive(Debug, Clone)]
pub struct OTelMetrics {
    /// The number of messages published. A message is typically a line of
    /// text
    message_counter: Counter<u64>,

    /// Number of bytes published to the endpoint
    bytes_counter: Counter<u64>,

    /// Histogram of line lengths
    line_length_hist: Histogram<u64>,

    /// Labels to apply to all exposed metrics
    labels: Arc<Vec<KeyValue>>,
}

impl OTelMetrics {
    /// Create  a new set of enpoint metrics from the provided meter.
    pub fn new(meter: &opentelemetry::metrics::Meter, labels: Arc<Vec<KeyValue>>) -> Self {
        Self {
         message_counter: meter
            .u64_counter("messages_published")
            .with_description("Number of messages published to the endpoint")
            .init(),

        bytes_counter: meter
            .u64_counter("bytes_published")
            .with_description("Number of bytes published to the endpoint")
            .init(),

        line_length_hist:
        meter.u64_histogram("line_length")
            .with_description(
                "The lengths of written lines. This will also be the sizes of messages bodies, excluding the trailing endline"
            ).init(),
            labels,
        }
    }
}

impl crate::metrics::Metrics for OTelMetrics {
    fn observe_line(&self, line: &[u8]) {
        self.message_counter.add(1, self.labels.as_slice());
        self.bytes_counter
            .add(line.len().try_into().unwrap(), self.labels.as_slice());
        self.line_length_hist
            .record(line.len().try_into().unwrap(), self.labels.as_slice());
    }
}

/// Start a HTTP server to report metrics.
pub async fn start_metrics_server(
    metrics_addr: SocketAddr,
    registry: Registry,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    info!("Starting metrics server on {metrics_addr}");
    let metrics_app = Router::new()
        .route("/metrics", get(handler))
        .with_state(registry.clone());
    let listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    axum::serve(listener, metrics_app)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await?;
    info!("Metrics server shutting down");
    Ok(())
}

/// Encode the current metrics to a string and return it as a reply to
/// the prometheus scrape
async fn handler(State(registry): State<Registry>) -> String {
    info!(registry=?registry, "Handling metrics request");
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    info!(families=?metric_families);
    let mut result = Vec::new();
    encoder.encode(&metric_families, &mut result).unwrap();

    String::from_utf8(result).unwrap()
}

/// Initialize metrics and start the server. Metrics will be served on
/// localhost:8001/metrics
pub fn init_telemetry(
    mount_path: &str,
    cancel: CancellationToken,
) -> std::io::Result<(EndpointMetrics, ServerTask)> {
    let registry = Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .unwrap();

    let provider = SdkMeterProvider::builder()
        .with_reader(exporter)
        .with_view(
            metrics::new_view(
                metrics::Instrument::new().name("line_length*"),
                metrics::Stream::new().aggregation(metrics::Aggregation::ExplicitBucketHistogram {
                    boundaries: vec![
                        0.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 1048576.0,
                        4194304.0,
                    ],
                    record_min_max: true,
                }),
            )
            .expect("Unable to initialize metrics"),
        )
        .build();
    opentelemetry::global::set_meter_provider(provider);
    let meter = opentelemetry::global::meter(crate::metrics::SERVICE_NAME);

    let labels = Arc::new(vec![KeyValue {
        key: "mount_path".into(),
        value: mount_path.to_owned().into(),
    }]);
    let labels2 = labels.clone();
    meter
        .u64_observable_gauge("up")
        .with_callback(move |up| up.observe(1, labels2.as_slice()))
        .init();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time before unix epoch")
        .as_secs();
    let labels2 = labels.clone();
    meter
        .u64_observable_gauge("start_time")
        .with_callback(move |start| {
            start.observe(now, labels2.as_slice());
        })
        .init();

    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut result = Vec::new();
    encoder.encode(&metric_families, &mut result).unwrap();
    println!("{}", String::from_utf8(result).unwrap());

    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);

    let server: ServerTask =
        tokio::spawn(
            async move { start_metrics_server(metrics_addr, registry.clone(), cancel).await },
        );
    Ok((OTelMetrics::new(&meter, labels.clone()).into(), server))
    // Define a tracere
    // let tracer = opentelemetry_otlp::new_pipeline()
    //     .tracing()
    //     .with_exporter(exporter)
    //     .with_trace_config(
    //         trace::config().with_resource(Resource::new(vec![KeyValue::new(
    //             opentelemetry_semantic_conventions::resource::SERVICE_NAME,
    //             SERVICE_NAME.to_string(),
    //         )])),
    //     )
    //     .install_batch(opentelemetry::runtime::Tokio)
    //     .expect("Error: Failed to initialize the tracer.");

    // // Define a subscriber.
    // let subscriber = Registry::default();
    // // Level filter layer to filter traces based on level (trace, debug, info, warn, error).
    // let level_filter_layer = EnvFilter::from_default_env();
    // // Layer for adding our configured tracer.
    // let tracing_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    // // Layer for printing spans to stdout
    // let formatting_layer = BunyanFormattingLayer::new(SERVICE_NAME.to_string(), std::io::stdout);
    // global::set_text_map_propagator(TraceContextPropagator::new());

    // subscriber
    //     .with(level_filter_layer)
    //     .with(tracing_layer)
    //     .with(JsonStorageLayer)
    //     // .with(formatting_layer)
    //     .init()
}

#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn init_metrics() -> eyre::Result<()> {
        init_telemetry();
        MESSAGE_COUNTER.get().unwrap().add(1, &[]);

        Ok(())
    }
}
