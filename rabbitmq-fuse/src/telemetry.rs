use std::{sync::OnceLock, time::SystemTime};

// use opentelemetry_sdk::WithExportConfig;
// use opentelemetry_sdk::{trace, Resource};
// use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use opentelemetry::metrics::{Counter, MeterProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;

use axum::{extract::State, routing::get, Router};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tracing::info;

use prometheus::{Encoder, Registry, TextEncoder};

/// Service name to inject into all exported metrics
const SERVICE_NAME: &str = "fusegate";
/// The number of messages published. A message is typically a line of
/// text
pub(crate) static MESSAGE_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
/// Number of bytes published to the endpoint
pub(crate) static BYTES_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();

/// Start a HTTP server to report metrics.
pub async fn start_metrics_server(
    metrics_addr: SocketAddr,
    registry: Registry,
) -> std::io::Result<()> {
    info!("Starting metrics server on {metrics_addr}");
    let metrics_app = Router::new()
        .route("/metrics", get(handler))
        .with_state(registry.clone());
    let listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    axum::serve(listener, metrics_app).await?;
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
pub async fn init_telemetry() -> std::io::Result<()> {
    let registry = Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .unwrap();

    let provider = SdkMeterProvider::builder().with_reader(exporter).build();
    let meter = provider.meter(SERVICE_NAME);
    MESSAGE_COUNTER.get_or_init(|| {
        meter
            .u64_counter("messages_published")
            .with_description("Number of messages publsihed to the endpoint")
            .init()
    });

    BYTES_COUNTER.get_or_init(|| {
        meter
            .u64_counter("bytes_published")
            .with_description("Number of bytes publsihed to the endpoint")
            .init()
    });
    meter
        .u64_observable_gauge("up")
        .with_callback(|up| up.observe(1, &[]))
        .init();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time before unix epoch")
        .as_secs();
    meter
        .u64_observable_gauge("start_time")
        .with_callback(move |start| {
            start.observe(now, &[]);
        })
        .init();

    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut result = Vec::new();
    encoder.encode(&metric_families, &mut result).unwrap();
    println!("{}", String::from_utf8(result).unwrap());

    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);

    start_metrics_server(metrics_addr, registry.clone()).await
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
