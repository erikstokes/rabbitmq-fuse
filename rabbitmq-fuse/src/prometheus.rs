//! Collect and export prometheus metrics

use prometheus_client::{encoding::text::encode, metrics::counter::Counter, registry::Registry};

use lazy_static::lazy_static;

use axum::{extract::State, routing::get, Router};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use tracing::info;

lazy_static! {
    pub(crate) static ref MESSAGE_COUNTER: Counter = Counter::default();
}

/// Start a HTTP server to report metrics.
pub async fn start_metrics_server(
    metrics_addr: SocketAddr,
    registry: Registry,
) -> std::io::Result<()> {
    info!("Starting metrics server on {metrics_addr}");
    let registry = Arc::new(registry);
    let metrics_app = Router::new()
        .route("/metrics", get(handler))
        .with_state(registry);
    let listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    axum::serve(listener, metrics_app).await?;
    info!("Metrics server shutting down");
    Ok(())
}

/// Encode the current metrics to a string and return it as a reply to
/// the prometheus scrape
async fn handler(State(registry): State<Arc<Registry>>) -> String {
    let mut buf = String::new();
    encode(&mut buf, &registry).unwrap();
    buf
}

/// Setup a default registry and start a server on port 8001 to be scraped.
pub fn setup_metrics() {
    let mut registry = <Registry>::with_prefix("rabbitmq_fuse");

    registry.register(
        "messages",
        "How many messages have been sent",
        MESSAGE_COUNTER.clone(),
    );
    // Spawn a server to serve the OpenMetrics endpoint.
    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
    tokio::spawn(start_metrics_server(metrics_addr, registry));
}
