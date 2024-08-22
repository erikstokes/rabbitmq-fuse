use std::cell::OnceCell;
use std::sync::{Mutex, OnceLock};
use std::{sync::Arc, time::SystemTime};

use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
pub use prometheus_client::metrics::counter::{Atomic, Counter};
pub use prometheus_client::metrics::family::Family;
pub use prometheus_client::metrics::gauge::Gauge;
pub use prometheus_client::metrics::histogram::Histogram;
pub use prometheus_client::registry::{self, Registry};

use axum::{extract::State, routing::get, Router};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio_util::sync::CancellationToken;

use tracing::info;

use crate::metrics::EndpointMetrics;

pub(crate) static REGISTRY: OnceLock<Arc<Mutex<Registry>>> = OnceLock::new();

/// The type of the spawned metrics serving HTTP server.
type ServerTask = tokio::task::JoinHandle<std::io::Result<()>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet, Default)]
pub struct Labels {
    mount_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct OpLabels {
    mount_path: String,
    op: String,
}

#[derive(Debug, Clone)]
pub struct PromMetrics {
    /// The number of messages published. A message is typically a line of
    /// text
    message_counter: Family<Labels, Counter<u64>>,

    /// Number of bytes published to the endpoint
    bytes_counter: Family<Labels, Counter<u64>>,

    /// Histogram of line lengths
    line_length_hist: Family<Labels, Histogram>,

    /// Count of filesystem tasks that failed
    task_errors: Family<Labels, Counter<u64>>,

    /// Number of filesystem syncs
    sync_counter: Family<Labels, Counter<u64>>,

    /// Counter for filesystem operations (read, write, sync,...)
    op_counter: Family<OpLabels, Counter<u64>>,

    /// Labels to apply to all exposed metrics
    labels: Labels,
}

impl PromMetrics {
    /// Create  a new set of enpoint metrics from the provided meter.
    pub fn new(registry: &mut Registry, labels: Labels) -> Self {
        let out = Self {
            labels,
            line_length_hist: Family::new_with_constructor(|| {
                Histogram::new(
                    [
                        0.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 1048576.0,
                        4194304.0,
                    ]
                    .into_iter(),
                )
            }),
            message_counter: Default::default(),
            bytes_counter: Default::default(),
            task_errors: Default::default(),
            sync_counter: Default::default(),
            op_counter: Default::default(),
        };

        registry.register(
            "messages_published",
            "Number of messages published to the endpoint",
            out.message_counter.clone(),
        );
        registry.register(
            "bytes_published",
            "Number of bytes published to the endpoint",
            out.bytes_counter.clone(),
        );
        registry.register(
            "task_errors",
            "Number of filesystem tasks that have panicked or otherwise failed",
            out.task_errors.clone(),
        );
        registry.register(
            "file_system_sync",
            "Number of file system sync operations, either internally or because of explicit sync(2) calls" ,
            out.sync_counter.clone()
        );
        registry.register(
            "filesystem_ops",
            "Count of various kinds of filesystem operations (read, write, etc)",
            out.op_counter.clone(),
        );
        registry.register(
            "line_length",
            "The lengths of written lines. This will also be the sizes of messages bodies, excluding the trailing endline" ,
            out.line_length_hist.clone()
        );
        out
    }
}

impl crate::metrics::Metrics for PromMetrics {
    fn observe_line(&self, line: &[u8]) {
        self.message_counter.get_or_create(&self.labels).inc();
        self.bytes_counter
            .get_or_create(&self.labels)
            .inc_by(line.len().try_into().unwrap());
        self.line_length_hist
            .get_or_create(&self.labels)
            .observe(line.len() as f64);
    }

    fn observe_error(&self) {
        self.task_errors.get_or_create(&self.labels).inc();
    }

    #[doc = " Record that the filesystem was synchronized to the endpoint"]
    fn observe_sync(&self) {
        self.sync_counter.get_or_create(&self.labels).inc();
    }

    #[doc = " Record that a filesystem operation was requested"]
    fn observe_op<T>(&self, op: &polyfuse::Operation<T>) {
        let op_name = match op {
            polyfuse::Operation::Lookup(_) => "lookup",
            polyfuse::Operation::Getattr(_) => "getattr",
            polyfuse::Operation::Setattr(_) => "setattr",
            polyfuse::Operation::Readlink(_) => "readlink",
            polyfuse::Operation::Symlink(_) => "symlink",
            polyfuse::Operation::Mknod(_) => "mknod",
            polyfuse::Operation::Mkdir(_) => "mkdir",
            polyfuse::Operation::Unlink(_) => "unlink",
            polyfuse::Operation::Rmdir(_) => "rmdir",
            polyfuse::Operation::Rename(_) => "rename",
            polyfuse::Operation::Link(_) => "link",
            polyfuse::Operation::Open(_) => "open",
            polyfuse::Operation::Read(_) => "read",
            polyfuse::Operation::Write(_, _) => "write",
            polyfuse::Operation::Release(_) => "release",
            polyfuse::Operation::Statfs(_) => "statfs",
            polyfuse::Operation::Fsync(_) => "fsync",
            polyfuse::Operation::Setxattr(_) => "setxattr",
            polyfuse::Operation::Getxattr(_) => "getxattr",
            polyfuse::Operation::Listxattr(_) => "listxattr",
            polyfuse::Operation::Removexattr(_) => "removexattr",
            polyfuse::Operation::Flush(_) => "flush",
            polyfuse::Operation::Opendir(_) => "opendir",
            polyfuse::Operation::Readdir(_) => "readdir",
            polyfuse::Operation::Releasedir(_) => "releasedir",
            polyfuse::Operation::Fsyncdir(_) => "fsyncdir",
            polyfuse::Operation::Getlk(_) => "getlnk",
            polyfuse::Operation::Setlk(_) => "setlk",
            polyfuse::Operation::Flock(_) => "flock",
            polyfuse::Operation::Access(_) => "access",
            polyfuse::Operation::Create(_) => "create",
            polyfuse::Operation::Bmap(_) => "bmap",
            polyfuse::Operation::Fallocate(_) => "fallocate",
            polyfuse::Operation::CopyFileRange(_) => "copyfilerange",
            polyfuse::Operation::Poll(_) => "poll",
            polyfuse::Operation::Forget(_) => "forget",
            polyfuse::Operation::Interrupt(_) => "interrupt",
            polyfuse::Operation::NotifyReply(_, _) => "notifyreply",
            _ => todo!(),
        };

        let labels = OpLabels {
            mount_path: self.labels.mount_path.clone(),
            op: op_name.to_owned(),
        };
        self.op_counter.get_or_create(&labels).inc();
    }
}

/// Start a HTTP server to report metrics.
pub async fn start_metrics_server(
    metrics_addr: SocketAddr,
    registry: Arc<Mutex<Registry>>,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    info!("Starting metrics server on {metrics_addr}");
    let metrics_app = Router::new()
        .route("/metrics", get(handler))
        .with_state(registry);
    let listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    axum::serve(listener, metrics_app)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await?;
    info!("Metrics server shutting down");
    Ok(())
}

/// Encode the current metrics to a string and return it as a reply to
/// the prometheus scrape
async fn handler(State(registry): State<Arc<Mutex<Registry>>>) -> String {
    let mut buffer = String::new();
    info!(registry=?registry, "Handling metrics request");
    prometheus_client::encoding::text::encode(&mut buffer, &registry.lock().unwrap()).unwrap();

    buffer
}

/// Initialize metrics and start the server. Metrics will be served on
/// localhost:8001/metrics
pub fn init_telemetry(
    mount_path: &str,
    cancel: CancellationToken,
) -> std::io::Result<(EndpointMetrics, ServerTask)> {
    let registry = REGISTRY.get_or_init(|| Arc::new(Mutex::new(Registry::default())));

    // let exporter = opentelemetry_prometheus::exporter()
    //     .with_registry(registry.clone())
    //     .build()
    //     .unwrap();

    // let provider = SdkMeterProvider::builder()
    //     .with_reader(exporter)
    //     .with_view(
    //         metrics::new_view(
    //             metrics::Instrument::new().name("line_length*"),
    //             metrics::Stream::new().aggregation(metrics::Aggregation::ExplicitBucketHistogram {
    //                 boundaries: vec![
    //                     0.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 1048576.0,
    //                     4194304.0,
    //                 ],
    //                 record_min_max: true,
    //             }),
    //         )
    //         .expect("Unable to initialize metrics"),
    //     )
    //     .build();
    // opentelemetry::global::set_meter_provider(provider);
    // let meter = opentelemetry::global::meter(crate::metrics::SERVICE_NAME);

    let labels = Labels {
        mount_path: mount_path.to_owned(),
    };
    let labels2 = labels.clone();
    let up = Family::<Labels, Counter>::default();
    registry.lock().unwrap().register("up", "", up.clone());
    up.get_or_create(&labels).inc();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time before unix epoch")
        .as_secs();
    let start_time = Family::<Labels, Gauge>::default();
    registry
        .lock()
        .unwrap()
        .register("start_time", "", start_time.clone());
    start_time
        .get_or_create(&labels)
        .set(now.try_into().unwrap());

    // let encoder = TextEncoder::new();
    // let metric_families = registry.gather();
    // let mut result = Vec::new();
    // encoder.encode(&metric_families, &mut result).unwrap();
    // println!("{}", String::from_utf8(result).unwrap());

    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);

    let metrics = PromMetrics::new(&mut registry.lock().unwrap(), labels.clone()).into();

    let server: ServerTask =
        tokio::spawn(
            async move { start_metrics_server(metrics_addr, registry.clone(), cancel).await },
        );
    Ok((metrics, server))
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
