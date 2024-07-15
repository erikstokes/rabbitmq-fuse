//! Command line parser
use clap::Parser;
use std::{path::PathBuf, sync::Arc};

use crate::amqp_fs::rabbit::lapin::command::RabbitCommand;
use crate::amqp_fs::{self, options::WriteOptions};

/// Endpoint subcommands. Each varient corresponds to one specific
/// endpoint which can be run by passing it's name on to the
/// command-line application
#[derive(clap::Subcommand, Debug)]
#[allow(clippy::large_enum_variant)] // This is only used in place,
                                     // the convenience outweighs the large size
pub enum Endpoints {
    /// RabbitMQ endpoint that publishes to a fixed exchange
    Rabbit(RabbitCommand),

    #[cfg(feature = "amqprs_endpoint")]
    /// Experiment suppert for amqprs Rabbit library
    Amqprs(crate::amqp_fs::rabbit::amqprs::Command),

    /// Endpoint that publishes to a file (for debugging)
    Stream(amqp_fs::publisher::StreamCommand),

    /// The same as rabbit, but modifies the output to be consumed by amqp-node
    #[allow(non_camel_case_types)]
    Amqp_Node(crate::amqp_fs::rabbit::amqp_node::Command),

    #[cfg(feature = "kafka_endpoint")]
    /// Publish messages to a Kafka topic
    Kafka(crate::amqp_fs::kafka::command::KafkaCommand),
}

impl Endpoints {
    /// Create a mounted filesystem that writes the selected endpoint.
    /// This simply defers to the `get_mount` implmementation on each
    /// of [`Endpoints`]' varients
    pub(crate) fn get_mount(
        &self,
        write: &WriteOptions,
    ) -> miette::Result<Box<dyn amqp_fs::Mountable + Send + Sync + 'static>> {
        match self {
            Endpoints::Rabbit(ep) => ep.get_mount(write),
            #[cfg(feature = "amqprs_endpoint")]
            Endpoints::Amqprs(ep) => ep.get_mount(write),
            Endpoints::Stream(ep) => ep.get_mount(write),
            Endpoints::Amqp_Node(ep) => ep.get_mount(write),
            #[cfg(feature = "kafka_endpoint")]
            Endpoints::Kafka(ep) => ep.get_mount(write),
        }
    }
}
/// Trait the produces mountable filesystems from command-line arguments.
pub(crate) trait EndpointCommand {
    /// Type of endpoint to be created from this command
    type Endpoint: amqp_fs::publisher::Endpoint + 'static;

    /// Get the endpoint correspoinding to this command
    fn as_endpoint(&self) -> miette::Result<Self::Endpoint>;

    /// Get the filesystem mount for the corresponding endpoint
    fn get_mount(
        &self,
        write: &WriteOptions,
    ) -> miette::Result<Box<dyn amqp_fs::Mountable + Send + Sync + 'static>> {
        let ep = self.as_endpoint()?;
        Ok(Box::new(amqp_fs::Filesystem::new(ep, write.clone())))
    }
}

/// Options controlling TLS connections and certificate based
/// authentication
#[derive(Clone, Debug, clap::Args)]
pub(crate) struct TlsArgs {
    /// P12 formatted key
    #[clap(short, long)]
    pub(crate) key: Option<String>,

    /// PEM formatted certificate chain
    #[clap(short, long)]
    pub(crate) cert: Option<String>,

    /// PEM formatted CA certificate chain
    #[clap(long)]
    pub(crate) ca_cert: Option<String>,

    /// Password for key, if encrypted
    #[clap(long)]
    pub(crate) password: Option<String>,
}

/// Options controlling the interaction with the kernel
#[derive(Clone, Debug, clap::Args)]
pub(crate) struct FuseOptions {
    /// The maximum number of fuse background tasks to allow at once
    #[clap(long, default_value_t = 10)]
    pub(crate) max_fuse_requests: u16,

    /// The maximum size of the write buffer passed from the kernel
    #[clap(long, default_value_t = 0x400000)]
    pub(crate) fuse_write_buffer: u32,
}

/// Fuse filesytem that publishes to a `RabbitMQ` server
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Directory the filesystem will be mounted to
    pub(crate) mountpoint: PathBuf,

    /// Options controlling the behavior of `write(2)`
    #[clap(flatten)]
    pub(crate) options: amqp_fs::options::WriteOptions,

    /// Options controlling the interaction with FUSE
    #[clap(flatten)]
    pub(crate) fuse_opts: FuseOptions,

    // /// Run the mount in debug mode where writes go to stdout
    // #[clap(long)]
    // pub(crate) debug: bool,
    /// Background the process after startup
    #[clap(long)]
    pub(crate) daemon: bool,

    /// File to write logs to. Will log to stderr if not given
    #[clap(long)]
    pub(crate) logfile: Option<PathBuf>,

    /// Endpoint to publish to
    #[command(subcommand)]
    pub(crate) endpoint: Endpoints,
}

#[cfg(test)]
mod test {
    use super::*;
    use clap::Parser;
    #[test]
    fn parse_fuse_opts() {
        let argv = "-- --max-fuse-requests=100 --fuse-write-buffer=123123 test/  stream"; // " stream";
        let argv = argv.split_ascii_whitespace();
        let args = Args::parse_from(argv);
        assert_eq!(args.fuse_opts.fuse_write_buffer, 123123);
        assert_eq!(args.mountpoint, PathBuf::from("test/"));
        if let Endpoints::Stream(_) = args.endpoint {
        } else {
            unreachable!("wrong enpoint");
        }
        // assert_eq!(args.fuse_opts.max_fuse_requests, 100);
    }
}
