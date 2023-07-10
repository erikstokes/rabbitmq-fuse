//! Fuse filesytem mount that publishes to a `RabbitMQ` server
//!
//! Usage:
//! ```
//! rabbitmq-fuse --exchange="exchange" --rabbit-addr="amqp://rabbit.host/" mountpoint/
//! ```
//!
//! Creates a one level deep filesystem. Directories
//! correspond to routing keys and files are essentially meaningless.
//! Each line written to `dirctory/file` is converted to a `RabbitMQ`
//! `basic_publish` with a `routing_key` of "directory" and an
//! exchange specified at mount time
//!
//! Confirmations are enabled and failed publishes will send an error
//! back after calling `fsync(2)` or `fclose(2)`.
//!
//! Publishing and writing are done asynchronously unless the file is
//! opened with `O_SYNC`, in which case, writes become blocking and very
//! slow.
//!
//! As is the normal case with  `write(2)`, the number of bytes stored
//! in  the  filesytems internal  buffers  will  be returned,  with  0
//! indicating  an error  and  setting `errno`.  Successful writes  do
//! *not* mean the data was published to the server, only that it will
//! be  published.  Only  complete   lines  (separated  by  '\n')  are
//! published.  By default, incomplete   lines  are  not  published,   even  after
//! `fsync(2)`, but will be published when the file handle is released
//! (that is, when the last holder of the descriptor releasees it). This behavior can be modified via [`amqp_fs::options::LinePublishOptions::fsync`]
//!
//! All files have size 0 and appear empty, even after writes.
//! Directories may not contain subdirectories and the mount point can
//! contain no regular files. Only regular files and directories are
//! supported.

#![warn(clippy::all)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::single_match_else)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::self_named_module_files)]
#![warn(clippy::perf)]
#![deny(missing_docs)]
#![warn(clippy::missing_panics_doc)]

use anyhow::{Context, Result};
use os_pipe::PipeWriter;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;

use daemonize::Daemonize;

use polyfuse::KernelConfig;
use signal_hook::{consts::TERM_SIGNALS, iterator::Signals};

#[allow(unused_imports)]
use tracing::{debug, error, info, Level};

use clap::Parser;

mod amqp_fs;
mod cli;
/// FUSE session
mod session;

#[cfg(feature = "prometheus_metrics")]
mod prometheus;

#[cfg(feature = "prometheus_metrics")]
use crate::prometheus::{setup_metrics, MESSAGE_COUNTER};

use crate::amqp_fs::publisher::Endpoint;

/// RabbitMQ endpoint
#[cfg(feature = "amqprs_endpoint")]
type RabbitEndpoint = crate::amqp_fs::rabbit::amqprs::AmqpRsExchange;

/// RabbitMQ endpoint
#[cfg(not(feature = "amqprs_endpoint"))]
type RabbitEndpoint = crate::amqp_fs::rabbit::lapin::RabbitExchnage;

/// Result of the main function, or it's daemon child process. The return value should be the process id of the running process, otherwise an error message should be returned from the daemon to the child
#[derive(Serialize, Deserialize)]
struct DaemonResult {
    /// The PID of the the spawned deamon process, or `None` if the
    /// process failed to spawn
    pid: Option<u32>,
    /// Message returned from the child process if there is an error
    message: String,
}

// use crate::amqp_fs::rabbit::lapin::RabbitExchnage;
use crate::amqp_fs::Filesystem;

impl From<u32> for DaemonResult {
    fn from(value: u32) -> Self {
        Self {
            pid: Some(value),
            message: "".to_string(),
        }
    }
}

/// Send the results (pid and message string) back to the parent
/// process, or whatever is listening on the other end of the pipe
fn send_result_to_parent(result: impl Into<DaemonResult>, ready_send: &mut PipeWriter) {
    let encoded = bincode::serialize::<DaemonResult>(&result.into()).unwrap();
    let _ = ready_send.write_all(&encoded);
}

/// Run the child process. Trap any error that returns and send the message back to the parent
async fn run_child(args: cli::Args, ready_send: &mut PipeWriter) -> Result<()> {
    tokio_main(args, ready_send).await.map_err(|e| {
        // error!(error=?e, "Child process failed");
        let result = DaemonResult {
            pid: None,
            message: e.to_string(),
        };
        let encoded = bincode::serialize(&result).unwrap();
        let _ = ready_send.write_all(&encoded);
        e
    })
}

/// Mount the give path and processing kernel requests on it.
///
/// When fuse reports the mount has completed, `ready_send` will write
/// Ok(pid) with the process id of the mounting process. When forking,
/// this is how you learn the child's PID. Otherwise polyfuse will
/// report an `io::Error` and the raw OS error will be returned, or 0
/// if there is no such
async fn tokio_main(args: cli::Args, ready_send: &mut PipeWriter) -> Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .pretty()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env());

    match &args.logfile {
        Some(file) => {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(file)?;
            subscriber
                .with_writer(std::sync::Mutex::new(f))
                .with_ansi(false)
                .init();
        }
        None => {
            subscriber.with_writer(std::io::stderr).init();
        }
    };

    #[cfg(feature = "prometheus_metrics")]
    setup_metrics();

    debug!("Got command line arguments {:?}", args);

    if !args.mountpoint.is_dir() {
        anyhow::bail!(
            "mountpoint {} must be a directory",
            &args.mountpoint.display()
        );
    }

    info!(
        "Mounting RabbitMQ server {host} at {mount}/",
        mount = &args.mountpoint.display(),
        host = args.rabbit_addr
    );

    let mut fuse_conf = KernelConfig::default();
    fuse_conf
        .export_support(false)
        .max_background(args.fuse_opts.max_fuse_requests)
        .max_write(args.fuse_opts.fuse_write_buffer);

    let session = session::AsyncSession::mount(args.mountpoint.clone(), fuse_conf)
        .await
        .with_context(|| {
            format!(
                "Failed to create fuse session at {}",
                args.mountpoint.display()
            )
        })?;
    // .map_err(|e| {
    //     let context = format!("Failed to create fuse session at {}",
    //                           args.mountpoint.display());
    //     e.with_context(context)
    // })?;

    let fs: Arc<dyn amqp_fs::Mountable + Send + Sync> = if args.debug {
        let endpoint = amqp_fs::publisher::StdOut::from_command_line(&args)?;
        Arc::new(Filesystem::new(endpoint, args.options))
    } else {
        let endpoint = RabbitEndpoint::from_command_line(&args).with_context(|| {
            format!(
                "Failed to create rabbit endpoint {} -> {}",
                args.mountpoint.display(),
                args.rabbit_addr
            )
        })?;
        Arc::new(Filesystem::new(endpoint, args.options))
    };

    let for_sig = fs.clone();

    let mut signals = Signals::new(TERM_SIGNALS)?;
    let mount_path = args.mountpoint.clone();

    std::thread::spawn(move || {
        for sig in signals.forever() {
            info!("Got signal {}. Shutting down", sig);
            for_sig.stop();
            // this is to make fuse wake up and return a final
            // request, so that the poller loop doesn't hang. We
            // actually expect this to error, so don't check the result.
            let _ = std::fs::metadata(&mount_path);
        }
    });

    let run = fs.run(session);
    send_result_to_parent(std::process::id(), ready_send);
    run.await?;

    info!("Shutting down");

    Ok(())
}

#[doc(hidden)]
fn main() -> Result<()> {
    let args = cli::Args::parse();
    let (recv, mut send) = os_pipe::pipe()?;

    if args.daemon {
        let daemon = Daemonize::new().working_directory(std::env::current_dir()?);
        let proc = daemon.execute();
        if proc.is_parent() {
            // let mut result = vec![];
            // recv.read_to_end(&mut result)?;
            let result: DaemonResult = bincode::deserialize_from(recv)?;
            match result.pid {
                Some(pid) => println!("{pid}"),
                None => anyhow::bail!("Failed to launch mount daemon. {}", result.message),
            };
            return Ok(());
        }
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async { run_child(args, &mut send).await })
}

// #[cfg(test)]
// mod tests {
//     extern crate test;
//     use super::*;
//     use test::Bencher;

//     #[bench]
//     fn rabbit_write_lines(bencher: &mut Bencher) -> Result<()> {
//          let args = cli::Args::parse();

//         let endpoint = amqp_fs::publisher::rabbit::RabbitExchnage::from_command_line(&args);
//         let fs = Arc::new(amqp_fs::Filesystem::new(endpoint, &args));

//         Ok(())
//     }
// }
