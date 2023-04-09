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
#![deny(missing_docs)]

#![feature(allocator_api)]

use anyhow::Result;
use std::sync::Arc;

use pipe_channel::{Sender, channel};
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

use crate::amqp_fs::publisher::Endpoint;
use crate::amqp_fs::rabbit::endpoint::RabbitExchnage;
use crate::amqp_fs::Filesystem;

/// Mount the give path and processing kernel requests on it.
///
/// When fuse reports the mount has completed, `ready_send` will write
/// Ok(pid) with the process id of the mounting process. When forking,
/// this is how you learn the child's PID. Otherwise polyfuse will
/// report an `io::Error` and the raw OS error will be returned, or 0
/// if there is no such
async fn tokio_main(args: cli::Args, mut ready_send:Sender<std::result::Result<u32, libc::c_int>> ) -> Result<()> {

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
        },
        None => {
            subscriber.with_writer(std::io::stderr)
                .init();
        }
    };

    debug!("Got command line arguments {:?}", args);

    if !args.mountpoint.is_dir() {
        eprintln!("mountpoint {} must be a directory",
                  &args.mountpoint.display());
        std::process::exit(1);
    }

    info!(
        "Mounting RabbitMQ server {host} at {mount}/",
        mount = &args.mountpoint.display(),
        host = args.rabbit_addr
    );

    let mut fuse_conf = KernelConfig::default();
    fuse_conf.export_support(false);
    let session =  session::AsyncSession::mount(args.mountpoint.clone(), fuse_conf).await
        .map_err(|e| {
            error!("Failed to mount {}", args.mountpoint.display());
            ready_send.send(Err(e.raw_os_error().unwrap_or(0))).unwrap();
            e
        })?;

    let fs: Arc<dyn amqp_fs::Mountable + Send + Sync> = if args.debug {
        let endpoint = amqp_fs::publisher::StdOut::from_command_line(&args)?;
        Arc::new(Filesystem::new(endpoint, args.options))
    } else {
        let endpoint = RabbitExchnage::from_command_line(&args)?;
        Arc::new(Filesystem::new(endpoint, args.options))
    };

    let for_ctrlc = fs.clone();
    ctrlc::set_handler(move || {
        for_ctrlc.stop();
    })
    .expect("Setting signal handler");
    let for_sig = fs.clone();

    let mut signals = Signals::new(TERM_SIGNALS)?;

    std::thread::spawn(move || {
        for sig in signals.forever() {
            info!("Got signal {}. Shutting down", sig);
            for_sig.stop();
        }
    });

    let run = fs.run(session);
    ready_send.send(Ok(std::process::id()))?;

    run.await?;

    info!("Shutting down");

    Ok(())
}

#[doc(hidden)]
fn main() -> Result<()> {
    let args = cli::Args::parse();
    let (send, mut recv) = channel();

    if args.daemon {
        let daemon = Daemonize::new()
            .working_directory(std::env::current_dir()?)
            .exit_action(move || {
                let pid = recv.recv().unwrap();
                match pid {
                    Ok(pid) => println!("{pid}"),
                    Err(e) => eprintln!("Failed to launch mount daemon. Error code {e}"),
                };
            });
        daemon.start()?;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        tokio_main(args, send).await
    })
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
