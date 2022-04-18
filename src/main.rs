#![warn(clippy::all)]

use anyhow::{ensure, Context as _, Result};
use std::{ path::PathBuf, sync::Arc};
use tokio::{
    task::{self, JoinHandle},
};

use polyfuse::{KernelConfig, Operation};

use tracing::{info, error, debug, Level};
use tracing_subscriber;

use clap::Parser;

mod amqp_fs;
mod session;


#[derive(Parser)]
struct Cli {
    mountpoint: PathBuf,
    #[clap(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f?auth_mechanism=external"))]
    rabbit_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // let mut args = pico_args::Arguments::from_env();
    let args = Cli::parse();

    // let mountpoint: PathBuf = args.free_from_str()?.context("missing mountpoint")?;
    ensure!(args.mountpoint.is_dir(), "mountpoint must be a directory");

    info!("Mounting RabbitMQ server {host} at {mount}/",
          mount=&args.mountpoint.display(), host=args.rabbit_addr);

    let session = session::AsyncSession::mount(args.mountpoint, KernelConfig::default()).await?;

    // let addr = "amqp://127.0.0.1:5671/%2f?auth_mechanism=external";


    let fs = Arc::new(amqp_fs::Rabbit::new(&args.rabbit_addr).await);

    while let Some(req) = session.next_request().await? {
        let fs = fs.clone();

        let _: JoinHandle<Result<()>> = task::spawn(async move {
            match req.operation()? {
                Operation::Lookup(op) => fs.lookup(&req, op).await?,
                Operation::Getattr(op) => fs.getattr(&req, op).await?,
                Operation::Read(op) => fs.read(&req, op).await?,
                Operation::Readdir(op) => fs.readdir(&req, op).await?,
                Operation::Write(op, data) => fs.write(&req, op, data).await?,
                _ => req.reply_error(libc::ENOSYS)?,
            }

            Ok(())
        });
    }

    Ok(())
}
