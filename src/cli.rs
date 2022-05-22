use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
pub struct Args {
    pub(crate) mountpoint: PathBuf,

    #[clap(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f?auth_mechanism=external"))]
    pub(crate) rabbit_addr: String,

    #[clap(short, long, default_value_t = String::from(""))]
    pub(crate) exchange: String,

    #[clap(short, long)]
    pub(crate) key: String,

    #[clap(short, long)]
    pub(crate) cert: String,

    #[clap(short, long, default_value_t=16777216)]
    pub(crate) buffer_size: usize,
}
