use clap::Parser;
use lapin_pool::ConnectionArgs;
use miette::{IntoDiagnostic, Result};

#[derive(clap::Parser, Clone, Debug)]
struct Args {
    #[command(flatten)]
    rabbit: ConnectionArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let opener = args.rabbit.connection_opener().into_diagnostic()?;
    let conn = opener.get_connection().await.into_diagnostic()?;
    let _chan = conn.create_channel().await.into_diagnostic()?;

    Ok(())
}
