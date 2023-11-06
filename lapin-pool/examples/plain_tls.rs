use lapin_pool::ConnectionBuilder;
use miette::{IntoDiagnostic, Result};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
        .tls()
        .plain_auth("rabbit")
        .with_password("rabbitpw")
        .with_p12("../test_all//tls-gen/basic/result/client_Lynx-167726_key.p12")
        .key_password("bunnies")
        .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
        .opener()?;

    let connection = opener.get_connection().await.into_diagnostic()?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await.into_diagnostic()?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");
    Ok(())
}
