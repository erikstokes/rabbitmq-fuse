use lapin_pool::ConnectionBuilder;
use miette::{IntoDiagnostic, Result};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::test]
async fn main() -> Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();
    let path = std::env::current_dir().unwrap();
    println!("The current directory is {}", path.display());

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
        .external_auth()
        .with_ca_pem("../test_all/tls-gen.new/basic/result/ca_certificate.pem")
        .with_p12("../test_all/tls-gen.new/basic/client_Lynx-167726/keycert.p12")
        // .key_password("bunnies")
        .opener()?;

    let connection = opener.get_connection().await.into_diagnostic()?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await.into_diagnostic()?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");
    Ok(())
}
