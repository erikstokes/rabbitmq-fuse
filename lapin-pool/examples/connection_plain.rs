use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use lapin_pool::options::AuthMethod;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let auth = lapin_pool::options::AmqpPlainAuth {
        amqp_user: "rabbit".to_string(),
        amqp_password: Some("rabbitpw".to_string()),
        amqp_password_file: None,
    };

    let cmd = lapin_pool::connection::RabbitCommand {
        rabbit_addr: "amqp://127.0.0.1:5671/%2f".to_string(),
        amqp_auth: Some(AuthMethod::Plain(auth)),
        tls_options: lapin_pool::options::TlsArgs {
            key: None,
            cert: None,
            ca_cert: Some("../test_all/tls-gen/basic/result/ca_certificate.pem".to_string()),
            password: None,
        },
    };

    let opener = lapin_pool::connection::Opener::from_command_line(
        &cmd,
        lapin::ConnectionProperties::default(),
    )?;

    let connection = opener.get_connection().await?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");
    Ok(())
}
