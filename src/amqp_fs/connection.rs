use std::fs::File;
use std::io::Read;

use lapin::PromiseChain;
use lapin::{
    Connection,
    tcp::AMQPUriTcpExt,
};

use native_tls;


fn identity_from_file(p12_file:&str, password:&str) -> native_tls::Identity {
    let mut f = File::open(p12_file).expect("Unable to open client cert");
    let mut key_cert = Vec::new();
    f.read_to_end(&mut key_cert).expect("unable to read cleint cert");
    native_tls::Identity::from_pkcs12(&key_cert, password).unwrap()
}

fn ca_chain_from_file(pem_file:&str) -> native_tls::Certificate {
    let mut f = File::open(pem_file).expect("Unable to open ca chain");
    let mut ca_chain = Vec::new();
    f.read_to_end(&mut ca_chain).expect("Unable to read ca chain");
    native_tls::Certificate::from_pem(&ca_chain).expect("unable to parse certificate")
}


pub fn get_connection(addr: &str, conn_props: lapin::ConnectionProperties) -> PromiseChain<Connection> {
    let uri = addr.parse::<lapin::uri::AMQPUri>().unwrap();

    let handshake = uri.connect().and_then(|stream| {
        let mut tls_builder = native_tls::TlsConnector::builder();
        tls_builder.identity(identity_from_file(
            "../rabbitmq_ssl/tls-gen/basic/client/keycert.p12",
            "bunnies"
        ));
        tls_builder.add_root_certificate(
            ca_chain_from_file("../rabbitmq_ssl/tls-gen/basic/result/ca_certificate.pem")
        );
        tls_builder.danger_accept_invalid_hostnames(true);
        stream.into_native_tls(
            tls_builder.build().expect("tls config"),
            &uri.authority.host,
        )
    });

    Connection::connector(conn_props)(uri, handshake)

}
