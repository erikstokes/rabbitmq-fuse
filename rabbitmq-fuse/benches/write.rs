use assert_cmd::prelude::*;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use criterion::BenchmarkId;
use criterion::Throughput;

use std::io::Read;
use std::process::Stdio;
use std::{
    fs::File,
    io::Write,
    process::{Child, Command},
};
use tempfile::TempDir;

// /// Runs `podman run --rm  -p 5672:5672  -p15672:15672 rabbitmq:3-management`
// fn start_rabbit() -> Result<Child> {
//     let rabbit = Command::new("podman")
//         .arg("run")
//         .arg("--rm")
//         .args(["-p", "5672:5672"])
//         .arg("rabbitmq:3-management")
//         .spawn()?;
//     // Wait for the server to finish starting and the port to become
//     // available
//     loop {
//         let stream = std::net::TcpStream::connect("127.0.0.1:5672");
//         if stream.is_ok() {
//             break;
//         }
//         println!("Waiting for rabbit to start");
//         std::thread::sleep(std::time::Duration::from_secs(1));
//     }

//     Ok(rabbit)
// }

/// Start a fuse mount and return the mount process and the mount path
fn start_mount(amqp_url: &str) -> eyre::Result<(i32, TempDir)> {
    let mount_dir = TempDir::with_prefix("fusegate")?;
    let mut mount = Command::cargo_bin("fusegate")?
        .arg(mount_dir.path().to_str().unwrap())
        .arg("--daemon")
        .arg("rabbit")
        .args(["--rabbit-addr", amqp_url])
        .arg("--immediate-connection")
        .stdout(Stdio::piped())
        .spawn()?;
    let mut out = String::new();
    // Wait for the mount to finish starting
    mount.wait().unwrap();
    mount
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut out)
        .unwrap();
    let pid = out.strip_suffix('\n').unwrap().parse().unwrap();
    Ok((pid, mount_dir))
}

/// The function we actually benchmark. Really pushing everything
/// through fuse
fn write(fp: &mut File, data: &[u8]) {
    let written = fp.write(data).unwrap();
    assert_eq!(written, data.len());
}

fn rabbit_is_up(amqp_url: &str) -> bool {
    let url = url::Url::parse(amqp_url).unwrap();
    let stream =
        std::net::TcpStream::connect(format!("{}:{}", url.host().unwrap(), url.port().unwrap()));
    stream.is_ok()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    static KB: usize = 1024;
    let amqp_url = std::env::var("RABBITMQ_URL").unwrap_or("amqp://127.0.0.1:5672".to_string());
    assert!(rabbit_is_up(&amqp_url),
            "Unable to connect to {}.\n Check that the server is started and set the RABBITMQ_URL environment variable",
            amqp_url
    );
    let (_pid, tmpdir) = start_mount(&amqp_url).expect("Unable to create mount");
    println!("Mounted {tmpdir:?}");
    let dir = tmpdir.path().join("test");
    std::fs::create_dir(&dir).unwrap();
    // let mut rabbit = start_rabbit().expect("Unable to use podman to start rabbit server");
    let file = dir.join("test.log");
    println!("Writing to file {file:?}");
    let mut fp = File::create(file).unwrap();
    let mut group = c.benchmark_group("fuse_write");
    for size in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB].iter() {
        let mut data: Vec<u8> = vec![b'-'; *size - 1];
        data.push(b'\n');
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| write(&mut fp, &data));
        });
        fp.sync_data()
            .expect("Write failed. Does the queue named 'test' exist?");
    }
    std::mem::drop(fp);
    Command::new("fusermount")
        .args(["-u", tmpdir.path().as_os_str().to_str().unwrap()])
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
    // mount.wait().unwrap();
    std::mem::drop(tmpdir); // make sure tmpdir lives until here
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
