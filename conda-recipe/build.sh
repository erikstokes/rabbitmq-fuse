#!/bin/bash

export OPENSSL_DIR=$PREFIX
export RUSTFLAGS='-Clink-arg=-fuse-ld=lld'
cargo build --release --features=prometheus_metrics
cp target/release/fusegate $PREFIX/bin/
