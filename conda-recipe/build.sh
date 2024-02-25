#!/bin/bash

export OPENSSL_DIR=$PREFIX

cargo build --release --features=prometheus_metrics
cp target/release/fusegate $PREFIX/bin/
