#!/bin/bash

# from https://blog.rng0.io/how-to-do-code-coverage-in-rust

cargo install grcov
RUST_LOG=debug CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw' cargo test
~/.cargo/bin/grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html
