#!/bin/bash

# from https://blog.rng0.io/how-to-do-code-coverage-in-rust

cargo install grcov
cargo clean
RUST_LOG=trace CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage -Cdebug-assertions=no' LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw' cargo test --features=lapin-pool/deadpool -- --include-ignored
~/.cargo/bin/grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html --excl-br-line "unreachable!" --excl-line "unreachable!"
