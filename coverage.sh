#!/bin/bash

set -e

# from https://blog.rng0.io/how-to-do-code-coverage-in-rust

# cargo install grcov
rm -r target/coverage/ || true
RUST_LOG=trace CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage -Cdebug-assertions=no -Clink-arg=-fuse-ld=lld' LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw' cargo test --features=lapin-pool/deadpool -- --include-ignored
grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html  --excl-line "unreachable!" --excl-br-line "^\s*((debug_)?assert(_eq|_ne)?!|#\[derive\()|unreachable!"

# cleanup the temp files
find . -name "*.profraw" -delete

# mangle the html files to point to a local copy of the css
HTML_BASE=target/coverage/html
ROOT=$(readlink -e $PWD)
cp bulma.min.css $HTML_BASE

find $HTML_BASE -name "*.html"  | while read file; do
    cd $(dirname "$file")
    rel_base=$(realpath -e --relative-to="$PWD" $ROOT/$HTML_BASE)
    ls $rel_base/bulma.min.css > /dev/null # will error if the paths don't exist
    sed -i -e "s|https://cdn.jsdelivr.net/npm/bulma@0.9.1/css|$rel_base/|" $(basename $file)
    cd $ROOT
done
