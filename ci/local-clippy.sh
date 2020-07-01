#!/bin/bash

cd "$(dirname "$0")"/..
set -ex

rustup toolchain install nightly --allow-downgrade --profile minimal --component clippy

cargo +nightly clippy --all-targets --all-features -- -D warnings
