#!/bin/bash

cd "$(dirname "$0")"/..
set -ex

rustup toolchain install nightly --allow-downgrade --profile minimal --component rustfmt

cargo +nightly fmt
