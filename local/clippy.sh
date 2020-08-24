#!/bin/bash

cd "$(dirname "$0")"/..
set -ex

cargo clippy --all-targets --all-features -- -D warnings
