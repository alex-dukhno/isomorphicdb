#!/bin/bash

cd "$(dirname "$0")"/../..
set -ex

cargo +nightly fmt
