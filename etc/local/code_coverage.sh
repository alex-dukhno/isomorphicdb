#!/bin/bash

docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin sh -c "apt-get update && apt-get -qq -y install llvm && cargo tarpaulin --all --ignore-tests -o Html --output-dir ./target/debug/coverage/"
