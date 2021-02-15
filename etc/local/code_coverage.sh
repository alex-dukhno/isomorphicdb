#!/bin/bash

docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin sh -c "cargo tarpaulin --ignore-tests -o Html --output-dir ./target/debug/coverage/"
