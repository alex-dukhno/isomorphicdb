#!/bin/bash

# This script is used for Github Action. It starts database service in backgound
# and runs Erlang tests.
# The second invocation of `cargo build` is tricky. It is blocked unless the
# first `cargo build` finishes. And `sleep 1` is used to wait for `cargo run`.
cargo build && \
  cargo run & \
  cargo build && \
  sleep 1 && \
  rebar3 ct --dir tests/erlang_client/ --logdir erlang_client_logs/ -v
