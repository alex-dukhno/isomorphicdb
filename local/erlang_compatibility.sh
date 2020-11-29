#!/bin/bash

docker run \
    --name erlang_compatibility_postgres \
    --rm \
    -p 5433:5432 \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -d postgres:12.4

sleep 1

rebar3 ct \
    --dir tests/erlang_compatibility/ \
    --logdir erlang_compatibility_logs/ \
    -v

docker stop erlang_compatibility_postgres
