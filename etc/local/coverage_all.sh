#!/bin/bash

rustup toolchain install nightly

export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort"
export RUSTDOCFLAGS="-Cpanic=abort"

#cargo clean
cargo +nightly build

./etc/local/test_module.sh sql_engine/catalog
./etc/local/test_module.sh sql_engine/data_manipulation/typed_tree
./etc/local/test_module.sh sql_engine/entities/types
./etc/local/test_module.sh sql_engine/query_analyzer

./etc/local/code_coverage.sh \
  -p pg_result \
  -p data_binary \
  -p data_scalar \
  -p data_definition_execution_plan \
  -p data_manipulation_operators \
  -p data_manipulation_query_plan \
  -p data_manipulation_query_result \
  -p data_manipulation_typed_queries \
  -p data_manipulation_typed_values \
  -p data_manipulation_untyped_queries \
  -p data_manipulation_untyped_tree \
  -p definition \
  -p query_planner \
  -p query_processing_type_check \
  -p query_processing_type_coercion \
  -p query_processing_type_inference \
  -p isomorphicdb

grcov ./target/debug/ -s . -t html --llvm --branch --ignore-not-existing -o ./target/debug/coverage/
