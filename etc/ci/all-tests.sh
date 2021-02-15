#!/bin/bash

# POSTGRES
./etc/ci/test_module.sh postgres/pg_result

# SQL-ENGINE
./etc/ci/test_module.sh sql_engine/catalog
./etc/ci/test_module.sh sql_engine/data/binary
./etc/ci/test_module.sh sql_engine/data/scalar
./etc/ci/test_module.sh sql_engine/data_definition/execution_plan
./etc/ci/test_module.sh sql_engine/data_manipulation/operators
./etc/ci/test_module.sh sql_engine/data_manipulation/query_plan
./etc/ci/test_module.sh sql_engine/data_manipulation/query_result
./etc/ci/test_module.sh sql_engine/data_manipulation/typed_queries
./etc/ci/test_module.sh sql_engine/data_manipulation/typed_tree
./etc/ci/test_module.sh sql_engine/data_manipulation/typed_values
./etc/ci/test_module.sh sql_engine/data_manipulation/untyped_queries
./etc/ci/test_module.sh sql_engine/data_manipulation/untyped_tree
./etc/ci/test_module.sh sql_engine/entities/definition
./etc/ci/test_module.sh sql_engine/entities/types
./etc/ci/test_module.sh sql_engine/query_analyzer
./etc/ci/test_module.sh sql_engine/query_planner
./etc/ci/test_module.sh sql_engine/query_processing/type_check
./etc/ci/test_module.sh sql_engine/query_processing/type_coercion
./etc/ci/test_module.sh sql_engine/query_processing/type_inference
./etc/ci/test_module.sh sql_engine/query_processing/type_inference

cargo test
