#!/bin/bash

./etc/local/code_coverage.sh \
  -p pg_result \
#  -p catalog \
  -p data_binary \
  -p data_scalar \
  -p data_definition_execution_plan \
  -p data_manipulation_operators \
  -p data_manipulation_query_plan \
  -p data_manipulation_query_result \
  -p data_manipulation_typed_queries \
  -p data_manipulation_typed_tree \
  -p data_manipulation_typed_values \
  -p data_manipulation_untyped_queries \
  -p data_manipulation_untyped_tree \
  -p definition \
#  -p types \
#  -p query_analyzer \
  -p query_planner \
  -p query_processing_type_check \
  -p query_processing_type_coercion \
  -p query_processing_type_inference \
  -p isomorphicdb
