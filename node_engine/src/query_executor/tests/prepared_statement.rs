// Copyright 2020 - 2021 Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[rstest::rstest]
fn prepare_execute_and_deallocate(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan (smallint, smallint, smallint) as insert into schema_name.table_name values ($1, $2, $3)",
        vec![OutboundMessage::StatementPrepared, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "execute foo_plan (123, 456, 789)",
        vec![OutboundMessage::RecordsInserted(1), OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "deallocate foo_plan",
        vec![OutboundMessage::StatementDeallocated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "select * from schema_name.table_name",
        vec![
            OutboundMessage::RowDescription(vec![
                ("column_1".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
                ("column_3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![string("123"), string("456"), string("789")]),
            OutboundMessage::RecordsSelected(1),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
fn deallocate_statement_that_does_not_exists(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (column_1 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "deallocate foo_plan",
        vec![
            QueryError::prepared_statement_does_not_exist("foo_plan").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
fn execute_deallocated_prepared_statement(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (column_1 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan (smallint) as insert into schema_name.table_name values ($1)",
        vec![OutboundMessage::StatementPrepared, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "deallocate foo_plan",
        vec![OutboundMessage::StatementDeallocated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "execute foo_plan(123)",
        vec![
            QueryError::prepared_statement_does_not_exist("foo_plan").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
#[ignore] // TODO: custom/unsupported types_old is not supported on parser level
fn prepare_with_wrong_type(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan (i, j, k) as insert into schema_name.table_name values ($1, $2, $3)",
        vec![QueryError::type_does_not_exist("i").into(), OutboundMessage::ReadyForQuery],
    );
}

#[rstest::rstest]
#[ignore]
// TODO: number of parameters is not counter
// TODO: correctness of param indexes is not checked
fn prepare_with_indeterminate_type(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan (smallint, smallint) as insert into schema_name.table_name values (1, $9)",
        vec![QueryError::indeterminate_parameter_data_type(3).into(), OutboundMessage::ReadyForQuery],
    );
}

#[rstest::rstest]
#[ignore]
// TODO: no parameter types_old is not supported
// TODO: type inference is not properly implemented
fn prepare_assign_operation_for_all_columns_analysis(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan as insert into schema_name.table_name values ($2, $3, $1)",
        vec![OutboundMessage::StatementPrepared, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "execute foo_plan(123, 456, 789)",
        vec![OutboundMessage::RecordsInserted(1), OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "deallocate foo_plan",
        vec![OutboundMessage::StatementDeallocated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "select * from schema_name.table_name",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec!["456".to_owned(), "789".to_owned(), "123".to_owned()]),
            OutboundMessage::RecordsSelected(1),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
#[ignore]
// TODO: no parameter types_old is not supported
// TODO: type inference is not properly implemented
fn prepare_assign_operation_for_specified_columns_analysis(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan as insert into schema_name.table_name (COL3, COL2, col1) values ($1, $2, $3)",
        vec![OutboundMessage::StatementPrepared, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "execute foo_plan(123, 456, 789)",
        vec![OutboundMessage::RecordsInserted(1), OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "deallocate foo_plan",
        vec![OutboundMessage::StatementDeallocated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "select * from schema_name.table_name",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec!["789".to_owned(), "456".to_owned(), "123".to_owned()]),
            OutboundMessage::RecordsSelected(1),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
#[ignore]
// TODO: no parameter types_old is not supported
// TODO: type inference is not properly implemented
fn prepare_reassign_operation_for_all_rows(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6)",
        vec![OutboundMessage::RecordsInserted(2), OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan as update schema_name.table_name set col3 = $1, COL1 = $2",
        vec![OutboundMessage::StatementPrepared, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "execute foo_plan(777, 999)",
        vec![OutboundMessage::RecordsUpdated(2), OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "deallocate foo_plan",
        vec![OutboundMessage::StatementDeallocated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "select * from schema_name.table_name",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec!["999".to_owned(), "2".to_owned(), "777".to_owned()]),
            OutboundMessage::DataRow(vec!["999".to_owned(), "5".to_owned(), "777".to_owned()]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
#[ignore]
// TODO: no parameter types_old is not supported
// TODO: type inference is not properly implemented
fn prepare_reassign_operation_for_specified_rows(with_schema: TransactionManager) {
    let mut query_plan_cache = QueryPlanCache::default();
    let txn = with_schema.start_transaction();

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint)",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6)",
        vec![OutboundMessage::RecordsInserted(2), OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "prepare foo_plan as update schema_name.table_name set col2 = $1 where COL3 = $2",
        vec![OutboundMessage::StatementPrepared, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "execute foo_plan(999, 6)",
        vec![OutboundMessage::RecordsUpdated(2), OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "deallocate foo_plan",
        vec![OutboundMessage::StatementDeallocated, OutboundMessage::ReadyForQuery],
    );

    assert_cached_statement(
        &mut query_plan_cache,
        &txn,
        "select * from schema_name.table_name",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec!["1".to_owned(), "999".to_owned(), "3".to_owned()]),
            OutboundMessage::DataRow(vec!["4".to_owned(), "999".to_owned(), "6".to_owned()]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
}
