// Copyright 2020 Alex Dukhno
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
use pg_model::{
    results::{QueryError, QueryEvent},
    Command,
};

#[rstest::rstest]
fn prepare_execute_and_deallocate(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint)"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "prepare fooplan (smallint, smallint) as insert into schema_name.table_name values ($1, 456, $2)"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementPrepared));

    engine
        .execute(Command::Query {
            sql: "execute fooplan(123, 789)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(Command::Query {
            sql: "deallocate fooplan".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementDeallocated));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("column_1", PgType::SmallInt),
            ColumnMetadata::new("column_2", PgType::SmallInt),
            ColumnMetadata::new("column_3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "123".to_owned(),
            "456".to_owned(),
            "789".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}

#[rstest::rstest]
fn execute_deallocated_prepared_statement(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_1 smallint)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "prepare fooplan (smallint) as insert into schema_name.table_name values ($1)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementPrepared));

    engine
        .execute(Command::Query {
            sql: "deallocate fooplan".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementDeallocated));

    engine
        .execute(Command::Query {
            sql: "execute fooplan(123)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Err(QueryError::prepared_statement_does_not_exist("fooplan")));
}

#[rstest::rstest]
fn prepare_with_wrong_type(database_with_table: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_table;
    engine
        .execute(Command::Query {
            sql: "prepare fooplan (i, j, k) as insert into schema_name.table_name values ($1, $2, $3)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Err(QueryError::type_does_not_exist("i")));
}

#[rstest::rstest]
fn prepare_with_indeterminate_type(database_with_table: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_table;
    engine
        .execute(Command::Query {
            sql: "prepare fooplan (smallint, smallint) as insert into schema_name.table_name values (1, $9)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Err(QueryError::indeterminate_parameter_data_type(2)));
}

#[rstest::rstest]
fn prepare_assign_operation_for_all_columns_analysis(database_with_table: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_table;
    engine
        .execute(Command::Query {
            sql: "prepare fooplan as insert into schema_name.table_name values ($2, $3, $1)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementPrepared));

    engine
        .execute(Command::Query {
            sql: "execute fooplan(123, 456, 789)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(Command::Query {
            sql: "deallocate fooplan".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementDeallocated));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
            ColumnMetadata::new("col3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "456".to_owned(),
            "789".to_owned(),
            "123".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}

#[rstest::rstest]
fn prepare_assign_operation_for_specified_columns_analysis(database_with_table: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_table;
    engine
        .execute(Command::Query {
            sql: "prepare fooplan as insert into schema_name.table_name (COL3, COL2, col1) values ($1, $2, $3)"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementPrepared));

    engine
        .execute(Command::Query {
            sql: "execute fooplan(123, 456, 789)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(Command::Query {
            sql: "deallocate fooplan".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementDeallocated));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
            ColumnMetadata::new("col3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "789".to_owned(),
            "456".to_owned(),
            "123".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}

#[rstest::rstest]
fn prepare_reassign_operation_for_all_rows(database_with_table: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_table;

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(Command::Query {
            sql: "prepare fooplan as update schema_name.table_name set col3 = $1, COL1 = $2".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementPrepared));

    engine
        .execute(Command::Query {
            sql: "execute fooplan(777, 999)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(2)));

    engine
        .execute(Command::Query {
            sql: "deallocate fooplan".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementDeallocated));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
            ColumnMetadata::new("col3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "999".to_owned(),
            "2".to_owned(),
            "777".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "999".to_owned(),
            "5".to_owned(),
            "777".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn prepare_reassign_operation_for_specified_rows(database_with_table: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_table;

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(Command::Query {
            sql: "prepare fooplan as update schema_name.table_name set col2 = $1 where COL3 = $2".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementPrepared));

    engine
        .execute(Command::Query {
            sql: "execute fooplan(999, 6)".to_owned(),
        })
        .expect("query executed");

    // TODO: `where` clause needs to be handled in `query_planner`.
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(2)));

    engine
        .execute(Command::Query {
            sql: "deallocate fooplan".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::StatementDeallocated));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name".to_owned(),
        })
        .expect("query executed");

    // TODO: `where` clause needs to be handled in `query_planner`.
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
            ColumnMetadata::new("col3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "999".to_owned(),
            "3".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "4".to_owned(),
            "999".to_owned(),
            "6".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}
