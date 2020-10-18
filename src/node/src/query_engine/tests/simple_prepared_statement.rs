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
use pg_model::pg_types::PostgreSqlType;
use protocol::{
    messages::ColumnMetadata,
    results::{QueryError, QueryEvent},
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
            ColumnMetadata::new("column_1", PostgreSqlType::SmallInt),
            ColumnMetadata::new("column_2", PostgreSqlType::SmallInt),
            ColumnMetadata::new("column_3", PostgreSqlType::SmallInt),
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
fn prepare_with_wrong_type(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "prepare fooplan (i, j, k) as insert into schema_name.table_name values ($1, $2, $3)".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Err(QueryError::type_does_not_exist("i")));
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
