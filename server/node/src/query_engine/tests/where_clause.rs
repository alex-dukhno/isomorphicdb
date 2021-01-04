// Copyright 2020 - present Alex Dukhno
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
use pg_model::{results::QueryEvent, Command};

#[rstest::rstest]
fn select_row_by_column_equality_predicate(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name where column_1 = 1;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("column_1", PgType::SmallInt),
            ColumnMetadata::new("column_2", PgType::SmallInt),
            ColumnMetadata::new("column_3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "4".to_owned(),
            "7".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}
