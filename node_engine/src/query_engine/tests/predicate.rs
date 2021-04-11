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
fn select_value_by_predicate_on_single_field(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(CommandMessage::Query {
            sql: "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(CommandMessage::Query {
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(CommandMessage::Query {
            sql: "select * from schema_name.table_name where col1 = 1".to_owned(),
        })
        .expect("query executed");

    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
            ColumnMetadata::new("col3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}
