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
fn inner_join_of_two_tables(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(CommandMessage::Query {
            sql: "create schema schema_1;".to_string(),
        })
        .expect("query expected");
    collector.assert_receive_single(Ok(QueryEvent::SchemaCreated));

    engine
        .execute(CommandMessage::Query {
            sql: "create table schema_1.table1 (t_1_col_1 smallint, t_1_col_2 smallint, t_1_col_3 smallint);"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(CommandMessage::Query {
            sql: "insert into schema_1.table1 values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(CommandMessage::Query {
            sql: "create table schema_1.table2 (t_2_col_1 smallint, t_2_col_2 smallint, t_2_col_3 smallint);"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(CommandMessage::Query {
            sql: "insert into schema_1.table2 values (1, 20, 30), (4, 50, 60), (7, 80, 90);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(CommandMessage::Query {
            sql: "select * from t1, t2 where t1.d = t2.d1".to_owned(),
        })
        .expect("query executed");

    engine
        .execute(CommandMessage::Query {
            sql: "select t_1_col_2, t_1_col_3, t2_col_2, t2_col_3 from schema_1.table1 join schema_1.table2 on t_1_col_1 = t_2_col2".to_owned()
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("t_1_col_2", PgType::SmallInt),
            ColumnMetadata::new("t_1_col_3", PgType::SmallInt),
            ColumnMetadata::new("t_2_col_2", PgType::SmallInt),
            ColumnMetadata::new("t_2_col_3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "2".to_owned(),
            "3".to_owned(),
            "20".to_owned(),
            "30".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "4".to_owned(),
            "5".to_owned(),
            "40".to_owned(),
            "50".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "7".to_owned(),
            "8".to_owned(),
            "70".to_owned(),
            "80".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}
