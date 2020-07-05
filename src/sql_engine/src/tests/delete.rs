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
use protocol::sql_types::PostgreSqlType;

#[rstest::rstest]
fn delete_from_nonexistent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("delete from schema_name.table_name;")
            .expect("no system errors"),
        Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
    );
}

#[rstest::rstest]
fn delete_all_records(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors")
        .expect("row inserted");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (456);")
        .expect("no system errors")
        .expect("row inserted");
    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()], vec!["456".to_owned()]]
        )))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("delete from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsDeleted(2))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![]
        )))
    );
}
