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
use protocol::{sql_types::PostgreSqlType, results::QueryErrorBuilder};

#[rstest::fixture]
fn int_table(mut sql_engine_with_schema: InMemorySqlEngine) -> InMemorySqlEngine {
    sql_engine_with_schema
        .execute("create table schema_name.table_name(col smallint);")
        .expect("no system errors")
        .expect("table created");

    sql_engine_with_schema
}

#[rstest::fixture]
fn str_table(mut sql_engine_with_schema: InMemorySqlEngine) -> InMemorySqlEngine {
    sql_engine_with_schema
        .execute("create table schema_name.table_name(col varchar(5));")
        .expect("no system errors")
        .expect("table created");

    sql_engine_with_schema
}

#[cfg(test)]
mod insert {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(mut int_table: InMemorySqlEngine) {
        let mut builder = QueryErrorBuilder::new();
        builder.out_of_range(PostgreSqlType::SmallInt);

        assert_eq!(
            int_table
                .execute("insert into schema_name.table_name values (32768);")
                .expect("no system errors"),
            Err(builder.build())
        );
    }

    #[rstest::rstest]
    fn type_mismatch(mut int_table: InMemorySqlEngine) {
        let mut builder = QueryErrorBuilder::new();
        builder.type_mismatch("str", PostgreSqlType::SmallInt);

        assert_eq!(
            int_table
                .execute("insert into schema_name.table_name values ('str');")
                .expect("no system errors"),
            Err(builder.build())
        )
    }

    #[rstest::rstest]
    fn value_too_long(mut str_table: InMemorySqlEngine) {
        let mut builder = QueryErrorBuilder::new();
        builder.string_length_mismatch(PostgreSqlType::VarChar, 5);
        assert_eq!(
            str_table
                .execute("insert into schema_name.table_name values ('123457890');")
                .expect("no system errors"),
            Err(builder.build())
        )
    }
}

#[cfg(test)]
mod update {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(mut int_table: InMemorySqlEngine) {
        let mut builder = QueryErrorBuilder::new();
        builder.out_of_range(PostgreSqlType::SmallInt);

        int_table
            .execute("insert into schema_name.table_name values (32767);")
            .expect("no system errors")
            .expect("record inserted");

        assert_eq!(
            int_table
                .execute("update schema_name.table_name set col = 32768;")
                .expect("no system errors"),
            Err(builder.build())
        );
    }

    #[rstest::rstest]
    fn type_mismatch(mut int_table: InMemorySqlEngine) {
        let mut builder = QueryErrorBuilder::new();
        builder.type_mismatch("str", PostgreSqlType::SmallInt);
        int_table
            .execute("insert into schema_name.table_name values (32767);")
            .expect("no system errors")
            .expect("record inserted");

        assert_eq!(
            int_table
                .execute("update schema_name.table_name set col = 'str';")
                .expect("no system errors"),
            Err(builder.build())
        )
    }

    #[rstest::rstest]
    fn value_too_long(mut str_table: InMemorySqlEngine) {
        let mut builder = QueryErrorBuilder::new();
        builder.string_length_mismatch(PostgreSqlType::VarChar, 5);

        str_table
            .execute("insert into schema_name.table_name values ('str');")
            .expect("no system errors")
            .expect("record inserted");

        assert_eq!(
            str_table
                .execute("update schema_name.table_name set col = '123457890';")
                .expect("no system errors"),
            Err(builder.build())
        )
    }
}
