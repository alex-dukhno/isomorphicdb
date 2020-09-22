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

use protocol::pgsql_types::PostgreSqlType;

use common::{database_with_schema, ResultCollector};
use parser::QueryParser;
use protocol::results::{QueryError, QueryEvent};
use sql_engine::QueryExecutor;

mod common;

// #[rstest::rstest]
// fn describe_select_statement(database_with_schema: (QueryExecutor, QueryParser, ResultCollector)) {
//     let (mut engine, mut parser, collector) = database_with_schema;
//
//     engine.execute(
//         &parser
//             .parse("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
//             .expect("parsed"),
//     );
//     collector.assert_receive_single(Ok(QueryEvent::TableCreated));
//
//     parser
//         .parse_prepared_statement(
//             "select * from schema_name.table_name where column = $1 and column_2 = $2;",
//             &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
//         )
//         .expect("no system errors");
//     collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));
//
//     engine
//         .describe_prepared_statement("statement_name")
//         .expect("no system errors");
//     collector.assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![
//         ("column_1".to_owned(), PostgreSqlType::SmallInt),
//         ("column_2".to_owned(), PostgreSqlType::SmallInt),
//     ])));
//     collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![
//         PostgreSqlType::SmallInt,
//         PostgreSqlType::SmallInt,
//     ])));
// }
//
// #[rstest::rstest]
// fn describe_update_statement(database_with_schema: (QueryExecutor, QueryParser, ResultCollector)) {
//     let (mut engine, mut parser, collector) = database_with_schema;
//
//     engine.execute(
//         &parser
//             .parse("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
//             .expect("parsed"),
//     );
//     collector.assert_receive_single(Ok(QueryEvent::TableCreated));
//
//     parser
//         .parse_prepared_statement(
//             "update schema_name.table_name set column_1 = $1 where column_2 = $2;",
//             &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
//         )
//         .expect("no system errors");
//     collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));
//
//     engine
//         .describe_prepared_statement("statement_name")
//         .expect("no system errors");
//     collector.assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![])));
//     collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![
//         PostgreSqlType::SmallInt,
//         PostgreSqlType::SmallInt,
//     ])));
// }

#[rstest::rstest]
fn describe_not_existed_statement(database_with_schema: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, _parser, collector) = database_with_schema;

    engine
        .describe_prepared_statement("non_existent")
        .expect("no system errors");
    collector.assert_receive_intermediate(Err(QueryError::prepared_statement_does_not_exist("non_existent")));
}
