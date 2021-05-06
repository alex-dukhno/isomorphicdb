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

// #[rstest::rstest]
// fn statement_description(database_with_table: (InMemory, ResultCollector)) {
//     let (mut engine, collector) = database_with_table;
//
//     engine
//         .execute(Inbound::Parse {
//             statement_name: "statement_name".to_owned(),
//             sql: "select * from schema_name.table_name;".to_owned(),
//             param_types: vec![],
//         })
//         .expect("statement parsed");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::ParseComplete));
//
//     engine
//         .execute(Inbound::DescribeStatement {
//             name: "statement_name".to_owned(),
//         })
//         .expect("statement described");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![
//             ("col1".to_owned(), SMALLINT),
//             ("col2".to_owned(), SMALLINT),
//             ("col3".to_owned(), SMALLINT),
//         ])));
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![])));
// }
//
// #[rstest::rstest]
// fn statement_parameters(database_with_table: (InMemory, ResultCollector)) {
//     let (mut engine, collector) = database_with_table;
//
//     engine
//         .execute(Inbound::Parse {
//             statement_name: "statement_name".to_owned(),
//             sql: "update schema_name.table_name set col1 = $1 where col2 = $2;".to_owned(),
//             param_types: vec![SMALLINT, SMALLINT],
//         })
//         .expect("statement parsed");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::ParseComplete));
//
//     engine
//         .execute(Inbound::DescribeStatement {
//             name: "statement_name".to_owned(),
//         })
//         .expect("statement described");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![])));
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![SMALLINT, SMALLINT])));
// }
//
// #[rstest::rstest]
// fn unsuccessful_statement_description(database_with_table: (InMemory, ResultCollector)) {
//     let (mut engine, collector) = database_with_table;
//
//     engine
//         .execute(Inbound::DescribeStatement {
//             name: "non_existent".to_owned(),
//         })
//         .expect("no errors");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Err(QueryError::prepared_statement_does_not_exist("non_existent")));
// }
