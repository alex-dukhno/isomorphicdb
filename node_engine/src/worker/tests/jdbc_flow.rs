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

#[test]
fn insert() {
    let mut connection = MockConnection::new(vec![
        InboundMessage::Query {
            sql: "create schema schema_name;".to_owned(),
        },
        InboundMessage::Query {
            sql: "create table schema_name.table_name(col_1 smallint, col_2 smallint, col_3 smallint);".to_owned(),
        },
        InboundMessage::Parse {
            statement_name: "".to_owned(),
            sql: "insert into schema_name.table_name values ($1, $2, $3)".to_owned(),
            param_types: vec![0, 0, 0],
        },
        InboundMessage::DescribeStatement { name: "".to_owned() },
        InboundMessage::Bind {
            portal_name: "".to_owned(),
            statement_name: "".to_owned(),
            query_param_formats: vec![1; 3],
            query_params: vec![Some(vec![0, 0, 0, 1]), Some(vec![0, 0, 0, 2]), Some(vec![0, 0, 0, 3])],
            result_value_formats: vec![],
        },
        InboundMessage::DescribePortal { name: "".to_owned() },
        InboundMessage::Execute {
            portal_name: "".to_owned(),
            max_rows: 1,
        },
        InboundMessage::Sync,
    ]);

    let node_engine = Worker;

    node_engine.process(&mut connection, "IN_MEMORY");

    assert_eq!(
        connection.outbound,
        vec![
            OutboundMessage::SchemaCreated,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TableCreated,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::ParseComplete,
            OutboundMessage::StatementDescription(vec![]),
            OutboundMessage::StatementParameters(vec![SMALLINT, SMALLINT, SMALLINT]),
            OutboundMessage::BindComplete,
            OutboundMessage::StatementDescription(vec![]),
            OutboundMessage::RecordsInserted(1),
            OutboundMessage::ReadyForQuery
        ]
    );
}

// #[rstest::rstest]
// fn update(database_with_table: (InMemory, ResultCollector)) {
//     let (mut engine, collector) = database_with_table;
//
//     engine
//         .execute(Inbound::Parse {
//             statement_name: "".to_owned(),
//             sql: "update schema_name.table_name set col1 = $1, col2 = $2, col3 = $3;".to_owned(),
//             param_types: vec![0, 0, 0],
//         })
//         .expect("statement parsed");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::ParseComplete));
//
//     engine
//         .execute(Inbound::DescribeStatement { name: "".to_owned() })
//         .expect("statement described");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![])));
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![SMALLINT, SMALLINT, SMALLINT])));
//
//     engine
//         .execute(Inbound::Parse {
//             statement_name: "".to_owned(),
//             sql: "update schema_name.table_name set col1 = $1, col2 = $2, col3 = $3;".to_owned(),
//             param_types: vec![SMALLINT, SMALLINT, SMALLINT],
//         })
//         .expect("statement parsed");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::ParseComplete));
//
//     engine
//         .execute(Inbound::Bind {
//             portal_name: "".to_owned(),
//             statement_name: "".to_owned(),
//             query_param_formats: vec![1; 3],
//             query_params: vec![Some(vec![0, 0, 0, 1]), Some(vec![0, 0, 0, 2]), Some(vec![0, 0, 0, 3])],
//             result_value_formats: vec![],
//         })
//         .expect("portal bound");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::BindComplete));
//
//     engine
//         .execute(Inbound::DescribePortal { name: "".to_owned() })
//         .expect("statement parsed");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![])));
//
//     engine
//         .execute(Inbound::Execute {
//             portal_name: "".to_owned(),
//             max_rows: 1,
//         })
//         .expect("portal executed");
//     collector
//         .lock()
//         .unwrap()
//         .assert_receive_intermediate(Ok(QueryEvent::RecordsUpdated(0)));
// }
