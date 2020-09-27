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
use plan::SchemaId;
use protocol::results::QueryError;
use sqlparser::ast::{ObjectName, ObjectType, Statement};

#[rstest::rstest]
fn drop_non_existent_schema(planner_and_sender: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert_eq!(
        query_planner.plan(&Statement::Drop {
            object_type: ObjectType::Schema,
            if_exists: false,
            names: vec![ObjectName(vec![ident("non_existent")])],
            cascade: false,
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::schema_does_not_exist("non_existent"))]);
}

#[rstest::rstest]
fn drop_schema_with_unqualified_name(planner_and_sender: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert!(matches!(
        query_planner.plan(&Statement::Drop {
            object_type: ObjectType::Schema,
            if_exists: false,
            names: vec![ObjectName(vec![
                ident("first_part"),
                ident("second_part"),
                ident("third_part"),
                ident("fourth_part")
            ])],
            cascade: false,
        }),
        Err(_)
    ));

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'",
    ))])
}

#[rstest::rstest]
fn drop_schema(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(&Statement::Drop {
            object_type: ObjectType::Schema,
            if_exists: false,
            names: vec![ObjectName(vec![ident(SCHEMA)])],
            cascade: false,
        }),
        Ok(Plan::DropSchemas(vec![(SchemaId::from(0), false)]))
    );

    collector.assert_content(vec![]);
}
