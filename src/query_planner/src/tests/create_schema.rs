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
use plan::SchemaCreationInfo;
use protocol::results::QueryError;
use sqlparser::ast::{ObjectName, Statement};

#[rstest::rstest]
fn create_new_schema(planner_and_sender: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert_eq!(
        query_planner.plan(&Statement::CreateSchema {
            schema_name: ObjectName(vec![ident(SCHEMA)])
        }),
        Ok(Plan::CreateSchema(SchemaCreationInfo::new(SCHEMA)))
    );

    collector.assert_content(vec![]);
}

#[rstest::rstest]
fn create_schema_with_the_same_name(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(&Statement::CreateSchema {
            schema_name: ObjectName(vec![ident(SCHEMA)])
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::schema_already_exists(SCHEMA))])
}

#[rstest::rstest]
fn create_schema_with_unqualified_name(planner_and_sender: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert!(matches!(
        query_planner.plan(&Statement::CreateSchema {
            schema_name: ObjectName(vec![
                ident("first_part"),
                ident("second_part"),
                ident("third_part"),
                ident("fourth_part")
            ])
        }),
        Err(_)
    ));

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'",
    ))])
}
