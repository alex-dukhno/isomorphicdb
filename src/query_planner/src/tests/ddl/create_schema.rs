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

use crate::tests::*;
use plan::SchemaCreationInfo;
use sqlparser::ast::{ObjectName, Statement};

#[rstest::rstest]
fn create_new_schema(planner: QueryPlanner) {
    assert_eq!(
        planner.plan(&Statement::CreateSchema {
            schema_name: ObjectName(vec![ident(SCHEMA)]),
            if_not_exists: false
        }),
        Ok(Plan::CreateSchema(SchemaCreationInfo::new(SCHEMA)))
    );
}

#[rstest::rstest]
fn create_schema_with_the_same_name(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::CreateSchema {
            schema_name: ObjectName(vec![ident(SCHEMA)]),
            if_not_exists: false
        }),
        Err(PlanError::schema_already_exists(&SCHEMA))
    );
}

#[rstest::rstest]
fn create_schema_with_unqualified_name(planner: QueryPlanner) {
    assert_eq!(
        planner.plan(&Statement::CreateSchema {
            schema_name: ObjectName(vec![
                ident("first_part"),
                ident("second_part"),
                ident("third_part"),
                ident("fourth_part")
            ]),
            if_not_exists: false
        }),
        Err(PlanError::syntax_error(
            &"only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'"
        ))
    );
}
