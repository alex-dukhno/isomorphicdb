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
use sqlparser::ast::{ObjectName, ObjectType, Statement};

#[rstest::rstest]
fn drop_non_existent_schema(planner: QueryPlanner) {
    assert_eq!(
        planner.plan(&Statement::Drop {
            object_type: ObjectType::Schema,
            if_exists: false,
            names: vec![ObjectName(vec![ident("non_existent")])],
            cascade: false,
        }),
        Err(PlanError::schema_does_not_exist(&"non_existent"))
    );
}

#[rstest::rstest]
fn drop_schema_with_unqualified_name(planner: QueryPlanner) {
    assert_eq!(
        planner.plan(&Statement::Drop {
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
        Err(PlanError::syntax_error(
            &"only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'"
        ))
    );
}

#[rstest::rstest]
fn drop_schema(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::Drop {
            object_type: ObjectType::Schema,
            if_exists: false,
            names: vec![ObjectName(vec![ident(SCHEMA)])],
            cascade: false,
        }),
        Ok(Plan::DropSchemas(vec![(SchemaId::from(0), false)]))
    );
}
