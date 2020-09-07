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
use protocol::results::QueryError;
use sqlparser::ast::{ColumnDef, DataType, Statement};

#[rstest::rstest]
fn create_table_with_unsupported_type(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert!(matches!(
        query_planner.plan(Statement::CreateTable {
            name: ObjectName(vec![ident("schema_name"), ident("table_name"),]),
            columns: vec![ColumnDef {
                name: ident("column_name"),
                data_type: DataType::Custom(ObjectName(vec![ident("strange_type_name_whatever")])),
                collation: None,
                options: vec![]
            }],
            constraints: vec![],
            with_options: vec![],
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
        }),
        Err(_)
    ));

    collector.assert_content(vec![Err(QueryError::feature_not_supported(
        "strange_type_name_whatever type is not supported",
    ))])
}

#[rstest::rstest]
fn create_table_with_unqualified_name(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert!(matches!(
        query_planner.plan(Statement::CreateTable {
            name: ObjectName(vec![ident("only_schema_in_the_name")]),
            columns: vec![ColumnDef {
                name: ident("column_name"),
                data_type: DataType::Custom(ObjectName(vec![ident("strange_type_name_whatever")])),
                collation: None,
                options: vec![]
            }],
            constraints: vec![],
            with_options: vec![],
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
        }),
        Err(_)
    ));

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
    ))])
}

#[rstest::rstest]
fn create_table_with_unsupported_name(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert!(matches!(
        query_planner.plan(Statement::CreateTable {
            name: ObjectName(vec![
                ident("first_part"),
                ident("second_part"),
                ident("third_part"),
                ident("fourth_part")
            ]),
            columns: vec![ColumnDef {
                name: ident("column_name"),
                data_type: DataType::Custom(ObjectName(vec![ident("strange_type_name_whatever")])),
                collation: None,
                options: vec![]
            }],
            constraints: vec![],
            with_options: vec![],
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
        }),
        Err(_)
    ));

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "unable to process table name 'first_part.second_part.third_part.fourth_part'",
    ))])
}
