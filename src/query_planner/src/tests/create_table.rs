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
use data_manager::ColumnDefinition;
use plan::TableCreationInfo;
use protocol::results::QueryError;
use sql_model::sql_types::SqlType;
use sqlparser::ast::{ColumnDef, DataType, ObjectName, Statement};

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: ident(name),
        data_type,
        collation: None,
        options: vec![],
    }
}

fn table(name: Vec<&str>, columns: Vec<ColumnDef>) -> Statement {
    Statement::CreateTable {
        or_replace: false,
        name: ObjectName(name.into_iter().map(ident).collect()),
        columns,
        constraints: vec![],
        with_options: vec![],
        if_not_exists: false,
        external: false,
        file_format: None,
        location: None,
        query: None,
        without_rowid: false,
    }
}

#[rstest::rstest]
fn create_table_with_nonexistent_schema(planner_and_sender: (InMemory, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert_eq!(
        query_planner.plan(&table(vec!["non_existent_schema", TABLE], vec![])),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::schema_does_not_exist("non_existent_schema"))])
}

#[rstest::rstest]
fn create_table_with_the_same_name(planner_and_sender_with_table: (InMemory, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_table;

    assert_eq!(query_planner.plan(&table(vec![SCHEMA, TABLE], vec![])), Err(()));

    collector.assert_content(vec![Err(QueryError::table_already_exists(format!(
        "{}.{}",
        SCHEMA, TABLE
    )))])
}

#[rstest::rstest]
fn create_table_with_unsupported_column_type(planner_and_sender_with_schema: (InMemory, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(&table(
            vec!["schema_name", "table_name"],
            vec![column(
                "column_name",
                DataType::Custom(ObjectName(vec![ident("strange_type_name_whatever")]))
            )]
        )),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::feature_not_supported(
        "'strange_type_name_whatever' type is not supported",
    ))])
}

#[rstest::rstest]
fn create_table_with_unqualified_name(planner_and_sender_with_schema: (InMemory, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(&table(
            vec!["only_schema_in_the_name"],
            vec![column("column_name", DataType::SmallInt)]
        )),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
    ))])
}

#[rstest::rstest]
fn create_table_with_unsupported_name(planner_and_sender_with_schema: (InMemory, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(&table(
            vec!["first_part", "second_part", "third_part", "fourth_part"],
            vec![column("column_name", DataType::SmallInt)]
        )),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "unable to process table name 'first_part.second_part.third_part.fourth_part'",
    ))])
}

#[rstest::rstest]
fn create_table(planner_and_sender_with_schema: (InMemory, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(&table(
            vec![SCHEMA, TABLE],
            vec![column("column_name", DataType::SmallInt)]
        )),
        Ok(Plan::CreateTable(TableCreationInfo::new(
            0,
            TABLE,
            vec![ColumnDefinition::new(
                "column_name",
                SqlType::SmallInt(i16::min_value())
            )]
        )))
    );

    collector.assert_content(vec![])
}
