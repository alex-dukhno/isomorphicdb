// Copyright 2020 - present Alex Dukhno
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
use crate::tests::ident;
use description::{DropSchemasInfo, DropTablesInfo, SchemaId};
use sql_ast::{ObjectName, ObjectType, Statement};

fn drop(names: Vec<ObjectName>, object_type: ObjectType) -> Statement {
    Statement::Drop {
        object_type,
        if_exists: false,
        names,
        cascade: false,
    }
}

#[test]
fn drop_non_existent_schema() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(vec![ObjectName(vec![ident("non_existent")])], ObjectType::Schema));
    assert_eq!(
        description,
        Err(DescriptionError::schema_does_not_exist(&"non_existent"))
    );
}

#[test]
fn drop_schema_with_unqualified_name() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(
        vec![ObjectName(vec![
            ident("first_part"),
            ident("second_part"),
            ident("third_part"),
            ident("fourth_part"),
        ])],
        ObjectType::Schema,
    ));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"Only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'"
        ))
    );
}

#[test]
fn drop_schema() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    metadata.create_schema(SCHEMA).expect("schema created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(vec![ObjectName(vec![ident(SCHEMA)])], ObjectType::Schema));
    assert_eq!(
        description,
        Ok(Description::DropSchemas(DropSchemasInfo {
            schema_ids: vec![SchemaId::from(0)],
            cascade: false,
            if_exists: false,
        }))
    );
}

#[test]
fn drop_table_from_nonexistent_schema() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(
        vec![ObjectName(vec![ident("non_existent_schema"), ident(TABLE)])],
        ObjectType::Table,
    ));
    assert_eq!(
        description,
        Err(DescriptionError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn drop_nonexistent_table() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    metadata.create_schema(SCHEMA).expect("schema created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(
        vec![ObjectName(vec![ident(SCHEMA), ident("non_existent_table")])],
        ObjectType::Table,
    ));
    assert_eq!(
        description,
        Err(DescriptionError::table_does_not_exist(&format!(
            "{}.{}",
            SCHEMA, "non_existent_table"
        )))
    );
}

#[test]
fn drop_table_with_unqualified_name() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    metadata.create_schema(SCHEMA).expect("schema created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(
        vec![ObjectName(vec![ident("only_schema_in_the_name")])],
        ObjectType::Table,
    ));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"Unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        ))
    );
}

#[test]
fn drop_table_with_unsupported_name() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(
        vec![ObjectName(vec![
            ident("first_part"),
            ident("second_part"),
            ident("third_part"),
            ident("fourth_part"),
        ])],
        ObjectType::Table,
    ));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"Unable to process table name 'first_part.second_part.third_part.fourth_part'",
        ))
    );
}

#[test]
fn drop_table() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let schema_id = metadata.create_schema(SCHEMA).expect("schema created");
    metadata.create_table(schema_id, TABLE, &[]).expect("table created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&drop(
        vec![ObjectName(vec![ident(SCHEMA), ident(TABLE)])],
        ObjectType::Table,
    ));
    assert_eq!(
        description,
        Ok(Description::DropTables(DropTablesInfo {
            full_table_ids: vec![FullTableId::from((0, 0))],
            cascade: false,
            if_exists: false
        }))
    );
}
