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
use description::ColumnDesc;
use pg_wire::PgType;
use sqlparser::ast::{ColumnDef, DataType};

#[allow(dead_code)]
fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: ident(name),
        data_type,
        collation: None,
        options: vec![],
    }
}

fn create_table(name: Vec<&str>, columns: Vec<ColumnDef>) -> Statement {
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

#[test]
fn create_table_with_nonexistent_schema() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_table(vec!["non_existent_schema", "non_existent_table"], vec![]));

    assert_eq!(
        description,
        Err(DescriptionError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn create_table_with_the_same_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    metadata.create_table(DEFAULT_CATALOG, SCHEMA, TABLE, &[]);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_table(vec![SCHEMA, TABLE], vec![]));

    assert_eq!(
        description,
        Err(DescriptionError::table_already_exists(&format!("{}.{}", SCHEMA, TABLE)))
    );
}

#[test]
fn create_table_with_unsupported_column_type() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_table(
        vec![SCHEMA, TABLE],
        vec![column(
            "column_name",
            DataType::Custom(ObjectName(vec![ident("strange_type_name_whatever")])),
        )],
    ));
    assert_eq!(
        description,
        Err(DescriptionError::feature_not_supported(
            &"'strange_type_name_whatever' type is not supported",
        ))
    );
}

#[test]
fn create_table_with_unqualified_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_table(
        vec!["only_schema_in_the_name"],
        vec![column("column_name", DataType::SmallInt)],
    ));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"Unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        ))
    );
}

#[test]
fn create_table_with_unsupported_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_table(
        vec!["first_part", "second_part", "third_part", "fourth_part"],
        vec![column("column_name", DataType::SmallInt)],
    ));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"Unable to process table name 'first_part.second_part.third_part.fourth_part'",
        ))
    );
}

#[test]
fn successfully_create_table() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_table(
        vec![SCHEMA, TABLE],
        vec![column("column_name", DataType::SmallInt)],
    ));
    assert_eq!(
        description,
        Ok(Description::CreateTable(TableCreationInfo {
            schema_id: 0,
            table_name: TABLE.to_owned(),
            columns: vec![ColumnDesc {
                name: "column_name".to_owned(),
                pg_type: PgType::SmallInt
            }]
        }))
    );
}
