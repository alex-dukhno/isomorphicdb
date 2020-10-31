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
use sqlparser::ast::{ColumnDef, DataType};

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: ident(name),
        data_type,
        collation: None,
        options: vec![],
    }
}

fn create_table(schema_name: &str, table_name: &str, columns: Vec<ColumnDef>) -> Statement {
    Statement::CreateTable {
        or_replace: false,
        name: ObjectName(vec![schema_name, table_name].into_iter().map(ident).collect()),
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
    let description = analyzer.describe(&create_table("non_existent_schema", "non_existent_table", vec![]));

    assert_eq!(
        description,
        Err(DescriptionError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[ignore]
#[test]
fn create_table_with_the_same_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    metadata.create_table(DEFAULT_CATALOG, SCHEMA, TABLE, &[]);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_table(SCHEMA, TABLE, vec![]));

    assert_eq!(
        description,
        Err(DescriptionError::table_already_exists(&format!("{}.{}", SCHEMA, TABLE)))
    );
}
