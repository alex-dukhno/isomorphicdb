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
use description::SchemaCreationInfo;
use sqlparser::ast::{ObjectName, Statement};

fn create_schema(schema_name: ObjectName) -> Statement {
    Statement::CreateSchema {
        schema_name,
        if_not_exists: false,
    }
}

#[test]
fn create_new_schema() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_schema(ObjectName(vec![ident(SCHEMA)])));
    assert_eq!(
        description,
        Ok(Description::CreateSchema(SchemaCreationInfo {
            schema_name: SCHEMA.to_owned()
        }))
    );
}

#[test]
fn create_schema_with_the_same_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_schema(ObjectName(vec![ident(SCHEMA)])));
    assert_eq!(description, Err(DescriptionError::schema_already_exists(&SCHEMA)));
}

#[test]
fn create_schema_with_unqualified_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&create_schema(ObjectName(vec![
        ident("first_part"),
        ident("second_part"),
        ident("third_part"),
        ident("fourth_part"),
    ])));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'"
        ))
    );
}
