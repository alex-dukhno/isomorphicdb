// Copyright 2020 - 2021 Alex Dukhno
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

fn create_schema(schema_name: &str) -> Definition {
    create_schema_if_not_exists(schema_name, false)
}

fn create_schema_if_not_exists(schema_name: &str, if_not_exists: bool) -> Definition {
    Definition::CreateSchema {
        schema_name: schema_name.to_owned(),
        if_not_exists,
    }
}

#[test]
fn create_new_schema() -> TransactionResult<()> {
    Database::in_memory("").transaction(|db| {
        let planner = DefinitionPlanner::from(db);
        assert_eq!(
            planner.plan(create_schema(SCHEMA)),
            Ok(SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name: SchemaName::from(&SCHEMA),
                if_not_exists: false,
            }))
        );
        Ok(())
    })
}

#[test]
fn create_new_schema_if_not_exists() -> TransactionResult<()> {
    Database::in_memory("").transaction(|db| {
        let planner = DefinitionPlanner::from(db);
        assert_eq!(
            planner.plan(create_schema_if_not_exists(SCHEMA, true)),
            Ok(SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name: SchemaName::from(&SCHEMA),
                if_not_exists: true,
            }))
        );
        Ok(())
    })
}

#[test]
fn create_schema_with_the_same_name() -> TransactionResult<()> {
    Database::in_memory("").transaction(|db| {
        let catalog = CatalogHandler::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();

        let planner = DefinitionPlanner::from(db);
        assert_eq!(
            planner.plan(create_schema(SCHEMA)),
            Ok(SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name: SchemaName::from(&SCHEMA),
                if_not_exists: false,
            }))
        );
        Ok(())
    })
}
