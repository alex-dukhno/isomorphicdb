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

#[test]
fn create_table_where_schema_not_found() {
    let executor = database();

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, TABLE)),
        Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    );
}

#[test]
fn create_table_with_the_same_name() {
    let executor = database();

    assert_eq!(
        executor.execute(create_schema_ops(SCHEMA)),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, TABLE)),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, TABLE)),
        Err(ExecutionError::TableAlreadyExists(SCHEMA.to_owned(), TABLE.to_owned()))
    );
}

#[test]
fn create_if_not_exists() {
    let executor = database();

    assert_eq!(
        executor.execute(create_schema_ops(SCHEMA)),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, TABLE)),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        executor.execute(create_table_if_not_exists_ops(SCHEMA, TABLE)),
        Ok(ExecutionOutcome::TableCreated)
    );
}

#[test]
fn drop_table_where_schema_not_found() {
    let executor = database();

    assert_eq!(
        executor.execute(drop_tables_ops(SCHEMA, vec![TABLE])),
        Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    );
}

#[test]
fn drop_nonexistent_table() {
    let executor = database();

    assert_eq!(
        executor.execute(create_schema_ops(SCHEMA)),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        executor.execute(drop_tables_ops(SCHEMA, vec![TABLE])),
        Err(ExecutionError::TableDoesNotExist(SCHEMA.to_owned(), TABLE.to_owned()))
    );
}

#[test]
fn drop_many() {
    let executor = database();

    assert_eq!(
        executor.execute(create_schema_ops(SCHEMA)),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, TABLE)),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, OTHER_TABLE)),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        executor.execute(drop_tables_ops(SCHEMA, vec![TABLE, OTHER_TABLE])),
        Ok(ExecutionOutcome::TableDropped)
    );
}

#[test]
fn drop_if_exists_first() {
    let executor = database();

    assert_eq!(
        executor.execute(create_schema_ops(SCHEMA)),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, TABLE)),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        executor.execute(drop_tables_if_exists_ops(SCHEMA, vec![TABLE, OTHER_TABLE])),
        Ok(ExecutionOutcome::TableDropped)
    );
}

#[test]
fn drop_if_exists_last() {
    let executor = database();

    assert_eq!(
        executor.execute(create_schema_ops(SCHEMA)),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        executor.execute(create_table_ops(SCHEMA, OTHER_TABLE)),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        executor.execute(drop_tables_if_exists_ops(SCHEMA, vec![TABLE, OTHER_TABLE])),
        Ok(ExecutionOutcome::TableDropped)
    );
}
