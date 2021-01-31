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

use catalog::{Database, SqlTable};
use data_manipulation_query_result::{QueryExecution, QueryExecutionError};
use data_manipulation_typed_queries::{DeleteQuery, InsertQuery, TypedWrite, UpdateQuery};
use std::sync::Arc;

#[derive(Clone)]
pub struct WriteQueryExecutor<D: Database> {
    database: Arc<D>,
}

impl<D: Database> WriteQueryExecutor<D> {
    pub fn new(database: Arc<D>) -> WriteQueryExecutor<D> {
        WriteQueryExecutor { database }
    }

    pub fn execute(&self, write_query: TypedWrite) -> Result<QueryExecution, QueryExecutionError> {
        match write_query {
            TypedWrite::Insert(InsertQuery {
                full_table_name,
                values,
            }) => Ok(QueryExecution::Inserted(
                self.database.work_with(&full_table_name, |table| table.insert(&values)),
            )),
            TypedWrite::Delete(DeleteQuery { full_table_name }) => Ok(QueryExecution::Deleted(
                self.database.work_with(&full_table_name, |table| table.delete_all()),
            )),
            TypedWrite::Update(UpdateQuery {
                full_table_name,
                column_names,
                assignments,
            }) => Ok(QueryExecution::Updated(
                self.database.work_with(&full_table_name, |table| {
                    table.update(column_names.clone(), assignments.clone())
                }),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::InMemoryDatabase;
    use data_definition_operations::{Kind, Record, Step, SystemObject, SystemOperation};
    use data_manipulation_typed_tree::{StaticTypedItem, StaticTypedTree, TypedValue};
    use definition::FullTableName;
    use types::SqlType;

    fn create_schema_ops(schema_name: &str) -> SystemOperation {
        SystemOperation {
            kind: Kind::Create(SystemObject::Schema),
            skip_steps_if: None,
            steps: vec![vec![
                Step::CheckExistence {
                    system_object: SystemObject::Schema,
                    object_name: vec![schema_name.to_owned()],
                },
                Step::CreateFolder {
                    name: schema_name.to_owned(),
                },
                Step::CreateRecord {
                    record: Record::Schema {
                        schema_name: schema_name.to_owned(),
                    },
                },
            ]],
        }
    }

    fn create_table_ops(schema_name: &str, table_name: &str, columns: Vec<(&str, SqlType)>) -> SystemOperation {
        let column_steps: Vec<Step> = columns
            .into_iter()
            .map(|(column_name, column_type)| Step::CreateRecord {
                record: Record::Column {
                    schema_name: schema_name.to_owned(),
                    table_name: table_name.to_owned(),
                    column_name: column_name.to_owned(),
                    sql_type: column_type,
                },
            })
            .collect();
        let mut all_steps: Vec<Step> = vec![
            Step::CheckExistence {
                system_object: SystemObject::Schema,
                object_name: vec![schema_name.to_owned()],
            },
            Step::CheckExistence {
                system_object: SystemObject::Table,
                object_name: vec![schema_name.to_owned(), table_name.to_owned()],
            },
            Step::CreateFile {
                folder_name: schema_name.to_owned(),
                name: table_name.to_owned(),
            },
            Step::CreateRecord {
                record: Record::Table {
                    schema_name: schema_name.to_owned(),
                    table_name: table_name.to_owned(),
                },
            },
        ];
        all_steps.extend(column_steps);
        SystemOperation {
            kind: Kind::Create(SystemObject::Table),
            skip_steps_if: None,
            steps: vec![all_steps],
        }
    }

    const SCHEMA: &str = "schema";
    const TABLE: &str = "table";

    #[test]
    fn insert_single_value() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::small_int())]))
            .unwrap();
        let executor = WriteQueryExecutor::new(database);

        let r = executor.execute(TypedWrite::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![Some(StaticTypedTree::Item(StaticTypedItem::Const(
                TypedValue::SmallInt(1),
            )))]],
        }));

        assert_eq!(r, Ok(QueryExecution::Inserted(1)));
    }
}
