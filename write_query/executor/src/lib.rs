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
    use bigdecimal::BigDecimal;
    use catalog::InMemoryDatabase;
    use data_definition_execution_plan::{
        ColumnInfo, CreateSchemaQuery, CreateTableQuery, ExecutionOutcome, SchemaChange,
    };
    use data_manipulation_typed_tree::{StaticTypedItem, StaticTypedTree, TypedValue};
    use definition::{FullTableName, SchemaName};
    use types::{SqlType, SqlTypeFamily};

    const SCHEMA: &str = "schema";
    const TABLE: &str = "table";

    #[test]
    fn insert_single_value() {
        let database = InMemoryDatabase::new();

        assert_eq!(
            database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name: SchemaName::from(&SCHEMA),
                if_not_exists: false,
            })),
            Ok(ExecutionOutcome::SchemaCreated)
        );
        assert_eq!(
            database.execute(SchemaChange::CreateTable(CreateTableQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                column_defs: vec![ColumnInfo {
                    name: "col_1".to_owned(),
                    sql_type: SqlType::small_int()
                }],
                if_not_exists: false,
            })),
            Ok(ExecutionOutcome::TableCreated)
        );

        let executor = WriteQueryExecutor::new(database);

        assert_eq!(
            executor.execute(TypedWrite::Insert(InsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                values: vec![vec![Some(StaticTypedTree::Item(StaticTypedItem::Const(
                    TypedValue::Num {
                        value: BigDecimal::from(1),
                        type_family: SqlTypeFamily::SmallInt
                    },
                )))]],
            })),
            Ok(QueryExecution::Inserted(1))
        );
    }
}
