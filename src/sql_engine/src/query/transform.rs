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

///! Module for transforming the input Query AST into representation the engine can proecess.

use storage::frontend::FrontendStorage;
use storage::backend::BackendStorage;
use sqlparser::ast::*;
use std::sync::{Arc, Mutex};
use super::*;
use plan::{TableInfo, SchemaInfo};
use crate::query::relation::RelationOp;
use super::{Plan, Datum, Row, ScalarOp, RelationOp, RelationError, TransformError, TableId, SchemaId, RelationType};
use storage::TableDescription;

// I do not know what the error type is yet.
type Result<T> = ::std::result::Result<T, TransformError>;

// this could probably just be a function.
/// structure for maintaining state while transforming the input statement.
pub struct QueryTransform<B: BackendStorage> {
    /// access to table and schema information.
    storage: Arc<Mutex<FrontendStorage<B>>>,
}

fn table_from_object(object: &ObjectName) -> Result<TableId> {
    if object.0.len() == 1 {
        Err(TransformError::SyntaxError(format!("unsupported table name '{}'. All table names must be qualified", object.to_string())))
    } else if object.0.len() != 2 {
        Err(TransformError::SyntaxError(format!("unable to process table name '{}'", object.to_string())))
    } else {
        let table_name = object.0.last().unwrap().value.clone();
        let schema_name = object.0.first().unwrap().value.clone();
        Ok(TableId(SchemaId(schema_name), table_name))
    }
}

fn schema_from_object(object: &ObjectName) -> Result<SchemaId> {
    if object.0.len() != 1 {
        Err(TransformError::SyntaxError(format!("only unqualified schema names are supported, '{}'", object.to_string())))
    }
    else {
        let schema_name = object.to_string();
        Ok(SchemaId(schema_name))
    }
}

impl<B: BackendStorage> QueryTransform<B> {
    pub fn new(frontend_storage: &'a FrontendStorage<B>) -> Self {
        Self {
                frontend_storage,
        }
    }

    pub fn process(&mut self, stmt: Statement) -> Result<Plan> {
        self.handle_statement(&stmt)
    }

    fn handle_statement(&mut self, stmt: &Statement) -> Result<Plan> {
         match stmt {
            Statement::StartTransaction { .. } |
            Statement::SetVariable { .. } => unimplemented!(),
            Statement::CreateTable { name, columns, .. } => {
                unimplemented!()
            }
            Statement::CreateSchema { schema_name, .. } => {
                let schema_id = schema_from_object(schema_name)?;
                Ok(Plan::CreateSchema(SchemaInfo { schema_name: schema_id.name().to_string() }))
            }
            Statement::Drop { object_type, names, .. } => match object_type {
                ObjectType::Table => {
                    let mut table_names = Vec::with_capacity(names.len());
                    for name in names {
                        let table_id = table_from_object(name)?;
                        // check if the
                        table_names.push(table_id);
                    }
                    Ok(Plan::DropTables(table_names))
                },
                ObjectType::Schema => {
                    let mut schema_names = Vec::with_capacity(names.len());
                    for name in names {
                        let schema_id = schema_from_object(name)?;
                        schema_names.push(schema_id);
                    }
                    Ok(Plan::DropSchemas(schema_names))
                },
                _ => { unimplemented!() }
            },
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => { unimplemented!() },
            Statement::Query(query) => {
                let op = self.handle_query(query.as_ref())?;
                Ok(Plan::Query(Box::new(op)))
            },
            Statement::Update {
                table_name,
                assignments,
                ..
            } => { unimplemented!() },
            Statement::Delete { table_name, .. } => { unimplemented!() }
            _ => unimplemented!()
        }
    }

    fn handle_query(&mut self, query: &Query) -> Result<RelationOp> {
        let Query {
            body,
            ..
            // order_by,
            // limit,
            // ctes,
            // offset,
            // fetch
        } = query;
        let relation_op = self.handle_set_expr(&body)?;

        Ok(relation_op)
    }

    fn handle_set_expr(&mut self, expr: &SetExpr) -> Result<RelationOp> {
        match expr {
            SetExpr::Select(select) => self.handle_select(select),
            SetExpr::Query(query) => self.handle_query(query),
            SetExpr::Values(values) => self.handle_values(values),
            e => Err(TransformError::UnimplementedFeature(format!("{:?}", e)))
        }
    }
    fn handle_select(&mut self, select: &Select) -> Result<RelationOp> {
        let Select { distinct, top, projection, from, selection, group_by, having } = select;
        // this implementation is naive.

        // 1. resolve the from clause to know the resulting relation of the selection (from clause) push down predicates when possible.
        let from_clause = self.handle_from_clause(from.as_slice())?;
        // 2. resolve remaining predicates.
        // 3. resolve any groupby clauses
        // 4. resolve having
        // 5. resolve projection

        Err(TransformError::UnimplementedFeature("select clauses are not implemented".to_string()))
    }

    fn handle_values(&self, values: &Values) -> Result<RelationOp> {
        Err(TransformError::UnimplementedFeature("values clauses are not implemented".to_string()))
    }

    fn handle_from_clause(&mut self, from: &[TableWithJoins]) -> Result<RelationOp> {
        // for now only handle when there is one table
        if from.len() == 1 {
            let TableWithJoins { relation, joins } = from.first().unwrap();
            let table_info = self.resolve_table_factor(relation)?;
            unimplemented!()
        }
        else {
            // cartician product.
            Err(TransformError::UnimplementedFeature("multiple table in the from clause are not implemented".to_string()))
        }
    }

    fn resolve_table_factor(&self, relation: &TableFactor) -> Result<RelationType> {
        match relation {
            TableFactor::Table { name, .. } => {
                // let table_info = self.frontend_storage.table_descriptor();
            },
            TableFactor::Derived { .. } => {},
            TableFactor::NestedJoin(table) => {},
        }
        unimplemented!()
    }
}
