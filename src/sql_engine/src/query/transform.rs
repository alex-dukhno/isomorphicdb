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

///! Module for transforming the input Query AST into representation the engine can process.
use crate::query::plan::SchemaCreationInfo;
use crate::query::{plan::Plan, SchemaId, TableCreationInfo, TableId, RelationError, ScalarOp, RelationOp, RelationType, TableInserts, Row};
use crate::query::expr::{resolve_static_expr, EvalError};
use protocol::{results::QueryErrorBuilder, Sender};
use sql_types::SqlType;
use sqlparser::ast::{ColumnDef, DataType, ObjectName, ObjectType, Statement, SetExpr, Select, Values, TableWithJoins, TableFactor, Query, Ident};
use std::sync::{Arc, Mutex, MutexGuard};
use storage::{
    backend::BackendStorage, frontend::FrontendStorage, ColumnDefinition, OperationOnTableError, TableDescription,
};

// I do not know what the error type is yet.
type Result<T> = ::std::result::Result<T, ()>;

// this could probably just be a function.
/// structure for maintaining state while transforming the input statement.
pub struct QueryProcessor<B: BackendStorage> {
    /// access to table and schema information.
    storage: Arc<Mutex<FrontendStorage<B>>>,
    session: Arc<dyn Sender>,
}

impl<'qp, B: BackendStorage> QueryProcessor<B> {
    pub fn new(storage: Arc<Mutex<FrontendStorage<B>>>, session: Arc<dyn Sender>) -> Self {
        Self { storage, session }
    }

    pub fn storage(&self) -> MutexGuard<FrontendStorage<B>> {
        self.storage.lock().unwrap()
    }

    pub fn process(&mut self, stmt: Statement) -> Result<Plan> {
        self.handle_statement(&stmt)
    }

    // this was moved out to clean up the code. This is a good place
    // to start but should not be the final code.
    fn table_from_object(&self, object: &ObjectName) -> Result<TableId> {
        if object.0.len() == 1 {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .syntax_error(format!(
                        "unsupported table name '{}'. All table names must be qualified",
                        object.to_string()
                    ))
                    .build()))
                .expect("To Send Query Result to Client");
            Err(())
        } else if object.0.len() != 2 {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .syntax_error(format!("unable to process table name '{}'", object.to_string()))
                    .build()))
                .expect("To Send Query Result to Client");
            Err(())
        } else {
            let table_name = object.0.last().unwrap().value.clone();
            let schema_name = object.0.first().unwrap().value.clone();
            Ok(TableId(SchemaId(schema_name), table_name))
        }
    }

    fn sql_type_from_datatype(&self, datatype: &DataType) -> Result<SqlType> {
        match datatype {
            DataType::SmallInt => Ok(SqlType::SmallInt(i16::min_value())),
            DataType::Int => Ok(SqlType::Integer(i32::min_value())),
            DataType::BigInt => Ok(SqlType::BigInt(i64::min_value())),
            DataType::Char(len) => Ok(SqlType::Char(len.unwrap_or(255))),
            DataType::Varchar(len) => Ok(SqlType::VarChar(len.unwrap_or(255))),
            DataType::Boolean => Ok(SqlType::Bool),
            DataType::Custom(name) => {
                let name = name.to_string();
                match name.as_str() {
                    "serial" => Ok(SqlType::Integer(1)),
                    "smallserial" => Ok(SqlType::SmallInt(1)),
                    "bigserial" => Ok(SqlType::BigInt(1)),
                    other_type => {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .feature_not_supported(format!("{} type is not supported", other_type))
                                .build()))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                }
            }
            other_type => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .feature_not_supported(format!("{} type is not supported", other_type))
                        .build()))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }

    fn schema_from_object(&mut self, object: &ObjectName) -> Result<SchemaId> {
        if object.0.len() != 1 {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .syntax_error(format!(
                        "only unqualified schema names are supported, '{}'",
                        object.to_string()
                    ))
                    .build()))
                .expect("To Send Query Result to Client");
            Err(())
        } else {
            let schema_name = object.to_string();
            Ok(SchemaId(schema_name))
        }
    }

    fn handle_statement(&mut self, stmt: &Statement) -> Result<Plan> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => self.handle_create_table(name, columns),
            Statement::CreateSchema { schema_name, .. } => {
                let schema_id = self.schema_from_object(schema_name)?;
                if self.storage().schema_exists(schema_id.name()) {
                    self.session
                        .send(Err(QueryErrorBuilder::new()
                            .schema_already_exists(schema_id.name().to_string())
                            .build()))
                        .expect("To Send Query Result to Client");
                    Err(())
                } else {
                    Ok(Plan::CreateSchema(SchemaCreationInfo {
                        schema_name: schema_id.name().to_string(),
                    }))
                }
            }
            Statement::Drop { object_type, names, .. } => self.handle_drop(object_type, names),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.handle_insert(table_name, columns, source),
            // Statement::Query(query) => {
            //     let op = self.handle_query(query.as_ref())?;
            //     Ok(Plan::Query(Box::new(op)))
            // }
            other => Ok(Plan::NotProcessed(other.clone())),
        }
    }

    fn resolve_column_definitions(&self, columns: &[ColumnDef]) -> Result<Vec<ColumnDefinition>> {
        let mut column_defs = Vec::new();
        for column in columns {
            let sql_type = self.sql_type_from_datatype(&column.data_type)?;
            // maybe a different type should be used to represent this instead of the storage's representation.
            let column_definition = ColumnDefinition::new(column.name.value.as_str(), sql_type);
            column_defs.push(column_definition);
        }
        Ok(column_defs)
    }

    fn handle_create_table(&mut self, name: &ObjectName, columns: &[ColumnDef]) -> Result<Plan> {
        let table_id = self.table_from_object(name)?;
        let schema_name = table_id.schema_name();
        let table_name = table_id.name();
        if !self.storage().schema_exists(schema_name) {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .schema_does_not_exist(schema_name.to_string())
                    .build()))
                .expect("To Send Query Result to Client");
            Err(())
        } else if self.storage().table_exists(schema_name, table_name) {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .table_already_exists(format!("{}.{}", schema_name, table_name))
                    .build()))
                .expect("To Send Query Result to Client");
            Err(())
        } else {
            let columns = self.resolve_column_definitions(columns)?;
            let table_info = TableCreationInfo {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
                columns,
            };
            Ok(Plan::CreateTable(table_info))
        }
    }

    fn handle_drop(&mut self, object_type: &ObjectType, names: &[ObjectName]) -> Result<Plan> {
        match object_type {
            ObjectType::Table => {
                let mut table_names = Vec::with_capacity(names.len());
                for name in names {
                    // I like the idea of abstracting this to a resolve_table_name(name) which would do
                    // this check for us and can be reused else where. ideally this function could handle aliasing as well.
                    let table_id = self.table_from_object(name)?;
                    let schema_name = table_id.schema_name();
                    let table_name = table_id.name();
                    if !self.storage().schema_exists(schema_name) {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .schema_does_not_exist(schema_name.to_string())
                                .build()))
                            .expect("To Send Query Result to Client");
                        return Err(());
                    } else if !self.storage().table_exists(schema_name, table_name) {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .table_does_not_exist(format!("{}.{}", schema_name, table_name))
                                .build()))
                            .expect("To Send Query Result to Client");
                        return Err(());
                    } else {
                        table_names.push(table_id);
                    }
                }
                Ok(Plan::DropTables(table_names))
            }
            ObjectType::Schema => {
                let mut schema_names = Vec::with_capacity(names.len());
                for name in names {
                    let schema_id = self.schema_from_object(name)?;
                    if !self.storage().schema_exists(schema_id.name()) {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .schema_does_not_exist(schema_id.name().to_string())
                                .build()))
                            .expect("To Send Query Result to Client");
                        return Err(());
                    }

                    schema_names.push(schema_id);
                }
                Ok(Plan::DropSchemas(schema_names))
            }
            _ => unimplemented!(),
        }
    }

    fn handle_insert(&mut self, name: &ObjectName, columns: &[Ident], source: &Query) -> Result<Plan> {
        let table_name = self.table_from_object(name)?;
        let query_op = self.handle_query(source)?;

        let table_descriptor = self
            .storage()
            .table_descriptor(table_name.schema_name(), table_name.name())
            .unwrap()
            .map_err(|e| match e {
                OperationOnTableError::SchemaDoesNotExist => {
                    // TransformError::from(PlanError::InvalidSchema(table_name.schema_name().to_string()))
                }
                OperationOnTableError::TableDoesNotExist => {
                    // TransformError::from(PlanError::InvalidTable(table_name.name().to_string()))
                }
                _ => unreachable!(),
            })?;

        let column_info = table_descriptor.column_data();

        let column_indices = if !columns.is_empty() {
            let mut column_indices = Vec::with_capacity(columns.len());
            for column in columns {
                if let Some((idx, _)) = column_info
                    .iter()
                    .enumerate()
                    .find(|(_, column_def)| column_def.name() == column.value)
                {
                    column_indices.push(ScalarOp::Column(idx));
                } else {
                    // return Err(TransformError::from(RelationError::InvalidColumnName {
                    //     column: column.value.clone(),
                    //     table: table_descriptor.full_name(),
                    // }));
                }
            }
            column_indices
        } else {
            (0..table_descriptor.column_len())
                .into_iter()
                .map(|i| ScalarOp::Column(i))
                .collect::<Vec<ScalarOp>>()
        };

        // @TODO: check type compatability between the resulting relation type and the actual type of the columns
        // the relation type are different then SqlType because the relation might be the result
        // of a join or some other operation.
        //
        //let relation_type = query_op.typ(); // maybe this returns an Option
        //relation_type.compatable(table_descriptor) or something like this?
        // - Andrew Bregger

        let table_insert = TableInserts {
            table_id: table_name,
            column_indices,
            input: Box::new(query_op),
        };

        Ok(Plan::InsertRows(table_insert))
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
            e => Err(QueryErrorBuilder::new().feature_not_supported(format!("{:?}", e)).build()).expect("To Send Query Processor Error to Client"),
        }
    }
    fn handle_select(&mut self, select: &Select) -> Result<RelationOp> {
        let Select {
            distinct: _,
            top: _,
            projection: _,
            from,
            selection: _,
            group_by: _,
            having: _,
        } = select;
        // this implementation is naive.

        // 1. resolve the from clause to know the resulting relation of the selection (from clause) push down predicates when possible.
        let _from_clause = self.handle_from_clause(from.as_slice())?;
        // 2. resolve remaining predicates.
        // 3. resolve any groupby clauses
        // 4. resolve having
        // 5. resolve projection

        self.session.send(Err(QueryErrorBuilder::new().feature_not_supported("select statements".to_string()).build())).expect("To Send Query Processor Error to Client");
        Err(())
    }

    fn handle_values(&self, values: &Values) -> Result<RelationOp> {
        let mut rows = Vec::new();
        for expr_row in values.0.iter() {
            let mut row = Vec::new();
            let mut errors = Vec::new();
            for expr in expr_row{
                match resolve_static_expr(&expr) {
                    Ok(datum) => rows.push(Row::pack(&row)),
                    Err(e) => errors.push(e),
                }
            }

            if !errors.is_empty() {
                // let mut builder = QueryErrorBuilder::new();
                // for error in errors {
                //     match error {
                //         EvalError::OutOfRangeNumeric(idx) => {
                //             // we do not have access to the information we need here to handle this here.
                //             // maybe there is a better place to handle these errors.
                //             builder.out_of_range();
                //         }
                //     }
                // }
                self.session.send(Err(QueryErrorBuilder::new().feature_not_supported("constraint errors are not handled right now".to_string()).build())).expect("Sending Error to Client");
                return Err(());
            }
        }
        Ok(RelationOp::Constants(rows))
    }

    fn handle_from_clause(&mut self, from: &[TableWithJoins]) -> Result<RelationOp> {
        // for now only handle when there is one table
        if from.len() == 1 {
            let TableWithJoins { relation, joins: _ } = from.first().unwrap();
            let _table_info = self.resolve_table_factor(relation)?;
            unimplemented!()
        } else {
            // cartician product.
            self.session.send(Err(QueryErrorBuilder::new().feature_not_supported("multi-table select".to_string()).build())).expect("To Send Query Processor Error to Client");
            Err(())
        }
    }

    fn resolve_table_factor(&self, relation: &TableFactor) -> Result<RelationType> {
        match relation {
            TableFactor::Table { name: _, .. } => {
                // let table_info = self.frontend_storage.table_descriptor();
            }
            TableFactor::Derived { .. } => {}
            TableFactor::NestedJoin(_table) => {}
        }
        unimplemented!()
    }
}
