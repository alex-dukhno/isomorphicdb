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

use std::convert::TryFrom;

use sql_model::Id;
use sqlparser::ast::ObjectName;
use std::fmt::{self, Display, Formatter};

use sql_model::sql_types::SqlType;

///! Module for representing how a query will be parameters bound, executed and
///! values represented during runtime.
pub mod plan;
pub mod planner;

/// represents a schema uniquely by its id
#[derive(PartialEq, Debug, Clone)]
pub struct SchemaId(Id);

impl AsRef<Id> for SchemaId {
    fn as_ref(&self) -> &Id {
        &self.0
    }
}

/// represents a schema uniquely by its name
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct SchemaName(String);

impl AsRef<str> for SchemaName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<&ObjectName> for SchemaName {
    type Error = SchemaNamingError;

    fn try_from(object: &ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() != 1 {
            Err(SchemaNamingError(object.to_string()))
        } else {
            Ok(SchemaName(object.to_string()))
        }
    }
}

impl Display for SchemaName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct SchemaNamingError(String);

impl Display for SchemaNamingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "only unqualified schema names are supported, '{}'", self.0)
    }
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct FullTableName(SchemaName, String);

impl FullTableName {
    fn as_tuple(&self) -> (&str, &str) {
        (&self.0.as_ref(), &self.1)
    }
}

impl Display for FullTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.0.as_ref(), self.1)
    }
}

impl TryFrom<&ObjectName> for FullTableName {
    type Error = TableNamingError;

    fn try_from(object: &ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() == 1 {
            Err(TableNamingError::Unqualified(object.to_string()))
        } else if object.0.len() != 2 {
            Err(TableNamingError::NotProcessed(object.to_string()))
        } else {
            let table_name = object.0.last().unwrap().value.clone();
            let schema_name = object.0.first().unwrap().value.clone();
            Ok(FullTableName(SchemaName(schema_name), table_name))
        }
    }
}

pub enum TableNamingError {
    Unqualified(String),
    NotProcessed(String),
}

impl Display for TableNamingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TableNamingError::Unqualified(table_name) => write!(
                f,
                "unsupported table name '{}'. All table names must be qualified",
                table_name
            ),
            TableNamingError::NotProcessed(table_name) => write!(f, "unable to process table name '{}'", table_name),
        }
    }
}

/// A type of a column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    #[allow(dead_code)]
    nullable: bool,
    sql_type: SqlType,
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableId((Id, Id));

impl AsRef<(Id, Id)> for TableId {
    fn as_ref(&self) -> &(Id, Id) {
        &self.0
    }
}

#[cfg(test)]
mod tests;

// pub struct Mapper {
//     data_manager: Arc<DataManager>,
//     sender: Arc<dyn Sender>,
// }
//
// impl Mapper {
//     fn new(data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Mapper {
//         Mapper { data_manager, sender }
//     }
//
//     fn convert(&self, expr: Expr) -> Result<ScalarOp, ()> {
//         // match expr {
//         //     Expr::Cast { expr, data_type } => match (&*expr, &data_type) {
//         //         (Expr::Value(Value::SingleQuotedString(v)), DataType::Boolean) => match Bool::from_str(v) {
//         //             Ok(Bool(boolean)) => Ok(ScalarOp::Literal(Datum::from_bool(boolean))),
//         //             Err(_) => Err(()),
//         //         },
//         //         (Expr::Value(Value::Boolean(val)), DataType::Boolean) => Ok(ScalarOp::Literal(Datum::from_bool(*val))),
//         //         _ => {
//         //             self.sender
//         //                 .send(Err(QueryError::syntax_error(format!(
//         //                     "Cast from {:?} to {:?} is not currently supported",
//         //                     expr, data_type
//         //                 ))))
//         //                 .expect("To Send Query Result to Client");
//         //             Err(())
//         //         }
//         //     },
//             // Expr::UnaryOp { op, expr } => match (op, expr.deref()) {
//             //     (UnaryOperator::Minus, Expr::Value(Value::Number(value))) => {
//             //         match Datum::try_from(&Value::Number(-value)) {
//             //             Ok(datum) => Ok(ScalarOp::Literal(datum)),
//             //             Err(e) => {
//             //                 let err = if let Some(meta_data) = expr_metadata.as_ref() {
//             //                     match e {
//             //                         EvalError::UnsupportedDatum(ty) => {
//             //                             QueryError::feature_not_supported(format!("Data type not supported: {}", ty))
//             //                         }
//             //                         EvalError::OutOfRangeNumeric(_) => QueryError::out_of_range(
//             //                             meta_data.column().sql_type().to_pg_types(),
//             //                             meta_data.column().name(),
//             //                             meta_data.index(),
//             //                         ),
//             //                         EvalError::UnsupportedOperation => {
//             //                             QueryError::feature_not_supported("Use of unsupported expression feature")
//             //                         }
//             //                     }
//             //                 } else {
//             //                     match e {
//             //                         EvalError::UnsupportedDatum(ty) => {
//             //                             QueryError::feature_not_supported(format!("Data type not supported: {}", ty))
//             //                         }
//             //                         EvalError::OutOfRangeNumeric(ty) => {
//             //                             QueryError::out_of_range(ty.to_pg_types(), String::new(), 0)
//             //                         }
//             //                         EvalError::UnsupportedOperation => {
//             //                             QueryError::feature_not_supported("Use of unsupported expression feature")
//             //                         }
//             //                     }
//             //                 };
//             //                 self.session.send(Err(err)).expect("To Send Query Result to Client");
//             //                 Err(())
//             //             }
//             //         }
//             //     }
//             //     (op, _operand) => {
//             //         self.session
//             //             .send(Err(QueryError::syntax_error(
//             //                 op.to_string() + expr.to_string().as_str(),
//             //             )))
//             //             .expect("To Send Query Result to Client");
//             //         Err(())
//             //     }
//             // },
//             // Expr::BinaryOp { op, left, right } => {
//             //     let lhs = self.inner_eval(left.deref(), expr_metadata)?;
//             //     let rhs = self.inner_eval(right.deref(), expr_metadata)?;
//             //     if let Some(ty) = self.compatible_types_for_op(op.clone(), lhs.scalar_type(), rhs.scalar_type()) {
//             //         match (lhs, rhs) {
//             //             (ScalarOp::Literal(left), ScalarOp::Literal(right)) => {
//             //                 UpdateEvalScalarOp::eval_binary_literal_expr(self.session.as_ref(), op.clone(), left, right)
//             //                     .map(ScalarOp::Literal)
//             //             }
//             //             (left, right) => Ok(ScalarOp::Binary(op.clone(), Box::new(left), Box::new(right), ty)),
//             //         }
//             //     } else {
//             //         let kind = QueryError::undefined_function(
//             //             op.to_string(),
//             //             lhs.scalar_type().to_string(),
//             //             rhs.scalar_type().to_string(),
//             //         );
//             //         self.session.send(Err(kind)).expect("To Send Query Result to Client");
//             //         Err(())
//             //     }
//             // }
//             // Expr::Value(value) => match Datum::try_from(*value) {
//             //     Ok(datum) => Ok(ScalarOp::Literal(datum)),
//             //     Err(e) => {
//             //         let err = if let Some(meta_data) = expr_metadata.as_ref() {
//             //             match e {
//             //                 EvalError::UnsupportedDatum(ty) => {
//             //                     QueryError::feature_not_supported(format!("Data type not supported: {}", ty))
//             //                 }
//             //                 EvalError::OutOfRangeNumeric(_) => QueryError::out_of_range(
//             //                     meta_data.column().sql_type().to_pg_types(),
//             //                     meta_data.column().name(),
//             //                     meta_data.index(),
//             //                 ),
//             //                 EvalError::UnsupportedOperation => {
//             //                     QueryError::feature_not_supported("Use of unsupported expression feature")
//             //                 }
//             //             }
//             //         } else {
//             //             match e {
//             //                 EvalError::UnsupportedDatum(ty) => {
//             //                     QueryError::feature_not_supported(format!("Data type not supported: {}", ty))
//             //                 }
//             //                 EvalError::OutOfRangeNumeric(ty) => {
//             //                     QueryError::out_of_range(ty.to_pg_types(), String::new(), 0)
//             //                 }
//             //                 EvalError::UnsupportedOperation => {
//             //                     QueryError::feature_not_supported("Use of unsupported expression feature")
//             //                 }
//             //             }
//             //         };
//             //
//             //         self.session.send(Err(err)).expect("To Send Query Result to Client");
//             //         Err(())
//             //     }
//             // },
//             // Expr::Identifier(ident) => {
//             //     if let Some((idx, column_def)) = self.find_column_by_name(ident.value.as_str())? {
//             //         let scalar_type = column_def.sql_type();
//             //         Ok(ScalarOp::Column(idx, Self::convert_sql_type(scalar_type)))
//             //     } else {
//             //         self.session
//             //             .send(Err(QueryError::undefined_column(ident.value.clone())))
//             //             .expect("To Send Query Result to Client");
//             //         Err(())
//             //     }
//             // }
//             _ => {
//                 self.sender
//                     .send(Err(QueryError::syntax_error(expr.to_string())))
//                     .expect("To Send Query Result to Client");
//                 Err(())
//             }
//         }
//     }
// }
//
// #[cfg(test)]
// mod mapper {
//     use super::*;
//     use ast::Datum;
//     use protocol::results::QueryResult;
//     use sqlparser::ast::{DataType, Value};
//     use std::io;
//     use std::ops::Deref;
//     use std::sync::Mutex;
//
//     struct Collector(Mutex<Vec<QueryResult>>);
//
//     impl Sender for Collector {
//         fn flush(&self) -> io::Result<()> {
//             Ok(())
//         }
//
//         fn send(&self, query_result: QueryResult) -> io::Result<()> {
//             self.0.lock().expect("locked").push(query_result);
//             Ok(())
//         }
//     }
//
//     impl Collector {
//         fn assert_content(&self, expected: Vec<QueryResult>) {
//             let result = self.0.lock().expect("locked");
//             assert_eq!(result.deref(), &expected)
//         }
//     }
//
//     type ResultCollector = Arc<Collector>;
//
//     fn sender() -> ResultCollector {
//         Arc::new(Collector(Mutex::new(vec![])))
//     }
//
//     fn data_manager() -> Arc<DataManager> {
//         Arc::new(DataManager::in_memory().expect("to create data manager"))
//     }
//
//     #[test]
//     fn not_supported_expression() {
//         let data_manager = data_manager();
//         let sender = sender();
//         let mapper = Mapper::new(data_manager.clone(), sender.clone());
//
//         assert_eq!(mapper.convert(Expr::Wildcard), Err(()));
//         sender.assert_content(vec![Err(QueryError::syntax_error(Expr::Wildcard.to_string()))]);
//     }
//
//     #[test]
//     fn bool_cast_string() {
//         let data_manager = data_manager();
//         let sender = sender();
//         let mapper = Mapper::new(data_manager.clone(), sender.clone());
//
//         assert_eq!(
//             mapper.convert(Expr::Cast {
//                 expr: Box::new(Expr::Value(Value::SingleQuotedString("true".to_string()))),
//                 data_type: DataType::Boolean
//             }),
//             Ok(ScalarOp::Literal(Datum::from_bool(true)))
//         );
//         sender.assert_content(vec![]);
//     }
//
//     #[test]
//     fn bool_cast_not_parsable_string() {
//         let data_manager = data_manager();
//         let sender = sender();
//         let mapper = Mapper::new(data_manager.clone(), sender.clone());
//
//         assert_eq!(
//             mapper.convert(Expr::Cast {
//                 expr: Box::new(Expr::Value(Value::SingleQuotedString("not a boolean".to_string()))),
//                 data_type: DataType::Boolean
//             }),
//             Err(())
//         );
//         sender.assert_content(vec![]);
//     }
//
//     #[test]
//     fn bool_cast_value() {
//         let data_manager = data_manager();
//         let sender = sender();
//         let mapper = Mapper::new(data_manager.clone(), sender.clone());
//
//         assert_eq!(
//             mapper.convert(Expr::Cast {
//                 expr: Box::new(Expr::Value(Value::Boolean(false))),
//                 data_type: DataType::Boolean
//             }),
//             Ok(ScalarOp::Literal(Datum::from_bool(false)))
//         );
//         sender.assert_content(vec![]);
//     }
// }
