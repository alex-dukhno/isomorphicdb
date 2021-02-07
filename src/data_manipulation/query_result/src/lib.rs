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

use data_scalar::ScalarValue;
use definition::ColumnDef;
use pg_result::QueryError;

#[derive(Debug, PartialEq)]
pub enum QueryExecution {
    Inserted(usize),
    Deleted(usize),
    Updated(usize),
    Selected((Vec<ColumnDef>, Vec<Vec<ScalarValue>>)),
}

#[derive(Debug, PartialEq)]
pub enum QueryExecutionError {
    SchemaDoesNotExist(String),
    ColumnNotFound(String),
    UndefinedFunction(String, String),
    DatatypeMismatch(String, String, String),
    InvalidArgumentForPowerFunction,
}

impl QueryExecutionError {
    pub fn undefined_function<Op: ToString, Ty: ToString>(operator: Op, type_family: Ty) -> QueryExecutionError {
        QueryExecutionError::UndefinedFunction(operator.to_string(), type_family.to_string())
    }

    pub fn datatype_mismatch<Op: ToString, TT: ToString, AT: ToString>(
        operator: Op,
        target_type: TT,
        actual_type: AT,
    ) -> QueryExecutionError {
        QueryExecutionError::DatatypeMismatch(operator.to_string(), target_type.to_string(), actual_type.to_string())
    }
}

impl From<QueryExecutionError> for pg_result::QueryError {
    fn from(error: QueryExecutionError) -> Self {
        match error {
            QueryExecutionError::SchemaDoesNotExist(schema) => QueryError::schema_does_not_exist(schema),
            QueryExecutionError::ColumnNotFound(column) => QueryError::column_does_not_exist(column),
            QueryExecutionError::UndefinedFunction(func, sql_type) => {
                QueryError::undefined_function(func, sql_type.as_str(), "")
            }
            QueryExecutionError::DatatypeMismatch(op, target_type, actual_type) => {
                QueryError::datatype_mismatch(op, target_type, actual_type)
            }
            QueryExecutionError::InvalidArgumentForPowerFunction => QueryError::invalid_argument_for_power_function(),
        }
    }
}
