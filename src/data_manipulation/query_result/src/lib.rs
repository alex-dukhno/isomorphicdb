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
    UndefinedBiFunction(String, String, String),
    DatatypeMismatch(String, String, String),
    InvalidArgumentForPowerFunction,
    InvalidTextRepresentation(String, String),
    MostSpecificTypeMismatch(String, String, String, usize),
}

impl QueryExecutionError {
    pub fn undefined_function<Op: ToString, Ty: ToString>(operator: Op, type_family: Ty) -> QueryExecutionError {
        QueryExecutionError::UndefinedFunction(operator.to_string(), type_family.to_string())
    }

    pub fn undefined_bi_function<Op: ToString, LeftTy: ToString, RightTy: ToString>(
        operator: Op,
        left_type_family: LeftTy,
        right_type_family: RightTy,
    ) -> QueryExecutionError {
        QueryExecutionError::UndefinedBiFunction(
            operator.to_string(),
            left_type_family.to_string(),
            right_type_family.to_string(),
        )
    }

    pub fn datatype_mismatch<Op: ToString, TT: ToString, AT: ToString>(
        operator: Op,
        target_type: TT,
        actual_type: AT,
    ) -> QueryExecutionError {
        QueryExecutionError::DatatypeMismatch(operator.to_string(), target_type.to_string(), actual_type.to_string())
    }

    pub fn invalid_text_representation<T: ToString, V: ToString>(sql_type: T, value: V) -> QueryExecutionError {
        QueryExecutionError::InvalidTextRepresentation(sql_type.to_string(), value.to_string())
    }

    pub fn most_specific_type_mismatch<V: ToString, Ty: ToString, C: ToString>(
        value: V,
        sql_type: Ty,
        column_name: C,
        index: usize,
    ) -> QueryExecutionError {
        QueryExecutionError::MostSpecificTypeMismatch(
            value.to_string(),
            sql_type.to_string(),
            column_name.to_string(),
            index,
        )
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
            QueryExecutionError::UndefinedBiFunction(func, left_type, right_type) => {
                QueryError::undefined_function(func, left_type, right_type)
            }
            QueryExecutionError::DatatypeMismatch(op, target_type, actual_type) => {
                QueryError::datatype_mismatch(op, target_type, actual_type)
            }
            QueryExecutionError::InvalidArgumentForPowerFunction => QueryError::invalid_argument_for_power_function(),
            QueryExecutionError::InvalidTextRepresentation(sql_type, value) => {
                QueryError::invalid_text_representation_2(sql_type, value)
            }
            QueryExecutionError::MostSpecificTypeMismatch(value, sql_type, column_name, index) => {
                QueryError::most_specific_type_mismatch2(value, sql_type, column_name, index)
            }
        }
    }
}
