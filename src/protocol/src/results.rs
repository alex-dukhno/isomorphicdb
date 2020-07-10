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

use crate::{sql_types, sql_types::PostgreSqlType, Message};
use std::fmt::{self, Display, Formatter};

/// Represents result of SQL query execution
pub type QueryResult = std::result::Result<QueryEvent, QueryError>;
/// Represents selected data from tables
pub type Projection = (Vec<(String, sql_types::PostgreSqlType)>, Vec<Vec<String>>);

/// Represents successful events that can happen in server backend
#[derive(Debug, PartialEq)]
pub enum QueryEvent {
    /// Schema successfully created
    SchemaCreated,
    /// Schema successfully dropped
    SchemaDropped,
    /// Table successfully created
    TableCreated,
    /// Table successfully dropped
    TableDropped,
    /// Variable successfully set
    VariableSet,
    /// Transaction is started
    TransactionStarted,
    /// Number of records inserted into a table
    RecordsInserted(usize),
    /// Records selected from database
    RecordsSelected(Projection),
    /// Number of records updated into a table
    RecordsUpdated(usize),
    /// Number of records deleted into a table
    RecordsDeleted(usize),
}

/// Message severities
/// Reference: defined in https://www.postgresql.org/docs/12/protocol-error-fields.html
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum Severity {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

// easy conversion into a string.
impl Into<String> for Severity {
    fn into(self) -> String {
        match self {
            Self::Error => "ERROR".to_string(),
            Self::Fatal => "FATAL".to_string(),
            Self::Panic => "PANIC".to_string(),
            Self::Warning => "WARNING".to_string(),
            Self::Notice => "NOTICE".to_string(),
            Self::Debug => "DEBUG".to_string(),
            Self::Info => "INFO".to_string(),
            Self::Log => "LOG".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ConstraintViolationKind {
    NumericTypeOutOfRange(PostgreSqlType),
    DataTypeMismatch(PostgreSqlType, String),
    StringTypeLengthMismatch(PostgreSqlType, u64),
}

/// Represents a constraint violation during query execution
/// It is separate from QueryError because there can be multiple
/// of these errors for one QueryError.
#[derive(Debug, Clone, PartialEq)]
pub struct ConstraintViolation {
    severity: Severity,
    code: String,
    kind: ConstraintViolationKind,
}

impl ConstraintViolation {
    /// sql error code
    pub fn code(&self) -> Option<String> {
        Some(self.code.clone())
    }

    /// error severity
    pub fn severity(&self) -> Option<String> {
        Some(self.severity.into())
    }

    /// error message
    pub fn message(&self) -> Option<String> {
        Some(format!("{}", self))
    }

    /// numeric out of range constructor
    pub fn out_of_range(pg_type: PostgreSqlType) -> Self {
        Self {
            severity: Severity::Error,
            code: "22003".to_owned(),
            kind: ConstraintViolationKind::NumericTypeOutOfRange(pg_type),
        }
    }

    /// type mismatch constructor
    pub fn type_mismatch(value: &str, pg_type: PostgreSqlType) -> Self {
        Self {
            severity: Severity::Error,
            code: "2200G".to_owned(),
            kind: ConstraintViolationKind::DataTypeMismatch(pg_type, value.to_owned()),
        }
    }

    /// length of string types do not match constructor
    pub fn string_length_mismatch(pg_type: PostgreSqlType, len: u64) -> Self {
        Self {
            severity: Severity::Error,
            code: "22026".to_owned(),
            kind: ConstraintViolationKind::StringTypeLengthMismatch(pg_type, len),
        }
    }
}

impl Display for ConstraintViolation {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self.kind {
            ConstraintViolationKind::NumericTypeOutOfRange(pg_type) => write!(f, "{} out of range", pg_type),
            ConstraintViolationKind::DataTypeMismatch(pg_type, value) => {
                write!(f, "invalid input syntax for type {}: \"{}\"", pg_type, value)
            }
            ConstraintViolationKind::StringTypeLengthMismatch(pg_type, len) => {
                write!(f, "value too long for type {}({})", pg_type, len)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum QueryErrorKind {
    SchemaAlreadyExists(String),
    TableAlreadyExists(String),
    SchemaDoesNotExist(String),
    TableDoesNotExist(String),
    ColumnDoesNotExist(Vec<String>),
    NotSupportedOperation(String),
    TooManyInsertExpressions,
    ConstraintViolations(Vec<ConstraintViolation>),
    UndefinedFunction(String, String, String),
    SyntaxError(String),
}

/// Represents error during query execution
#[derive(Debug, PartialEq)]
pub struct QueryError {
    severity: Severity,
    code: String,
    kind: QueryErrorKind,
}

impl QueryError {
    /// error code
    pub fn code(&self) -> Option<String> {
        Some(self.code.clone())
    }

    /// error severity
    pub fn severity(&self) -> Option<String> {
        Some(self.severity.into())
    }

    /// error message
    pub fn message(&self) -> Option<String> {
        Some(format!("{}", self.kind))
    }

    pub(crate) fn into_messages(self) -> Vec<Message> {
        match self.kind {
            QueryErrorKind::ConstraintViolations(violations) => {
                let mut messages = Vec::with_capacity(violations.len());
                for violation in violations {
                    messages.push(Message::ErrorResponse(
                        violation.severity(),
                        violation.code(),
                        violation.message(),
                    ))
                }
                messages
            }
            _ => vec![Message::ErrorResponse(self.severity(), self.code(), self.message())],
        }
    }

    /// operator or function is not found for operands
    pub fn undefined_function(operator: String, left_type: String, right_type: String) -> Self {
        Self {
            severity: Severity::Error,
            code: "42883".to_owned(),
            kind: QueryErrorKind::UndefinedFunction(operator, left_type, right_type),
        }
    }

    /// syntax error in the expression as part of query
    pub fn syntax_error(expression: String) -> Self {
        Self {
            severity: Severity::Error,
            code: "42601".to_owned(),
            kind: QueryErrorKind::SyntaxError(expression),
        }
    }

    /// schema already exists error constructor
    pub fn schema_already_exists(schema_name: String) -> Self {
        Self {
            severity: Severity::Error,
            code: "42P06".to_owned(),
            kind: QueryErrorKind::SchemaAlreadyExists(schema_name),
        }
    }

    /// schema does not exist error constructor
    pub fn schema_does_not_exist(schema_name: String) -> Self {
        Self {
            severity: Severity::Error,
            code: "3F000".to_owned(),
            kind: QueryErrorKind::SchemaDoesNotExist(schema_name),
        }
    }

    /// table already exists error constructor
    pub fn table_already_exists(table_name: String) -> Self {
        Self {
            severity: Severity::Error,
            code: "42P07".to_owned(),
            kind: QueryErrorKind::TableAlreadyExists(table_name),
        }
    }

    /// table does not exist error constructor
    pub fn table_does_not_exist(table_name: String) -> Self {
        Self {
            severity: Severity::Error,
            code: "42P01".to_owned(),
            kind: QueryErrorKind::TableDoesNotExist(table_name),
        }
    }

    /// column does not exists error constructor
    pub fn column_does_not_exist(non_existing_columns: Vec<String>) -> Self {
        Self {
            severity: Severity::Error,
            code: "42703".to_owned(),
            kind: QueryErrorKind::ColumnDoesNotExist(non_existing_columns),
        }
    }

    /// not supported operation error constructor
    pub fn not_supported_operation(raw_sql_query: String) -> Self {
        Self {
            severity: Severity::Error,
            code: "42601".to_owned(),
            kind: QueryErrorKind::NotSupportedOperation(raw_sql_query),
        }
    }

    /// too many insert expressions errors constructor
    pub fn too_many_insert_expressions() -> Self {
        Self {
            severity: Severity::Error,
            code: "42601".to_owned(),
            kind: QueryErrorKind::TooManyInsertExpressions,
        }
    }

    /// constraint violation errors constructor
    pub fn constraint_violations(violations: Vec<ConstraintViolation>) -> Self {
        Self {
            severity: Severity::Error,
            // there isn't a single code fo this so I am leaving it empty.
            code: String::new(),
            kind: QueryErrorKind::ConstraintViolations(violations),
        }
    }
}

impl Display for QueryErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaAlreadyExists(schema_name) => write!(f, "schema \"{}\" already exists", schema_name),
            Self::TableAlreadyExists(table_name) => write!(f, "table \"{}\" already exists", table_name),
            Self::SchemaDoesNotExist(schema_name) => write!(f, "schema \"{}\" does not exist", schema_name),
            Self::TableDoesNotExist(table_name) => write!(f, "table \"{}\" does not exist", table_name),
            Self::ColumnDoesNotExist(columns) => {
                if columns.len() > 1 {
                    write!(f, "columns {} do not exist", columns.join(", "))
                } else {
                    write!(f, "column {} does not exist", columns[0])
                }
            }
            Self::NotSupportedOperation(raw_sql_query) => {
                write!(f, "Currently, Query '{}' can't be executed", raw_sql_query)
            }
            Self::TooManyInsertExpressions => write!(f, "INSERT has more expressions then target columns"),
            Self::ConstraintViolations(_) => {
                log::error!("should not use Display to generate the message for Constraint Violations");
                write!(f, "do not use display with ConstraintViolation errors")
            }
            Self::UndefinedFunction(operator, left_type, right_type) => write!(
                f,
                "operator does not exist: ({} {} {})",
                left_type, operator, right_type
            ),
            Self::SyntaxError(expression) => write!(f, "syntax error in {}", expression),
        }
    }
}
