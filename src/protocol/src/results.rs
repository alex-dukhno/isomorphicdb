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

use crate::{
    messages::{BackendMessage, ColumnMetadata},
    sql_types::PostgreSqlType,
};
use std::fmt::{self, Display, Formatter};

/// Represents result of SQL query execution
pub type QueryResult = std::result::Result<QueryEvent, QueryError>;
/// Represents selected columns from tables
pub type Description = Vec<(String, PostgreSqlType)>;
/// Represents selected data from tables
pub type Projection = (Description, Vec<Vec<String>>);

/// Represents successful events that can happen in server backend
#[derive(Clone, Debug, PartialEq)]
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
    /// Parameters described needed by a prepared statement
    PreparedStatementDescribed(Vec<PostgreSqlType>, Description),
    /// Parsing the exteneded query is complete
    ParseComplete,
    /// Processing of the query is complete
    QueryComplete,
}

impl Into<Vec<BackendMessage>> for QueryEvent {
    fn into(self) -> Vec<BackendMessage> {
        match self {
            QueryEvent::SchemaCreated => vec![BackendMessage::CommandComplete("CREATE SCHEMA".to_owned())],
            QueryEvent::SchemaDropped => vec![BackendMessage::CommandComplete("DROP SCHEMA".to_owned())],
            QueryEvent::TableCreated => vec![BackendMessage::CommandComplete("CREATE TABLE".to_owned())],
            QueryEvent::TableDropped => vec![BackendMessage::CommandComplete("DROP TABLE".to_owned())],
            QueryEvent::VariableSet => vec![BackendMessage::CommandComplete("SET".to_owned())],
            QueryEvent::TransactionStarted => vec![BackendMessage::CommandComplete("BEGIN".to_owned())],
            QueryEvent::RecordsInserted(records) => {
                vec![BackendMessage::CommandComplete(format!("INSERT 0 {}", records))]
            }
            QueryEvent::RecordsSelected(projection) => {
                let definition = projection.0;
                let description: Vec<ColumnMetadata> = definition
                    .into_iter()
                    .map(|(name, sql_type)| ColumnMetadata::new(name, sql_type.pg_oid(), sql_type.pg_len()))
                    .collect();
                let records = projection.1;
                let len = records.len();
                let mut messages = vec![BackendMessage::RowDescription(description)];
                for record in records {
                    messages.push(BackendMessage::DataRow(record));
                }
                messages.push(BackendMessage::CommandComplete(format!("SELECT {}", len)));
                messages
            }
            QueryEvent::RecordsUpdated(records) => vec![BackendMessage::CommandComplete(format!("UPDATE {}", records))],
            QueryEvent::RecordsDeleted(records) => vec![BackendMessage::CommandComplete(format!("DELETE {}", records))],
            QueryEvent::PreparedStatementDescribed(param_types, description) => {
                let desc_message = if description.is_empty() {
                    BackendMessage::NoData
                } else {
                    let columns: Vec<ColumnMetadata> = description
                        .into_iter()
                        .map(|(name, sql_type)| ColumnMetadata::new(name, sql_type.pg_oid(), sql_type.pg_len()))
                        .collect();
                    BackendMessage::RowDescription(columns)
                };

                let type_ids = param_types.iter().map(|t| t.pg_oid()).collect();
                vec![BackendMessage::ParameterDescription(type_ids), desc_message]
            }
            QueryEvent::QueryComplete => vec![BackendMessage::ReadyForQuery],
            QueryEvent::ParseComplete => vec![BackendMessage::ParseComplete],
        }
    }
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
impl Into<&'static str> for Severity {
    fn into(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Panic => "PANIC",
            Self::Warning => "WARNING",
            Self::Notice => "NOTICE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Log => "LOG",
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum QueryErrorKind {
    SchemaAlreadyExists(String),
    TableAlreadyExists(String),
    SchemaDoesNotExist(String),
    SchemaHasDependentObjects(String),
    TableDoesNotExist(String),
    ColumnDoesNotExist(Vec<String>),
    PreparedStatementDoesNotExist(String),
    FeatureNotSupported(String),
    TooManyInsertExpressions,
    NumericTypeOutOfRange {
        pg_type: PostgreSqlType,
        column_name: String,
        row_index: usize, // TODO make it optional - does not make sense for update query
    },
    DataTypeMismatch {
        pg_type: PostgreSqlType,
        value: String,
        column_name: String,
        row_index: usize,
    },
    StringTypeLengthMismatch {
        pg_type: PostgreSqlType,
        len: u64,
        column_name: String,
        row_index: usize,
    },
    UndefinedFunction {
        operator: String,
        left_type: String,
        right_type: String,
    },
    SyntaxError(String),
}

impl QueryErrorKind {
    fn code(&self) -> &'static str {
        match self {
            Self::SchemaAlreadyExists(_) => "42P06",
            Self::TableAlreadyExists(_) => "42P07",
            Self::SchemaDoesNotExist(_) => "3F000",
            Self::SchemaHasDependentObjects(_) => "2BP01",
            Self::TableDoesNotExist(_) => "42P01",
            Self::ColumnDoesNotExist(_) => "42703",
            Self::PreparedStatementDoesNotExist(_) => "26000",
            Self::FeatureNotSupported(_) => "0A000",
            Self::TooManyInsertExpressions => "42601",
            Self::NumericTypeOutOfRange { .. } => "22003",
            Self::DataTypeMismatch { .. } => "2200G",
            Self::StringTypeLengthMismatch { .. } => "22026",
            Self::UndefinedFunction { .. } => "42883",
            Self::SyntaxError(_) => "42601",
        }
    }
}

impl Display for QueryErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaAlreadyExists(schema_name) => write!(f, "schema \"{}\" already exists", schema_name),
            Self::TableAlreadyExists(table_name) => write!(f, "table \"{}\" already exists", table_name),
            Self::SchemaDoesNotExist(schema_name) => write!(f, "schema \"{}\" does not exist", schema_name),
            Self::SchemaHasDependentObjects(schema_name) => {
                write!(f, "schema \"{}\" has dependent objects", schema_name)
            }
            Self::TableDoesNotExist(table_name) => write!(f, "table \"{}\" does not exist", table_name),
            Self::ColumnDoesNotExist(columns) => {
                if columns.len() > 1 {
                    write!(f, "columns {} do not exist", columns.join(", "))
                } else {
                    write!(f, "column {} does not exist", columns[0])
                }
            }
            Self::PreparedStatementDoesNotExist(statement_name) => {
                write!(f, "prepared statement {} does not exist", statement_name)
            }
            Self::FeatureNotSupported(raw_sql_query) => {
                write!(f, "Currently, Query '{}' can't be executed", raw_sql_query)
            }
            Self::TooManyInsertExpressions => write!(f, "INSERT has more expressions than target columns"),
            Self::NumericTypeOutOfRange {
                pg_type,
                column_name,
                row_index,
            } => write!(
                f,
                "{} is out of range for column '{}' at row {}",
                pg_type, column_name, row_index
            ),
            Self::DataTypeMismatch {
                pg_type,
                value,
                column_name,
                row_index,
            } => write!(
                f,
                "invalid input syntax for type {} for column '{}' at row {}: \"{}\"",
                pg_type, column_name, row_index, value
            ),
            Self::StringTypeLengthMismatch {
                pg_type,
                len,
                column_name,
                row_index,
            } => write!(
                f,
                "value too long for type {}({}) for column '{}' at row {}",
                pg_type, len, column_name, row_index
            ),
            Self::UndefinedFunction {
                operator,
                left_type,
                right_type,
            } => write!(
                f,
                "operator does not exist: ({} {} {})",
                left_type, operator, right_type
            ),
            Self::SyntaxError(expression) => write!(f, "syntax error in {}", expression),
        }
    }
}

/// Represents error during query execution
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct QueryErrorInner {
    severity: Severity,
    kind: QueryErrorKind,
}

impl QueryErrorInner {
    fn code(&self) -> Option<&'static str> {
        Some(self.kind.code())
    }

    fn severity(&self) -> Option<&'static str> {
        let severity: &'static str = self.severity.into();
        Some(severity)
    }

    fn message(&self) -> Option<String> {
        Some(format!("{}", self.kind))
    }
}

/// a container of errors that occurred during query execution
#[derive(Debug, PartialEq, Clone)]
pub struct QueryError {
    errors: Vec<QueryErrorInner>,
}

impl QueryError {
    pub(crate) fn new(errors: Vec<QueryErrorInner>) -> Self {
        Self { errors }
    }
}

impl Into<Vec<BackendMessage>> for QueryError {
    fn into(self) -> Vec<BackendMessage> {
        self.errors
            .into_iter()
            .map(|inner| BackendMessage::ErrorResponse(inner.severity(), inner.code(), inner.message()))
            .collect::<Vec<_>>()
    }
}

/// a structure for building a QueryError
#[derive(Default, Debug)]
pub struct QueryErrorBuilder {
    errors: Vec<QueryErrorInner>,
}

impl QueryErrorBuilder {
    /// constructs a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// builds a QueryError containing all of the error generated
    pub fn build(self) -> QueryError {
        QueryError::new(self.errors)
    }

    // these error will stop the execution of the query; therefore there will only
    // ever be one.

    /// schema already exists error constructor
    pub fn schema_already_exists(mut self, schema_name: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::SchemaAlreadyExists(schema_name),
        });
        self
    }

    /// schema does not exist error constructor
    pub fn schema_does_not_exist(mut self, schema_name: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::SchemaDoesNotExist(schema_name),
        });
        self
    }

    /// schema has dependent objects error constructor
    pub fn schema_has_dependent_objects(mut self, schema_name: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::SchemaHasDependentObjects(schema_name),
        });
        self
    }

    /// table already exists error constructor
    pub fn table_already_exists(mut self, table_name: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::TableAlreadyExists(table_name),
        });
        self
    }

    /// table does not exist error constructor
    pub fn table_does_not_exist(mut self, table_name: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::TableDoesNotExist(table_name),
        });
        self
    }

    /// column does not exists error constructor
    pub fn column_does_not_exist(mut self, non_existing_columns: Vec<String>) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::ColumnDoesNotExist(non_existing_columns),
        });
        self
    }

    /// prepared statement does not exist error constructor
    pub fn prepared_statement_does_not_exist(mut self, statement_name: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::PreparedStatementDoesNotExist(statement_name),
        });
        self
    }

    /// not supported operation error constructor
    pub fn feature_not_supported(mut self, feature_description: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::FeatureNotSupported(feature_description),
        });
        self
    }

    /// too many insert expressions errors constructors
    pub fn too_many_insert_expressions(mut self) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::TooManyInsertExpressions,
        });
        self
    }

    /// syntax error in the expression as part of query
    pub fn syntax_error(mut self, expression: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::SyntaxError(expression),
        });
        self
    }

    /// operator or function is not found for operands
    pub fn undefined_function(mut self, operator: String, left_type: String, right_type: String) -> Self {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::UndefinedFunction {
                operator,
                left_type,
                right_type,
            },
        });
        self
    }

    // These errors can be generated multiple at a time which is why they are &mut self
    // and the rest are mut self.

    /// numeric out of range constructor
    pub fn out_of_range(&mut self, pg_type: PostgreSqlType, column_name: String, row_index: usize) {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::NumericTypeOutOfRange {
                pg_type,
                column_name,
                row_index,
            },
        });
    }

    /// type mismatch constructor
    pub fn type_mismatch(&mut self, value: &str, pg_type: PostgreSqlType, column_name: String, row_index: usize) {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::DataTypeMismatch {
                pg_type,
                value: value.to_owned(),
                column_name,
                row_index,
            },
        });
    }

    /// length of string types do not match constructor
    pub fn string_length_mismatch(&mut self, pg_type: PostgreSqlType, len: u64, column_name: String, row_index: usize) {
        self.errors.push(QueryErrorInner {
            severity: Severity::Error,
            kind: QueryErrorKind::StringTypeLengthMismatch {
                pg_type,
                len,
                column_name,
                row_index,
            },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod query_event {
        use super::*;

        #[test]
        fn create_schema() {
            let messages: Vec<BackendMessage> = QueryEvent::SchemaCreated.into();
            assert_eq!(
                messages,
                vec![BackendMessage::CommandComplete("CREATE SCHEMA".to_owned())]
            )
        }

        #[test]
        fn drop_schema() {
            let messages: Vec<BackendMessage> = QueryEvent::SchemaDropped.into();
            assert_eq!(
                messages,
                vec![BackendMessage::CommandComplete("DROP SCHEMA".to_owned())]
            )
        }

        #[test]
        fn create_table() {
            let messages: Vec<BackendMessage> = QueryEvent::TableCreated.into();
            assert_eq!(
                messages,
                vec![BackendMessage::CommandComplete("CREATE TABLE".to_owned())]
            );
        }

        #[test]
        fn drop_table() {
            let messages: Vec<BackendMessage> = QueryEvent::TableDropped.into();
            assert_eq!(messages, vec![BackendMessage::CommandComplete("DROP TABLE".to_owned())]);
        }

        #[test]
        fn insert_record() {
            let records_number = 3;
            let messages: Vec<BackendMessage> = QueryEvent::RecordsInserted(records_number).into();
            assert_eq!(
                messages,
                vec![BackendMessage::CommandComplete(format!("INSERT 0 {}", records_number))]
            )
        }

        #[test]
        fn select_records() {
            let projection = (
                vec![
                    ("column_name_1".to_owned(), PostgreSqlType::SmallInt),
                    ("column_name_2".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["1".to_owned(), "2".to_owned()],
                    vec!["3".to_owned(), "4".to_owned()],
                ],
            );
            let messages: Vec<BackendMessage> = QueryEvent::RecordsSelected(projection).into();
            assert_eq!(
                messages,
                vec![
                    BackendMessage::RowDescription(vec![
                        ColumnMetadata::new("column_name_1".to_owned(), 21, 2),
                        ColumnMetadata::new("column_name_2".to_owned(), 21, 2)
                    ]),
                    BackendMessage::DataRow(vec!["1".to_owned(), "2".to_owned()]),
                    BackendMessage::DataRow(vec!["3".to_owned(), "4".to_owned()]),
                    BackendMessage::CommandComplete("SELECT 2".to_owned())
                ]
            );
        }

        #[test]
        fn update_records() {
            let records_number = 3;
            let messages: Vec<BackendMessage> = QueryEvent::RecordsUpdated(records_number).into();
            assert_eq!(
                messages,
                vec![BackendMessage::CommandComplete(format!("UPDATE {}", records_number))]
            );
        }

        #[test]
        fn delete_records() {
            let records_number = 3;
            let messages: Vec<BackendMessage> = QueryEvent::RecordsDeleted(records_number).into();
            assert_eq!(
                messages,
                vec![BackendMessage::CommandComplete(format!("DELETE {}", records_number))]
            )
        }

        #[test]
        fn describe_prepared_statement() {
            let messages: Vec<BackendMessage> = QueryEvent::PreparedStatementDescribed(
                vec![PostgreSqlType::SmallInt],
                vec![("si_column".to_owned(), PostgreSqlType::SmallInt)],
            )
            .into();
            assert_eq!(
                messages,
                [
                    BackendMessage::ParameterDescription(vec![21]),
                    BackendMessage::RowDescription(vec![ColumnMetadata {
                        name: "si_column".to_owned(),
                        type_id: 21,
                        type_size: 2
                    }])
                ]
            )
        }

        #[test]
        fn complete_parse() {
            let messages: Vec<BackendMessage> = QueryEvent::ParseComplete.into();
            assert_eq!(messages, [BackendMessage::ParseComplete])
        }

        #[test]
        fn complete_query() {
            let messages: Vec<BackendMessage> = QueryEvent::QueryComplete.into();
            assert_eq!(messages, [BackendMessage::ReadyForQuery])
        }
    }

    #[cfg(test)]
    mod query_error {
        use super::*;

        #[test]
        fn schema_already_exists() {
            let schema_name = "some_table_name".to_owned();
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .schema_already_exists(schema_name.clone())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42P06"),
                    Some(format!("schema \"{}\" already exists", schema_name)),
                )]
            )
        }

        #[test]
        fn schema_does_not_exists() {
            let schema_name = "some_table_name".to_owned();
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .schema_does_not_exist(schema_name.clone())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("3F000"),
                    Some(format!("schema \"{}\" does not exist", schema_name)),
                )]
            )
        }

        #[test]
        fn table_already_exists() {
            let table_name = "some_table_name".to_owned();
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .table_already_exists(table_name.clone())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42P07"),
                    Some(format!("table \"{}\" already exists", table_name)),
                )]
            )
        }

        #[test]
        fn table_does_not_exists() {
            let table_name = "some_table_name".to_owned();
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .table_does_not_exist(table_name.clone())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42P01"),
                    Some(format!("table \"{}\" does not exist", table_name)),
                )]
            )
        }

        #[test]
        fn one_column_does_not_exists() {
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .column_does_not_exist(vec!["column_not_in_table".to_owned()])
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42703"),
                    Some("column column_not_in_table does not exist".to_owned()),
                )]
            )
        }

        #[test]
        fn multiple_columns_does_not_exists() {
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .column_does_not_exist(vec![
                    "column_not_in_table1".to_owned(),
                    "column_not_in_table2".to_owned(),
                ])
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42703"),
                    Some("columns column_not_in_table1, column_not_in_table2 do not exist".to_owned()),
                )]
            )
        }

        #[test]
        fn prepared_statement_does_not_exists() {
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .prepared_statement_does_not_exist("statement_name".to_owned())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("26000"),
                    Some("prepared statement statement_name does not exist".to_owned()),
                )]
            )
        }

        #[test]
        fn feature_not_supported() {
            let raw_sql_query = "some SQL query".to_owned();
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .feature_not_supported(raw_sql_query.clone())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("0A000"),
                    Some(format!("Currently, Query '{}' can't be executed", raw_sql_query)),
                )]
            )
        }

        #[test]
        fn too_many_insert_expressions() {
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new().too_many_insert_expressions().build().into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42601"),
                    Some("INSERT has more expressions than target columns".to_owned()),
                )]
            )
        }

        #[test]
        fn out_of_range_constraint_violation() {
            let mut builder = QueryErrorBuilder::new();
            builder.out_of_range(PostgreSqlType::SmallInt, "col1".to_string(), 1);
            let messages: Vec<BackendMessage> = builder.build().into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("22003"),
                    Some("smallint is out of range for column 'col1' at row 1".to_owned())
                )]
            )
        }

        #[test]
        fn type_mismatch_constraint_violation() {
            let mut builder = QueryErrorBuilder::new();
            builder.type_mismatch("abc", PostgreSqlType::SmallInt, "col1".to_string(), 1);
            let messages: Vec<BackendMessage> = builder.build().into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("2200G"),
                    Some("invalid input syntax for type smallint for column 'col1' at row 1: \"abc\"".to_owned())
                )]
            )
        }

        #[test]
        fn string_length_mismatch_constraint_violation() {
            let mut builder = QueryErrorBuilder::new();
            builder.string_length_mismatch(PostgreSqlType::Char, 5, "col1".to_string(), 1);
            let messages: Vec<BackendMessage> = builder.build().into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("22026"),
                    Some("value too long for type character(5) for column 'col1' at row 1".to_owned())
                )]
            )
        }

        #[test]
        fn undefined_function() {
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .undefined_function("||".to_owned(), "NUMBER".to_owned(), "NUMBER".to_owned())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42883"),
                    Some("operator does not exist: (NUMBER || NUMBER)".to_owned())
                )]
            )
        }

        #[test]
        fn syntax_error() {
            let messages: Vec<BackendMessage> = QueryErrorBuilder::new()
                .syntax_error("expression".to_owned())
                .build()
                .into();
            assert_eq!(
                messages,
                vec![BackendMessage::ErrorResponse(
                    Some("ERROR"),
                    Some("42601"),
                    Some("syntax error in expression".to_owned())
                )]
            )
        }
    }

    #[cfg(test)]
    mod severity {
        use super::*;

        #[test]
        fn error() {
            let severity: &'static str = Severity::Error.into();
            assert_eq!(severity, "ERROR")
        }

        #[test]
        fn fatal() {
            let severity: &'static str = Severity::Fatal.into();
            assert_eq!(severity, "FATAL")
        }

        #[test]
        fn panic() {
            let severity: &'static str = Severity::Panic.into();
            assert_eq!(severity, "PANIC")
        }

        #[test]
        fn warning() {
            let severity: &'static str = Severity::Warning.into();
            assert_eq!(severity, "WARNING")
        }

        #[test]
        fn notice() {
            let severity: &'static str = Severity::Notice.into();
            assert_eq!(severity, "NOTICE")
        }

        #[test]
        fn debug() {
            let severity: &'static str = Severity::Debug.into();
            assert_eq!(severity, "DEBUG")
        }

        #[test]
        fn info() {
            let severity: &'static str = Severity::Info.into();
            assert_eq!(severity, "INFO")
        }

        #[test]
        fn log() {
            let severity: &'static str = Severity::Log.into();
            assert_eq!(severity, "LOG")
        }
    }
}
