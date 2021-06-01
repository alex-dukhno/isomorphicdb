// Copyright 2020 - 2021 Alex Dukhno
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

use std::fmt::{self, Display, Formatter};
use wire_protocol_payload::*;

/// Represents selected columns from tables
pub type Description = Vec<(String, u32)>;

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
    /// Index successfully created
    IndexCreated,
    /// Variable successfully set
    VariableSet,
    /// Transaction is started
    TransactionStarted,
    /// Number of records inserted into a table
    RecordsInserted(usize),
    /// Row description information
    RowDescription(Vec<(String, u32)>),
    /// Row data
    DataRow(Vec<String>),
    /// Records selected from database
    RecordsSelected(usize),
    /// Number of records updated into a table
    RecordsUpdated(usize),
    /// Number of records deleted into a table
    RecordsDeleted(usize),
    /// Prepared statement successfully prepared for execution
    StatementPrepared,
    /// Prepared statement successfully deallocated
    StatementDeallocated,
    /// Prepared statement parameters
    StatementParameters(Vec<u32>),
    /// Prepare statement description
    StatementDescription(Description),
    /// Processing of the query is complete
    QueryComplete,
    /// Parsing the extended query is complete
    ParseComplete,
    /// Binding the extended query is complete
    BindComplete,
}

impl From<QueryEvent> for OutboundMessage {
    fn from(event: QueryEvent) -> OutboundMessage {
        match event {
            QueryEvent::SchemaCreated => OutboundMessage::SchemaCreated,
            QueryEvent::SchemaDropped => OutboundMessage::SchemaDropped,
            QueryEvent::TableCreated => OutboundMessage::TableCreated,
            QueryEvent::TableDropped => OutboundMessage::TableDropped,
            QueryEvent::IndexCreated => OutboundMessage::IndexCreated,
            QueryEvent::VariableSet => OutboundMessage::VariableSet,
            QueryEvent::TransactionStarted => OutboundMessage::TransactionBegin,
            QueryEvent::RecordsInserted(records) => OutboundMessage::RecordsInserted(records),
            QueryEvent::RowDescription(description) => OutboundMessage::RowDescription(description),
            QueryEvent::DataRow(row) => OutboundMessage::DataRow(row),
            QueryEvent::RecordsSelected(records) => OutboundMessage::RecordsSelected(records),
            QueryEvent::RecordsUpdated(records) => OutboundMessage::RecordsUpdated(records),
            QueryEvent::RecordsDeleted(records) => OutboundMessage::RecordsDeleted(records),
            QueryEvent::StatementPrepared => OutboundMessage::StatementPrepared,
            QueryEvent::StatementDeallocated => OutboundMessage::StatementDeallocated,
            QueryEvent::StatementParameters(param_types) => OutboundMessage::StatementParameters(param_types),
            QueryEvent::StatementDescription(description) => OutboundMessage::StatementDescription(description),
            QueryEvent::QueryComplete => OutboundMessage::ReadyForQuery,
            QueryEvent::ParseComplete => OutboundMessage::ParseComplete,
            QueryEvent::BindComplete => OutboundMessage::BindComplete,
        }
    }
}

impl From<QueryEvent> for Vec<u8> {
    fn from(event: QueryEvent) -> Vec<u8> {
        fn command_complete(command: &str) -> Vec<u8> {
            let mut command_buff = Vec::new();
            command_buff.extend_from_slice(&[COMMAND_COMPLETE]);
            command_buff.extend_from_slice(&(4 + command.len() as i32 + 1).to_be_bytes());
            command_buff.extend_from_slice(command.as_bytes());
            command_buff.extend_from_slice(&[0]);
            command_buff
        }

        /// Returns PostgreSQL type length
        pub fn type_len(oid: u32) -> i16 {
            match oid {
                BOOL => 1,
                CHAR => 1,
                BIGINT => 8,
                SMALLINT => 2,
                INT => 4,
                VARCHAR => -1,
                _ => unimplemented!(),
            }
        }

        match event {
            QueryEvent::SchemaCreated => command_complete("CREATE SCHEMA"),
            QueryEvent::SchemaDropped => command_complete("DROP SCHEMA"),
            QueryEvent::TableCreated => command_complete("CREATE TABLE"),
            QueryEvent::TableDropped => command_complete("DROP TABLE"),
            QueryEvent::IndexCreated => command_complete("CREATE INDEX"),
            QueryEvent::VariableSet => command_complete("SET"),
            QueryEvent::TransactionStarted => command_complete("BEGIN"),
            QueryEvent::RecordsInserted(records) => command_complete(format!("INSERT 0 {}", records).as_str()),
            QueryEvent::RowDescription(description) => {
                let mut buff = Vec::new();
                let len = description.len();
                for (name, oid) in description {
                    buff.extend_from_slice(name.as_bytes());
                    buff.extend_from_slice(&[0]); // end of c string
                    buff.extend_from_slice(&(0i32).to_be_bytes()); // table id
                    buff.extend_from_slice(&(0i16).to_be_bytes()); // column id
                    buff.extend_from_slice(&oid.to_be_bytes());
                    buff.extend_from_slice(&(type_len(oid)).to_be_bytes());
                    buff.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
                    buff.extend_from_slice(&0i16.to_be_bytes());
                }
                let mut len_buff = Vec::new();
                len_buff.extend_from_slice(&[ROW_DESCRIPTION]);
                len_buff.extend_from_slice(&(6 + buff.len() as i32).to_be_bytes());
                len_buff.extend_from_slice(&(len as i16).to_be_bytes());
                len_buff.extend_from_slice(&buff);
                len_buff
            }
            QueryEvent::DataRow(row) => {
                let mut row_buff = Vec::new();
                for field in row.iter() {
                    row_buff.extend_from_slice(&(field.len() as i32).to_be_bytes());
                    row_buff.extend_from_slice(field.as_str().as_bytes());
                }
                let mut len_buff = Vec::new();
                len_buff.extend_from_slice(&[DATA_ROW]);
                len_buff.extend_from_slice(&(6 + row_buff.len() as i32).to_be_bytes());
                len_buff.extend_from_slice(&(row.len() as i16).to_be_bytes());
                len_buff.extend_from_slice(&row_buff);
                len_buff
            }
            QueryEvent::RecordsSelected(records) => command_complete(format!("SELECT {}", records).as_str()),
            QueryEvent::RecordsUpdated(records) => command_complete(format!("UPDATE {}", records).as_str()),
            QueryEvent::RecordsDeleted(records) => command_complete(format!("DELETE {}", records).as_str()),
            QueryEvent::StatementPrepared => command_complete("PREPARE"),
            QueryEvent::StatementDeallocated => command_complete("DEALLOCATE"),
            QueryEvent::StatementParameters(param_types) => {
                let mut type_id_buff = Vec::new();
                for oid in param_types.iter() {
                    type_id_buff.extend_from_slice(&oid.to_be_bytes());
                }
                let mut buff = Vec::new();
                buff.extend_from_slice(&[PARAMETER_DESCRIPTION]);
                buff.extend_from_slice(&(6 + type_id_buff.len() as i32).to_be_bytes());
                buff.extend_from_slice(&(param_types.len() as i16).to_be_bytes());
                buff.extend_from_slice(&type_id_buff);
                buff
            }
            QueryEvent::StatementDescription(description) => {
                if description.is_empty() {
                    vec![NO_DATA, 0, 0, 0, 4]
                } else {
                    let mut buff = Vec::new();
                    let len = description.len();
                    for (name, oid) in description {
                        buff.extend_from_slice(name.as_bytes());
                        buff.extend_from_slice(&[0]); // end of c string
                        buff.extend_from_slice(&(0i32).to_be_bytes()); // table id
                        buff.extend_from_slice(&(0i16).to_be_bytes()); // column id
                        buff.extend_from_slice(&oid.to_be_bytes());
                        buff.extend_from_slice(&(type_len(oid)).to_be_bytes());
                        buff.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
                        buff.extend_from_slice(&0i16.to_be_bytes());
                    }
                    let mut len_buff = Vec::new();
                    len_buff.extend_from_slice(&[ROW_DESCRIPTION]);
                    len_buff.extend_from_slice(&(6 + buff.len() as i32).to_be_bytes());
                    len_buff.extend_from_slice(&(len as i16).to_be_bytes());
                    len_buff.extend_from_slice(&buff);
                    len_buff
                }
            }
            QueryEvent::QueryComplete => vec![READY_FOR_QUERY, 0, 0, 0, 5, EMPTY_QUERY_RESPONSE],
            QueryEvent::ParseComplete => vec![PARSE_COMPLETE, 0, 0, 0, 4],
            QueryEvent::BindComplete => vec![BIND_COMPLETE, 0, 0, 0, 4],
        }
    }
}

/// Message severities
/// Reference: defined in https://www.postgresql.org/docs/12/protocol-error-fields.html
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[allow(dead_code)]
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
impl From<Severity> for &'static str {
    fn from(severity: Severity) -> &'static str {
        match severity {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            Severity::Panic => "PANIC",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Log => "LOG",
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
    ColumnDoesNotExist(String),
    IndeterminateParameterDataType {
        param_index: usize,
    },
    InvalidParameterValue(String),
    PreparedStatementDoesNotExist(String),
    PortalDoesNotExist(String),
    TypeDoesNotExist(String),
    ProtocolViolation(String),
    FeatureNotSupported(String),
    TooManyInsertExpressions,
    NumericTypeOutOfRange {
        pg_type: u32,
        column_name: String,
        row_index: usize, // TODO make it optional - does not make sense for update query
    },
    NumericTypeOutOfRange2 {
        pg_type: String,
        column_name: String,
        row_index: usize, // TODO make it optional - does not make sense for update query
    },
    MostSpecificTypeMismatch {
        pg_type: u32,
        value: String,
        column_name: String,
        row_index: usize, // TODO make it optional - does not make sense for update query
    },
    MostSpecificTypeMismatch2 {
        pg_type: String,
        value: String,
        column_name: String,
        row_index: usize, // TODO make it optional - does not make sense for update query
    },
    StringTypeLengthMismatch {
        pg_type: u32,
        len: u64,
        column_name: String,
        row_index: usize, // TODO make it optional - does not make sense for update query
    },
    UndefinedBinaryFunction {
        operator: String,
        left_type: String,
        right_type: String,
    },
    UndefinedUnaryFunction {
        operator: String,
        sql_type: String,
    },
    AmbiguousColumnName {
        column: String,
    },
    UndefinedColumn {
        column: String,
    },
    SyntaxError(String),
    InvalidTextRepresentation {
        pg_type: u32,
        value: String,
    },
    DuplicateColumn(String),
    DatatypeMismatch {
        op: String,
        // TODO: make them not Strings
        target_type: String,
        actual_type: String,
    },
    InvalidArgumentForPowerFunction,
    InvalidTextRepresentation2(String, String),
    CannotCoerce(String, String),
    AmbiguousFunction(String, String),
}

impl QueryErrorKind {
    fn code(&self) -> &'static str {
        match self {
            QueryErrorKind::SchemaAlreadyExists(_) => "42P06",
            QueryErrorKind::TableAlreadyExists(_) => "42P07",
            QueryErrorKind::SchemaDoesNotExist(_) => "3F000",
            QueryErrorKind::SchemaHasDependentObjects(_) => "2BP01",
            QueryErrorKind::TableDoesNotExist(_) => "42P01",
            QueryErrorKind::ColumnDoesNotExist(_) => "42703",
            QueryErrorKind::IndeterminateParameterDataType { .. } => "42P18",
            QueryErrorKind::InvalidParameterValue(_) => "22023",
            QueryErrorKind::PreparedStatementDoesNotExist(_) => "26000",
            QueryErrorKind::PortalDoesNotExist(_) => "26000",
            QueryErrorKind::TypeDoesNotExist(_) => "42704",
            QueryErrorKind::ProtocolViolation(_) => "08P01",
            QueryErrorKind::FeatureNotSupported(_) => "0A000",
            QueryErrorKind::TooManyInsertExpressions => "42601",
            QueryErrorKind::NumericTypeOutOfRange { .. } => "22003",
            QueryErrorKind::NumericTypeOutOfRange2 { .. } => "22003",
            QueryErrorKind::MostSpecificTypeMismatch { .. } => "2200G",
            QueryErrorKind::MostSpecificTypeMismatch2 { .. } => "2200G",
            QueryErrorKind::StringTypeLengthMismatch { .. } => "22026",
            QueryErrorKind::UndefinedBinaryFunction { .. } => "42883",
            QueryErrorKind::UndefinedUnaryFunction { .. } => "42883",
            QueryErrorKind::AmbiguousColumnName { .. } => "42702",
            QueryErrorKind::UndefinedColumn { .. } => "42883",
            QueryErrorKind::SyntaxError(_) => "42601",
            QueryErrorKind::InvalidTextRepresentation { .. } => "22P02",
            QueryErrorKind::InvalidTextRepresentation2(_, _) => "22P02",
            QueryErrorKind::DuplicateColumn(_) => "42701",
            QueryErrorKind::DatatypeMismatch { .. } => "42804",
            QueryErrorKind::InvalidArgumentForPowerFunction => "2201F",
            QueryErrorKind::CannotCoerce(_, _) => "42846",
            QueryErrorKind::AmbiguousFunction(_, _) => "42725",
        }
    }
}

impl Display for QueryErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            QueryErrorKind::SchemaAlreadyExists(schema_name) => write!(f, "schema \"{}\" already exists", schema_name),
            QueryErrorKind::TableAlreadyExists(table_name) => write!(f, "table \"{}\" already exists", table_name),
            QueryErrorKind::SchemaDoesNotExist(schema_name) => write!(f, "schema \"{}\" does not exist", schema_name),
            QueryErrorKind::SchemaHasDependentObjects(schema_name) => {
                write!(f, "schema \"{}\" has dependent objects", schema_name)
            }
            QueryErrorKind::TableDoesNotExist(table_name) => write!(f, "table \"{}\" does not exist", table_name),
            QueryErrorKind::ColumnDoesNotExist(column) => write!(f, "column {} does not exist", column),
            QueryErrorKind::IndeterminateParameterDataType { param_index } => {
                write!(f, "could not determine data type of parameter ${}", param_index + 1)
            }
            QueryErrorKind::InvalidParameterValue(message) => write!(f, "{}", message),
            QueryErrorKind::PreparedStatementDoesNotExist(statement_name) => {
                write!(f, "prepared statement {} does not exist", statement_name)
            }
            QueryErrorKind::PortalDoesNotExist(portal_name) => write!(f, "portal {} does not exist", portal_name),
            QueryErrorKind::TypeDoesNotExist(type_name) => write!(f, "type \"{}\" does not exist", type_name),
            QueryErrorKind::ProtocolViolation(message) => write!(f, "{}", message),
            QueryErrorKind::FeatureNotSupported(raw_sql_query) => {
                write!(f, "Currently, Query '{}' can't be executed", raw_sql_query)
            }
            QueryErrorKind::TooManyInsertExpressions => write!(f, "INSERT has more expressions than target columns"),
            QueryErrorKind::NumericTypeOutOfRange {
                pg_type,
                column_name,
                row_index,
            } => write!(f, "{} is out of range for column '{}' at row {}", pg_type, column_name, row_index),
            QueryErrorKind::NumericTypeOutOfRange2 {
                pg_type,
                column_name,
                row_index,
            } => write!(f, "{} is out of range for column '{}' at row {}", pg_type, column_name, row_index),
            QueryErrorKind::MostSpecificTypeMismatch {
                pg_type,
                value,
                column_name,
                row_index,
            } => write!(
                f,
                "invalid input syntax for type {} for column '{}' at row {}: \"{}\"",
                pg_type, column_name, row_index, value
            ),
            QueryErrorKind::MostSpecificTypeMismatch2 {
                pg_type,
                value,
                column_name,
                row_index,
            } => write!(
                f,
                "invalid input syntax for type {} for column '{}' at row {}: \"{}\"",
                pg_type, column_name, row_index, value
            ),
            QueryErrorKind::StringTypeLengthMismatch {
                pg_type,
                len,
                column_name,
                row_index,
            } => write!(
                f,
                "value too long for type {}({}) for column '{}' at row {}",
                pg_type, len, column_name, row_index
            ),
            QueryErrorKind::UndefinedBinaryFunction {
                operator,
                left_type,
                right_type,
            } => write!(f, "operator does not exist: ({} {} {})", left_type, operator, right_type),
            QueryErrorKind::UndefinedUnaryFunction { operator, sql_type } => {
                write!(f, "operator does not exist: {} {}", operator, sql_type)
            }
            QueryErrorKind::AmbiguousColumnName { column } => write!(f, "use of ambiguous column name in context: '{}'", column),
            QueryErrorKind::UndefinedColumn { column } => write!(f, "use of undefined column: '{}'", column),
            QueryErrorKind::SyntaxError(expression) => write!(f, "syntax error: {}", expression),
            QueryErrorKind::InvalidTextRepresentation { pg_type, value } => {
                write!(f, "invalid input syntax for type {}: \"{}\"", pg_type, value)
            }
            QueryErrorKind::InvalidTextRepresentation2(sql_type, value) => {
                write!(f, "invalid input syntax for type {}: \"{}\"", sql_type, value)
            }
            QueryErrorKind::DuplicateColumn(name) => write!(f, "column \"{}\" specified more than once", name),
            QueryErrorKind::DatatypeMismatch {
                op,
                target_type,
                actual_type,
            } => write!(f, "argument of {} must be type {}, not type {}", op, target_type, actual_type),
            QueryErrorKind::InvalidArgumentForPowerFunction => write!(f, "cannot take square root of a negative number"),
            QueryErrorKind::CannotCoerce(from_type, to_type) => write!(f, "cannot cast type {} to {}", from_type, to_type),
            QueryErrorKind::AmbiguousFunction(operator, sql_type) => write!(f, " operator is not unique: {} {}", operator, sql_type),
        }
    }
}

/// Represents error during query execution
#[derive(Debug, PartialEq, Clone)]
pub struct QueryError {
    severity: Severity,
    kind: QueryErrorKind,
}

impl QueryError {
    fn code(&self) -> &'static str {
        self.kind.code()
    }

    fn severity(&self) -> &'static str {
        let severity: &'static str = self.severity.into();
        severity
    }

    fn message(&self) -> String {
        format!("{}", self.kind)
    }
}

impl From<QueryError> for OutboundMessage {
    fn from(error: QueryError) -> Self {
        OutboundMessage::Error(error.severity().to_owned(), error.code().to_owned(), error.message())
    }
}

impl From<QueryError> for Vec<u8> {
    fn from(error: QueryError) -> Vec<u8> {
        let mut error_response_buff = Vec::new();
        error_response_buff.extend_from_slice(&[ERROR_RESPONSE]);
        let mut message_buff = Vec::new();
        message_buff.extend_from_slice(&[SEVERITY]);
        message_buff.extend_from_slice(error.severity().as_bytes());
        message_buff.extend_from_slice(&[0]);
        message_buff.extend_from_slice(&[CODE]);
        message_buff.extend_from_slice(error.code().as_bytes());
        message_buff.extend_from_slice(&[0]);
        message_buff.extend_from_slice(&[MESSAGE]);
        message_buff.extend_from_slice(error.message().as_bytes());
        message_buff.extend_from_slice(&[0]);
        error_response_buff.extend_from_slice(&(message_buff.len() as i32 + 4 + 1).to_be_bytes());
        error_response_buff.extend_from_slice(message_buff.as_ref());
        error_response_buff.extend_from_slice(&[0]);
        error_response_buff.to_vec()
    }
}

impl QueryError {
    /// schema already exists error constructor
    pub fn schema_already_exists<S: ToString>(schema_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::SchemaAlreadyExists(schema_name.to_string()),
        }
    }

    /// schema does not exist error constructor
    pub fn schema_does_not_exist<S: ToString>(schema_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::SchemaDoesNotExist(schema_name.to_string()),
        }
    }

    /// schema has dependent objects error constructor
    pub fn schema_has_dependent_objects<S: ToString>(schema_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::SchemaHasDependentObjects(schema_name.to_string()),
        }
    }

    /// table already exists error constructor
    pub fn table_already_exists<S: ToString>(table_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::TableAlreadyExists(table_name.to_string()),
        }
    }

    /// table does not exist error constructor
    pub fn table_does_not_exist<S: ToString>(table_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::TableDoesNotExist(table_name.to_string()),
        }
    }

    /// column does not exists error constructor
    pub fn column_does_not_exist<S: ToString>(non_existing_column: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::ColumnDoesNotExist(non_existing_column.to_string()),
        }
    }

    /// indeterminate parameter data type constructor
    pub fn indeterminate_parameter_data_type(param_index: usize) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::IndeterminateParameterDataType { param_index },
        }
    }

    /// invalid parameter value error constructor
    pub fn invalid_parameter_value<S: ToString>(message: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::InvalidParameterValue(message.to_string()),
        }
    }

    /// prepared statement does not exist error constructor
    pub fn prepared_statement_does_not_exist<S: ToString>(statement_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::PreparedStatementDoesNotExist(statement_name.to_string()),
        }
    }

    /// portal does not exist error constructor
    pub fn portal_does_not_exist<S: ToString>(portal_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::PortalDoesNotExist(portal_name.to_string()),
        }
    }

    /// type does not exist error constructor
    pub fn type_does_not_exist<S: ToString>(type_name: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::TypeDoesNotExist(type_name.to_string()),
        }
    }

    /// protocol violation error constructor
    pub fn protocol_violation<S: ToString>(message: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::ProtocolViolation(message.to_string()),
        }
    }

    /// not supported operation error constructor
    pub fn feature_not_supported<S: ToString>(feature_description: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::FeatureNotSupported(feature_description.to_string()),
        }
    }

    /// too many insert expressions errors constructors
    pub fn too_many_insert_expressions() -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::TooManyInsertExpressions,
        }
    }

    /// syntax error in the expression as part of query
    pub fn syntax_error<S: ToString>(expression: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::SyntaxError(expression.to_string()),
        }
    }

    /// operator or function is not found for operands
    pub fn undefined_binary_function<O: ToString, S: ToString>(operator: O, left_type: S, right_type: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::UndefinedBinaryFunction {
                operator: operator.to_string(),
                left_type: left_type.to_string(),
                right_type: right_type.to_string(),
            },
        }
    }

    /// operator or function is not found for operands
    pub fn undefined_unary_function<O: ToString, S: ToString>(operator: O, sql_type: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::UndefinedUnaryFunction {
                operator: operator.to_string(),
                sql_type: sql_type.to_string(),
            },
        }
    }

    /// when the name of a column is ambiguous in a multi-table context
    pub fn ambiguous_column<S: ToString>(column: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::AmbiguousColumnName { column: column.to_string() },
        }
    }

    /// user of an undefined column
    pub fn undefined_column<S: ToString>(column: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::UndefinedColumn { column: column.to_string() },
        }
    }

    /// numeric out of range constructor
    pub fn out_of_range<S: ToString>(pg_type: u32, column_name: S, row_index: usize) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::NumericTypeOutOfRange {
                pg_type,
                column_name: column_name.to_string(),
                row_index,
            },
        }
    }

    /// numeric out of range constructor
    pub fn out_of_range_2<T: ToString, S: ToString>(pg_type: T, column_name: S, row_index: usize) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::NumericTypeOutOfRange2 {
                pg_type: pg_type.to_string(),
                column_name: column_name.to_string(),
                row_index,
            },
        }
    }

    pub fn datatype_mismatch(op: String, target_type: String, actual_type: String) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::DatatypeMismatch {
                op,
                target_type,
                actual_type,
            },
        }
    }

    /// type mismatch constructor
    pub fn most_specific_type_mismatch<S: ToString>(value: S, pg_type: u32, column_name: S, row_index: usize) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::MostSpecificTypeMismatch {
                pg_type,
                value: value.to_string(),
                column_name: column_name.to_string(),
                row_index,
            },
        }
    }

    /// type mismatch constructor
    pub fn most_specific_type_mismatch2(value: String, pg_type: String, column_name: String, row_index: usize) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::MostSpecificTypeMismatch2 {
                pg_type,
                value,
                column_name,
                row_index,
            },
        }
    }

    /// length of string types do not match constructor
    pub fn string_length_mismatch<S: ToString>(pg_type: u32, len: u64, column_name: S, row_index: usize) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::StringTypeLengthMismatch {
                pg_type,
                len,
                column_name: column_name.to_string(),
                row_index,
            },
        }
    }

    /// invalid text representation
    pub fn invalid_text_representation<S: ToString>(pg_type: u32, value: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::InvalidTextRepresentation {
                pg_type,
                value: value.to_string(),
            },
        }
    }

    /// invalid text representation
    pub fn invalid_text_representation_2<T: ToString, V: ToString>(sql_type: T, value: V) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::InvalidTextRepresentation2(sql_type.to_string(), value.to_string()),
        }
    }

    /// duplicate column
    pub fn duplicate_column<S: ToString>(column: S) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::DuplicateColumn(column.to_string()),
        }
    }

    pub fn invalid_argument_for_power_function() -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::InvalidArgumentForPowerFunction,
        }
    }

    pub fn cannot_coerce<FT: ToString, TT: ToString>(from_type: FT, to_type: TT) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::CannotCoerce(from_type.to_string(), to_type.to_string()),
        }
    }

    pub fn ambiguous_function<Op: ToString, Ty: ToString>(operator: Op, sql_type: Ty) -> QueryError {
        QueryError {
            severity: Severity::Error,
            kind: QueryErrorKind::AmbiguousFunction(operator.to_string(), sql_type.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
