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

use definition::{FullTableName, SchemaName};
use expr_operators::{Operation, Operator};
use meta_def::Id;
use types::SqlType;

pub type AnalysisResult<A> = Result<A, AnalysisError>;

#[derive(Debug, PartialEq)]
pub struct FullTableId((Id, Id));

impl From<(Id, Id)> for FullTableId {
    fn from(tuple: (Id, Id)) -> FullTableId {
        FullTableId(tuple)
    }
}

impl AsRef<(Id, Id)> for FullTableId {
    fn as_ref(&self) -> &(Id, Id) {
        &self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct TableInfo {
    pub schema_id: Id,
    pub schema_name: String,
    pub table_name: String,
}

impl TableInfo {
    pub fn new<S: ToString, T: ToString>(schema_id: Id, schema_name: &S, table_name: &T) -> TableInfo {
        TableInfo {
            schema_id,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateSchemaQuery {
    pub schema_name: SchemaName,
    pub if_not_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropSchemasQuery {
    pub schema_names: Vec<SchemaName>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: SqlType,
}

#[derive(Debug, PartialEq)]
pub struct CreateTableQuery {
    pub table_info: TableInfo,
    pub column_defs: Vec<ColumnInfo>,
    pub if_not_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropTablesQuery {
    pub table_infos: Vec<TableInfo>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct InsertQuery {
    pub full_table_name: FullTableName,
    pub column_types: Vec<SqlType>,
    pub values: Vec<Vec<InsertTreeNode>>,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDesc {
    pub name: String,
    pub sql_type: SqlType,
    pub ord_num: usize,
}

impl From<(String, SqlType, usize)> for ColumnDesc {
    fn from(tuple: (String, SqlType, usize)) -> ColumnDesc {
        let (name, sql_type, ord_num) = tuple;
        ColumnDesc {
            name,
            sql_type,
            ord_num,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum InsertTreeNode {
    Operation {
        left: Box<InsertTreeNode>,
        op: Operation,
        right: Box<InsertTreeNode>,
    },
    Item(Operator),
}

#[derive(Debug, PartialEq)]
pub struct UpdateQuery {
    pub full_table_id: FullTableId,
    pub sql_types: Vec<SqlType>,
    pub assignments: Vec<UpdateTreeNode>,
}

#[derive(Debug, PartialEq)]
pub enum UpdateTreeNode {
    Operation {
        left: Box<UpdateTreeNode>,
        op: Operation,
        right: Box<UpdateTreeNode>,
    },
    Item(Operator),
}

#[derive(Debug, PartialEq)]
pub struct SelectQuery {
    pub full_table_id: FullTableId,
    pub projection_items: Vec<ProjectionTreeNode>,
}

#[derive(Debug, PartialEq)]
pub enum ProjectionTreeNode {
    Operation {
        left: Box<ProjectionTreeNode>,
        op: Operation,
        right: Box<ProjectionTreeNode>,
    },
    Item(Operator),
}

#[derive(Debug, PartialEq)]
pub struct DeleteQuery {
    pub full_table_id: FullTableId,
}

#[derive(Debug, PartialEq)]
pub enum SchemaChange {
    CreateSchema(CreateSchemaQuery),
    DropSchemas(DropSchemasQuery),
    CreateTable(CreateTableQuery),
    DropTables(DropTablesQuery),
}

#[derive(Debug, PartialEq)]
pub enum Write {
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
}

#[derive(Debug, PartialEq)]
pub enum QueryAnalysis {
    DataDefinition(SchemaChange),
    Write(Write),
    Read(SelectQuery),
}

#[derive(Debug, PartialEq)]
pub enum AnalysisError {
    SchemaNamingError(String),
    SchemaDoesNotExist(String),
    SchemaAlreadyExists(String),
    TableNamingError(String),
    TableDoesNotExist(String),
    TableAlreadyExists(String),
    TypeIsNotSupported(String),
    SyntaxError(String),
    ColumnNotFound(String),
    ColumnCantBeReferenced(String),                                  // Error code: 42703
    InvalidInputSyntaxForType { sql_type: SqlType, value: String },  // Error code: 22P02
    StringDataRightTruncation(SqlType),                              // Error code: 22001
    DatatypeMismatch { column_type: SqlType, source_type: SqlType }, // Error code: 42804
    AmbiguousFunction(Operation),                                    // Error code: 42725
    UndefinedFunction(Operation),                                    // Error code: 42883
    FeatureNotSupported(Feature),
}

impl AnalysisError {
    pub fn schema_naming_error<M: ToString>(message: M) -> AnalysisError {
        AnalysisError::SchemaNamingError(message.to_string())
    }

    pub fn schema_does_not_exist<S: ToString>(schema_name: S) -> AnalysisError {
        AnalysisError::SchemaDoesNotExist(schema_name.to_string())
    }

    pub fn schema_already_exists<S: ToString>(schema_name: S) -> AnalysisError {
        AnalysisError::SchemaAlreadyExists(schema_name.to_string())
    }

    pub fn table_naming_error<M: ToString>(message: M) -> AnalysisError {
        AnalysisError::TableNamingError(message.to_string())
    }

    pub fn table_does_not_exist<T: ToString>(table_name: T) -> AnalysisError {
        AnalysisError::TableDoesNotExist(table_name.to_string())
    }

    pub fn table_already_exists<T: ToString>(table_name: T) -> AnalysisError {
        AnalysisError::TableAlreadyExists(table_name.to_string())
    }

    pub fn type_is_not_supported<T: ToString>(type_name: T) -> AnalysisError {
        AnalysisError::TypeIsNotSupported(type_name.to_string())
    }

    pub fn syntax_error(message: String) -> AnalysisError {
        AnalysisError::SyntaxError(message)
    }

    pub fn column_not_found<C: ToString>(column_name: C) -> AnalysisError {
        AnalysisError::ColumnNotFound(column_name.to_string())
    }

    pub fn column_cant_be_referenced<C: ToString>(column_name: C) -> AnalysisError {
        AnalysisError::ColumnCantBeReferenced(column_name.to_string())
    }

    pub fn invalid_input_syntax_for_type<V: ToString>(sql_type: SqlType, value: V) -> AnalysisError {
        AnalysisError::InvalidInputSyntaxForType {
            sql_type,
            value: value.to_string(),
        }
    }

    pub fn string_data_right_truncation(sql_type: SqlType) -> AnalysisError {
        AnalysisError::StringDataRightTruncation(sql_type)
    }

    pub fn datatype_mismatch(column_type: SqlType, source_type: SqlType) -> AnalysisError {
        AnalysisError::DatatypeMismatch {
            column_type,
            source_type,
        }
    }

    pub fn feature_not_supported(feature: Feature) -> AnalysisError {
        AnalysisError::FeatureNotSupported(feature)
    }
}

#[derive(Debug, PartialEq)]
pub enum Feature {
    SetOperations,
    SubQueries,
    NationalStringLiteral,
    HexStringLiteral,
    TimeInterval,
    Joins,
    NestedJoin,
    FromSubQuery,
    TableFunctions,
    Aliases,
    QualifiedAliases,
    InsertIntoSelect,
}
