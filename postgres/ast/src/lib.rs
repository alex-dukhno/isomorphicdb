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

use std::fmt::Debug;

#[derive(Debug, PartialEq)]
pub enum Statement {
    DDL(Definition),
    DDM(Manipulation),
}

#[derive(Debug, PartialEq)]
pub enum Definition {
    CreateSchema {
        schema_name: String,
        if_not_exists: bool,
    },
    CreateTable {
        schema_name: String,
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    CreateIndex {
        name: String,
        table_name: (String, String),
        column_names: Vec<String>,
    },
    DropSchemas {
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },
    DropTables {
        names: Vec<(String, String)>,
        if_exists: bool,
        cascade: bool,
    },
}

#[derive(Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, PartialEq)]
pub enum DataType {
    SmallInt,
    Int,
    BigInt,
    Char(u32),
    VarChar(Option<u32>),
    Real,
    Double,
    Bool,
}

#[derive(Debug, PartialEq)]
pub enum ObjectType {
    Schema,
    Table,
}

#[derive(Debug, PartialEq)]
pub enum Manipulation {}
