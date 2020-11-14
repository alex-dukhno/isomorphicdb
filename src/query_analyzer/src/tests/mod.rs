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

mod ddl;
mod insert;
mod select;

use super::*;
use meta_def::ColumnDefinition;
use sql_model::{sql_types::SqlType, DEFAULT_CATALOG};
use sqlparser::ast::{Expr, Ident, ObjectName, Query, SetExpr, Value, Values};
use std::sync::Arc;

const SCHEMA: &str = "schema_name";
const TABLE: &str = "table_name";

fn ident<S: ToString>(name: S) -> Ident {
    Ident {
        value: name.to_string(),
        quote_style: None,
    }
}
