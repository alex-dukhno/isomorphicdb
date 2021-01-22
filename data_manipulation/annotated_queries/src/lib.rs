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

use data_manipulation_untyped_tree::{DynamicEvaluationTree, StaticEvaluationTree};
use definition::FullTableName;
use types::SqlType;

#[derive(Debug, PartialEq)]
pub struct InsertQuery {
    pub full_table_name: FullTableName,
    pub column_types: Vec<SqlType>,
    pub values: Vec<Vec<StaticEvaluationTree>>,
}

#[derive(Debug, PartialEq)]
pub struct UpdateQuery {
    pub full_table_name: FullTableName,
    pub sql_types: Vec<SqlType>,
    pub assignments: Vec<DynamicEvaluationTree>,
}

#[derive(Debug, PartialEq)]
pub struct SelectQuery {
    pub full_table_name: FullTableName,
    pub projection_items: Vec<DynamicEvaluationTree>,
}

#[derive(Debug, PartialEq)]
pub struct DeleteQuery {
    pub full_table_name: FullTableName,
}

#[derive(Debug, PartialEq)]
pub enum Write {
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
}
