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

use definition::FullTableName;
use exec_tree::ExecutableTree;

#[derive(Debug, PartialEq, Clone)]
pub struct ExecutableInsertQuery {
    pub full_table_name: FullTableName,
    pub values: Vec<Vec<Option<ExecutableTree>>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecutableDeleteQuery {
    pub full_table_name: FullTableName,
    pub filter: Option<ExecutableTree>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecutableUpdateQuery {
    pub full_table_name: FullTableName,
    pub assignments: Vec<Option<ExecutableTree>>,
    pub filter: Option<ExecutableTree>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecutableSelectQuery {
    pub full_table_name: FullTableName,
    pub projection_items: Vec<ExecutableTree>,
    pub filter: Option<ExecutableTree>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExecutableQuery {
    Insert(ExecutableInsertQuery),
    Delete(ExecutableDeleteQuery),
    Update(ExecutableUpdateQuery),
    Select(ExecutableSelectQuery),
}
