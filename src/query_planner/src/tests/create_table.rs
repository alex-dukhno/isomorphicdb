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

use super::*;
use protocol::results::QueryError;
use sqlparser::ast::{ColumnDef, DataType, Ident, Statement};

#[rstest::rstest]
fn create_table_with_unsupported_type(planner_and_sender: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert!(matches!(
        query_planner.plan(Statement::CreateTable {
            name: ObjectName(vec![ident("schema_name"), ident("table_name"),]),
            columns: vec![ColumnDef {
                name: ident("column_name"),
                data_type: DataType::Custom(ObjectName(vec![ident("strange_type_name_whatever")])),
                collation: None,
                options: vec![]
            }],
            constraints: vec![],
            with_options: vec![],
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
        }),
        Err(_)
    ));

    collector.assert_content(vec![Err(QueryError::feature_not_supported(
        "strange_type_name_whatever type is not supported",
    ))])
}

fn ident<S: ToString>(name: S) -> Ident {
    Ident {
        value: name.to_string(),
        quote_style: None,
    }
}
