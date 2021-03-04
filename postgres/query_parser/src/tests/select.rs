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

use super::*;

#[test]
fn select_all_from_table() {
    let statements = QUERY_PARSER.parse("select * from schema_name.table_name;");

    assert_eq!(
        statements,
        Ok(vec![Statement::DML(Query::Select(SelectStatement {
            select_items: vec![SelectItem::Wildcard],
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            where_clause: None,
        }))])
    );
}
