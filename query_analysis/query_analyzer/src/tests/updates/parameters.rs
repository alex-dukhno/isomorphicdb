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
fn update_table_with_parameters() {
    let (data_definition, schema_id, table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::SmallInt),
        ColumnDefinition::new("col_2", SqlType::Integer),
    ]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_stmt_with_parameters(vec![SCHEMA, TABLE])),
        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            sql_types: vec![SqlType::Integer],
            assignments: vec![UpdateTreeNode::Item(Operator::Param(0))]
        })))
    );
}
