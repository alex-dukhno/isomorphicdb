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

fn insert_with_parameters(full_name: Vec<&'static str>, parameters: Vec<&'static str>) -> sql_ast::Statement {
    insert_with_values(
        full_name,
        vec![parameters
            .into_iter()
            .map(ident)
            .map(sql_ast::Expr::Identifier)
            .collect()],
    )
}

#[test]
fn insert_into_table_with_parameters() {
    let (data_definition, schema_id, table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::SmallInt),
        ColumnDefinition::new("col_2", SqlType::SmallInt),
    ]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(insert_with_parameters(vec![SCHEMA, TABLE], vec!["$1", "$2"])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_types: vec![SqlType::SmallInt, SqlType::SmallInt],
            values: vec![vec![
                InsertTreeNode::Item(Operator::Param(0)),
                InsertTreeNode::Item(Operator::Param(1))
            ]],
        })))
    );
}

#[test]
fn insert_into_table_with_parameters_and_values() {
    let (data_definition, schema_id, table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::SmallInt),
        ColumnDefinition::new("col_2", SqlType::SmallInt),
    ]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(insert_with_values(
            vec![SCHEMA, TABLE],
            vec![vec![
                sql_ast::Expr::Identifier(ident("$1")),
                sql_ast::Expr::Value(number(1))
            ]]
        )),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_types: vec![SqlType::SmallInt, SqlType::SmallInt],
            values: vec![vec![
                InsertTreeNode::Item(Operator::Param(0)),
                InsertTreeNode::Item(Operator::Const(ScalarValue::Number(BigDecimal::from(1))))
            ]],
        })))
    );
}
