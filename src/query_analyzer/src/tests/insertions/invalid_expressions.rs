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

#[test]
fn insert_identifier() {
    let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(insert_with_values(
            vec![SCHEMA, TABLE],
            vec![vec![ast::Expr::Identifier(ident("col"))]]
        )),
        Err(AnalysisError::column_cant_be_referenced(&"col"))
    );
}

#[cfg(test)]
mod implicit_cast {
    use super::*;

    #[test]
    fn string_to_boolean() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::SingleQuotedString(
                    "string".to_owned()
                ))]]
            )),
            Err(AnalysisError::invalid_input_syntax_for_type(SqlType::Bool, &"string"))
        );
    }

    #[test]
    fn boolean_to_number() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::Boolean(true))]]
            )),
            Err(AnalysisError::datatype_mismatch(SqlType::SmallInt, SqlType::Bool))
        );
    }

    #[test]
    fn string_to_number() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::SingleQuotedString(
                    "some garbage".to_owned()
                ))]]
            )),
            Err(AnalysisError::invalid_input_syntax_for_type(
                SqlType::SmallInt,
                &"some garbage"
            ))
        );
    }
}
