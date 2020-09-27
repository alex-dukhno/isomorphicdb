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

use protocol::results::QueryError;
use sqlparser::{ast::Statement, dialect::Dialect, parser::Parser};

#[derive(Default)]
pub struct QueryParser {
    dialect: PreparedStatementDialect,
}

impl QueryParser {
    pub fn parse(&self, query: &str) -> Result<Vec<Statement>, QueryError> {
        match Parser::parse_sql(&self.dialect, query) {
            Ok(statements) => {
                log::trace!("stmts: {:#?}", statements);
                Ok(statements)
            }
            Err(e) => {
                log::error!("{:?} can't be parsed. Error: {:?}", query, e);
                Err(QueryError::syntax_error(format!("{:?} can't be parsed", query)))
            }
        }
    }
}

#[derive(Debug, Default)]
struct PreparedStatementDialect;

impl Dialect for PreparedStatementDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '$' || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '$' || ch == '_'
    }
}

#[cfg(test)]
mod parsing_errors {
    use super::*;

    #[test]
    fn parse_wrong_select_syntax() {
        assert_eq!(
            QueryParser::default().parse("selec col from schema_name.table_name"),
            Err(QueryError::syntax_error(
                "\"selec col from schema_name.table_name\" can\'t be parsed"
            ))
        );
    }
}
