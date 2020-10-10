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

use sqlparser::{ast::Statement, dialect::Dialect, parser::Parser};
use std::fmt::{self, Display, Formatter};

#[derive(Debug, PartialEq)]
pub struct ParserError(String);

impl From<&str> for ParserError {
    fn from(query: &str) -> Self {
        ParserError(query.to_owned())
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} can't be parsed", self.0)
    }
}

#[derive(Default)]
pub struct QueryParser {
    dialect: PreparedStatementDialect,
}

impl QueryParser {
    pub fn parse(&self, query: &str) -> Result<Vec<Statement>, ParserError> {
        match Parser::parse_sql(&self.dialect, query) {
            Ok(statements) => {
                log::trace!("stmts: {:#?}", statements);
                Ok(statements)
            }
            Err(e) => {
                log::error!("{:?} can't be parsed. Error: {:?}", query, e);
                Err(query.into())
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
            Err(ParserError::from("selec col from schema_name.table_name"))
        );
    }
}
