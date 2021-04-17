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

use data_manipulation::UntypedQuery;
use entities::SqlTypeFamily;
use postgre_sql::query_ast::Query;
use scalar::ScalarValue;

#[derive(Clone, Debug, PartialEq)]
pub enum PreparedStatementState {
    Parsed(Query),
    Described { query: UntypedQuery, param_types: Vec<u32> },
    ParsedWithParams { query: Query, param_types: Vec<u32> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedStatement {
    state: PreparedStatementState,
    sql: String,
}

impl PreparedStatement {
    pub fn parsed(sql: String, query: Query) -> PreparedStatement {
        PreparedStatement {
            state: PreparedStatementState::Parsed(query),
            sql,
        }
    }

    pub fn param_types(&self) -> Option<&[u32]> {
        match &self.state {
            PreparedStatementState::Parsed(_) => None,
            PreparedStatementState::Described { param_types, .. } => Some(&param_types),
            PreparedStatementState::ParsedWithParams { param_types, .. } => Some(&param_types),
        }
    }

    pub fn query(&self) -> Option<Query> {
        match &self.state {
            PreparedStatementState::Parsed(query) => Some(query.clone()),
            PreparedStatementState::Described { .. } => None,
            PreparedStatementState::ParsedWithParams { query, .. } => Some(query.clone()),
        }
    }

    pub fn described(&mut self, query: UntypedQuery, param_types: Vec<u32>) {
        self.state = PreparedStatementState::Described { query, param_types };
    }

    pub fn parsed_with_params(&mut self, query: Query, param_types: Vec<u32>) {
        self.state = PreparedStatementState::ParsedWithParams { query, param_types };
    }

    pub fn raw_query(&self) -> &str {
        self.sql.as_str()
    }
}

/// A portal represents the execution state of a running or runnable query.
#[derive(Clone, Debug)]
pub struct Portal {
    /// The name of the prepared statement that is bound to this portal.
    statement_name: String,
    /// The bound SQL statement from the prepared statement.
    stmt: UntypedQuery,
    /// The desired output format for each column in the result set.
    result_formats: Vec<i16>,
    param_values: Vec<ScalarValue>,
    param_types: Vec<SqlTypeFamily>,
}

impl Portal {
    /// Constructs a new `Portal`.
    pub fn new(
        statement_name: String,
        stmt: UntypedQuery,
        result_formats: Vec<i16>,
        param_values: Vec<ScalarValue>,
        param_types: Vec<SqlTypeFamily>,
    ) -> Portal {
        Portal {
            statement_name,
            stmt,
            result_formats,
            param_values,
            param_types,
        }
    }

    /// Returns the bound SQL statement.
    pub fn stmt(&self) -> UntypedQuery {
        self.stmt.clone()
    }

    #[allow(dead_code)]
    pub fn stmt_name(&self) -> &str {
        self.statement_name.as_str()
    }

    pub fn param_values(&self) -> Vec<ScalarValue> {
        self.param_values.clone()
    }

    pub fn param_types(&self) -> &[SqlTypeFamily] {
        &self.param_types
    }
}
