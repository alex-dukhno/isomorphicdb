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

//! Prepared statements maintain in-progress state during a session.
//!
//! In PostgreSQL there are two ways to construct prepared statements:
//!
//! * Via an explicit, user-provided `PREPARE <name> AS <sql>` sql statement.
//! * As part of the PostgreSQL Frontend/Backend protocol, where prepared
//!   statements are created implicitly by client libraries on behalf of users.
//!
//! For Frontend/Backend protocol, there are multiple steps to use prepared
//! statements:
//!
//! 1. Receive a `Parse` message. `Parse` messages included a name for the
//!    prepared statement, in addition to some other possible metadata.
//! 2. After validation, we stash the statement in the `Session` associated with
//!    the current user's session.
//! 3. The client issues a `Bind` message, which provides a name for a portal,
//!    and associates that name with a previously-named prepared statement. This
//!    is the point at which all possible parameters are associated with the
//!    statement, there are no longer any free variables permitted.
//! 4. The client issues an `Execute` message with the name of a portal, causing
//!    that portal to actually start scanning and returning results.

use data_manipulation::UntypedQuery;
use data_scalar::ScalarValue;
use entities::SqlTypeFamily;
use postgres::query_ast::Query;
use postgres::{
    query_response::Description,
    wire_protocol::{PgFormat, PgType},
};

#[derive(Clone, Debug, PartialEq)]
pub enum PreparedStatementState {
    Parsed(Query),
    Described {
        query: UntypedQuery,
        param_types: Vec<PgType>,
    },
    ParsedWithParams {
        query: Query,
        param_types: Vec<PgType>,
    },
    Bound {
        query: Query,
        params: Vec<ScalarValue>,
    },
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

    pub fn param_types(&self) -> Option<&[PgType]> {
        match &self.state {
            PreparedStatementState::Parsed(_) => None,
            PreparedStatementState::Described { param_types, .. } => Some(&param_types),
            PreparedStatementState::ParsedWithParams { param_types, .. } => Some(&param_types),
            PreparedStatementState::Bound { .. } => None,
        }
    }

    pub fn query(&self) -> Option<Query> {
        match &self.state {
            PreparedStatementState::Parsed(query) => Some(query.clone()),
            PreparedStatementState::Described { .. } => None,
            PreparedStatementState::ParsedWithParams { query, .. } => Some(query.clone()),
            PreparedStatementState::Bound { query, .. } => Some(query.clone()),
        }
    }

    pub fn described(&mut self, query: UntypedQuery, param_types: Vec<PgType>) {
        self.state = PreparedStatementState::Described { query, param_types };
    }

    pub fn parsed_with_params(&mut self, query: Query, param_types: Vec<PgType>) {
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
    result_formats: Vec<PgFormat>,
    param_values: Vec<ScalarValue>,
    param_types: Vec<SqlTypeFamily>,
}

impl Portal {
    /// Constructs a new `Portal`.
    pub fn new(
        statement_name: String,
        stmt: UntypedQuery,
        result_formats: Vec<PgFormat>,
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
