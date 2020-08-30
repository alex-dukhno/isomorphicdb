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

pub(crate) mod statement;

use protocol::sql_formats::PostgreSqlFormat;
use sqlparser::ast::Statement;
use statement::{Portal, PreparedStatement};
use std::collections::HashMap;

/// A `Session` holds SQL state that is attached to a session.
#[derive(Clone, Debug)]
pub struct Session {
    /// A map from statement names to parameterized statements
    prepared_statements: HashMap<String, PreparedStatement>,
    /// A map from statement names to bound statements
    portals: HashMap<String, Portal>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    pub fn get_prepared_statement(&self, name: &str) -> Option<&PreparedStatement> {
        self.prepared_statements.get(name)
    }

    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement) {
        self.prepared_statements.insert(name, statement);
    }

    pub fn get_portal(&self, name: &str) -> Option<&Portal> {
        self.portals.get(name)
    }

    pub fn set_portal(
        &mut self,
        portal_name: String,
        statement_name: String,
        stmt: Statement,
        result_formats: Vec<PostgreSqlFormat>,
    ) {
        let new_portal = Portal::new(statement_name, stmt, result_formats);
        self.portals.insert(portal_name, new_portal);
    }
}
