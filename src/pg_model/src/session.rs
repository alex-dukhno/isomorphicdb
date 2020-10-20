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

use crate::{
    pg_types::PostgreSqlFormat,
    statement::{Portal, PreparedStatement},
};
use std::collections::HashMap;

/// A `Session` holds SQL state that is attached to a session.
#[derive(Clone, Debug)]
pub struct Session<S> {
    /// A map from statement names to parameterized statements
    prepared_statements: HashMap<String, PreparedStatement<S>>,
    /// A map from statement names to bound statements
    portals: HashMap<String, Portal<S>>,
}

impl<S> Default for Session<S> {
    fn default() -> Session<S> {
        Session {
            prepared_statements: HashMap::default(),
            portals: HashMap::default(),
        }
    }
}

impl<S> Session<S> {
    /// get `PreparedStatement` by its name
    pub fn get_prepared_statement(&self, name: &str) -> Option<&PreparedStatement<S>> {
        self.prepared_statements.get(name)
    }

    /// save `PreparedStatement` associated with a name
    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement<S>) {
        self.prepared_statements.insert(name, statement);
    }

    /// remove `PreparedStatement` by its name
    pub fn remove_prepared_statement(&mut self, name: &str) {
        self.prepared_statements.remove(name);
    }

    /// get `Portal` by its name
    pub fn get_portal(&self, name: &str) -> Option<&Portal<S>> {
        self.portals.get(name)
    }

    /// save `Portal` associated with a name
    pub fn set_portal(
        &mut self,
        portal_name: String,
        statement_name: String,
        stmt: S,
        result_formats: Vec<PostgreSqlFormat>,
    ) {
        let new_portal = Portal::new(statement_name, stmt, result_formats);
        self.portals.insert(portal_name, new_portal);
    }
}
