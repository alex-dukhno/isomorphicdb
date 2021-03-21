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

use crate::session::statement::{Portal, PreparedStatement};
use std::collections::HashMap;

/// Module contains functionality to hold data about `PreparedStatement`
pub mod statement;

/// A `Session` holds SQL state that is attached to a session.
#[derive(Clone, Debug)]
pub struct Session {
    /// A map from statement names to parameterized statements
    prepared_statements: HashMap<String, PreparedStatement>,
    /// A map from statement names to bound statements
    portals: HashMap<String, Portal>,
}

impl Default for Session {
    fn default() -> Session {
        Session {
            prepared_statements: HashMap::default(),
            portals: HashMap::default(),
        }
    }
}

impl Session {
    /// get `PreparedStatement` by its name
    pub fn get_prepared_statement(&mut self, name: &str) -> Option<&mut PreparedStatement> {
        self.prepared_statements.get_mut(name)
    }

    /// save `PreparedStatement` associated with a name
    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement) {
        self.prepared_statements.insert(name, statement);
    }

    /// get `Portal` by its name
    pub fn get_portal(&self, name: &str) -> Option<&Portal> {
        self.portals.get(name)
    }

    /// save `Portal` associated with a name
    pub fn set_portal(&mut self, portal_name: String, portal: Portal) {
        self.portals.insert(portal_name, portal);
    }

    pub fn remove_portal(&mut self, portal_name: &str) {
        self.portals.remove(portal_name);
    }
}
