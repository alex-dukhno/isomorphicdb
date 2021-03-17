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
use postgres::wire_protocol::{PgFormat, PgType};
use std::collections::HashMap;

/// Module contains functionality to hold data about `PreparedStatement`
pub mod statement;

/// Result of handling incoming bytes from a client
#[derive(Debug, PartialEq)]
pub enum Command {
    /// Client commands to bind a prepared statement to a portal
    Bind {
        /// The destination portal. An empty string selects the unnamed
        /// portal. The portal can later be executed with the `Execute` command.
        portal_name: String,
        /// The source prepared statement. An empty string selects the unnamed
        /// prepared statement.
        statement_name: String,
        /// The formats used to encode the parameters.
        param_formats: Vec<PgFormat>,
        /// The value of each parameter.
        raw_params: Vec<Option<Vec<u8>>>,
        /// The desired formats for the columns in the result set.
        result_formats: Vec<PgFormat>,
    },
    /// Nothing needs to handle on client, just to receive next message
    Continue,
    /// Client commands to describe a prepared statement
    DescribeStatement {
        /// The name of the prepared statement to describe.
        name: String,
    },
    /// Client commands to describe a prepared statement
    DescribePortal {
        /// The name of the prepared statement to describe.
        name: String,
    },
    /// Client commands to execute a portal
    Execute {
        /// The name of the portal to execute.
        portal_name: String,
        /// The maximum number of rows to return before suspending.
        ///
        /// 0 or negative means infinite.
        max_rows: i32,
    },
    /// Client commands to flush the output stream
    Flush,
    /// Client commands to prepare a statement for execution
    Parse {
        /// The name of the prepared statement to create. An empty string
        /// specifies the unnamed prepared statement.
        statement_name: String,
        /// The SQL to parse.
        sql: String,
        /// The number of specified parameter data types can be less than the
        /// number of parameters specified in the query.
        param_types: Vec<Option<PgType>>,
    },
    /// Client commands to execute a `Query`
    Query {
        /// The SQL to execute.
        sql: String,
    },
    /// Client commands to terminate current connection
    Terminate,
}

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
