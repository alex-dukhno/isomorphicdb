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

use crate::pg_types::{PostgreSqlFormat, PostgreSqlType};
use protocol::{Error, ProtocolResult};
use rand::Rng;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
};

/// Module contains functionality to represent PostgreSQL types
pub mod pg_types;
/// Module contains functionality to represent query result
pub mod results;
/// Module contains functionality to represent server side client session
pub mod session;
/// Module contains functionality to hold data about `PreparedStatement`
pub mod statement;

/// Connection ID
pub type ConnId = i32;
/// Connection secret key
pub type ConnSecretKey = i32;

/// Manages allocation of Connection IDs and secret keys.
pub struct ConnSupervisor {
    next_id: ConnId,
    max_id: ConnId,
    free_ids: VecDeque<ConnId>,
    current_mapping: HashMap<ConnId, ConnSecretKey>,
}

impl ConnSupervisor {
    /// Creates a new Connection Supervisor.
    pub fn new(min_id: ConnId, max_id: ConnId) -> Self {
        Self {
            next_id: min_id,
            max_id,
            free_ids: VecDeque::new(),
            current_mapping: HashMap::new(),
        }
    }

    /// Allocates a new Connection ID and secret key.
    pub fn alloc(&mut self) -> ProtocolResult<(ConnId, ConnSecretKey)> {
        let conn_id = self.generate_conn_id()?;
        let secret_key = rand::thread_rng().gen();
        self.current_mapping.insert(conn_id, secret_key);
        Ok((conn_id, secret_key))
    }

    /// Releases a Connection ID back to the pool.
    pub fn free(&mut self, conn_id: ConnId) {
        if self.current_mapping.remove(&conn_id).is_some() {
            self.free_ids.push_back(conn_id);
        }
    }

    /// Validates whether the secret key matches the specified Connection ID.
    pub fn verify(&self, conn_id: ConnId, secret_key: ConnSecretKey) -> bool {
        match self.current_mapping.get(&conn_id) {
            Some(s) => *s == secret_key,
            None => false,
        }
    }

    pub fn generate_conn_id(&mut self) -> ProtocolResult<ConnId> {
        match self.free_ids.pop_front() {
            Some(id) => Ok(id),
            None => {
                let id = self.next_id;
                if id > self.max_id {
                    return Err(Error::ConnectionIdExhausted);
                }

                self.next_id += 1;
                Ok(id)
            }
        }
    }
}

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
        param_formats: Vec<PostgreSqlFormat>,
        /// The value of each parameter.
        raw_params: Vec<Option<Vec<u8>>>,
        /// The desired formats for the columns in the result set.
        result_formats: Vec<PostgreSqlFormat>,
    },
    /// Nothing needs to handle on client, just to receive next message
    Continue,
    /// Client commands to describe a prepared statement
    DescribeStatement {
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
        param_types: Vec<PostgreSqlType>,
    },
    /// Client commands to execute a `Query`
    Query {
        /// The SQL to execute.
        sql: String,
    },
    /// Client commands to terminate current connection
    Terminate,
}

/// Accepting or Rejecting SSL connection
pub enum Encryption {
    /// Accept SSL connection from client
    AcceptSsl,
    /// Reject SSL connection from client
    RejectSsl,
}

impl Into<&'_ [u8]> for Encryption {
    fn into(self) -> &'static [u8] {
        match self {
            Self::AcceptSsl => &[b'S'],
            Self::RejectSsl => &[b'N'],
        }
    }
}

/// Struct to configure possible secure providers for client-server communication
/// PostgreSQL Wire Protocol supports `ssl`/`tls` and `gss` encryption
pub struct ProtocolConfiguration {
    ssl_conf: Option<(PathBuf, String)>,
}

#[allow(dead_code)]
impl ProtocolConfiguration {
    /// Creates configuration that support neither `ssl` nor `gss` encryption
    pub fn none() -> Self {
        Self { ssl_conf: None }
    }

    /// Creates configuration that support only `ssl`
    pub fn with_ssl(cert: PathBuf, password: String) -> Self {
        Self {
            ssl_conf: Some((cert, password)),
        }
    }

    /// returns `true` if support `ssl` connection
    pub fn ssl_support(&self) -> bool {
        self.ssl_conf.is_some()
    }

    /// cert file and its password
    pub fn ssl_config(&self) -> Option<&(PathBuf, String)> {
        self.ssl_conf.as_ref()
    }

    /// returns `true` if support `gss` encrypted connection
    pub fn gssenc_support(&self) -> bool {
        false
    }
}
