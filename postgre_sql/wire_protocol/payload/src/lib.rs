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

// type oids
pub const BOOL: u32 = 16;
pub const CHAR: u32 = 18;
pub const VARCHAR: u32 = 1043;
pub const INT: u32 = 23;
pub const BIGINT: u32 = 20;
pub const SMALLINT: u32 = 21;

pub const COMMAND_COMPLETE: u8 = b'C';
pub const DATA_ROW: u8 = b'D';
pub const ERROR_RESPONSE: u8 = b'E';
pub const SEVERITY: u8 = b'S';
pub const CODE: u8 = b'C';
pub const MESSAGE: u8 = b'M';
pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
pub const NOTICE_RESPONSE: u8 = b'N';
pub const AUTHENTICATION: u8 = b'R';
pub const BACKEND_KEY_DATA: u8 = b'K';
pub const PARAMETER_STATUS: u8 = b'S';
pub const ROW_DESCRIPTION: u8 = b'T';
pub const READY_FOR_QUERY: u8 = b'Z';
pub const PARAMETER_DESCRIPTION: u8 = b't';
pub const NO_DATA: u8 = b'n';
pub const PARSE_COMPLETE: u8 = b'1';
pub const BIND_COMPLETE: u8 = b'2';
pub const CLOSE_COMPLETE: u8 = b'3';

pub const QUERY: u8 = b'Q';
pub const BIND: u8 = b'B';
pub const CLOSE: u8 = b'C';
pub const DESCRIBE: u8 = b'D';
pub const EXECUTE: u8 = b'E';
pub const FLUSH: u8 = b'H';
pub const PARSE: u8 = b'P';
pub const SYNC: u8 = b'S';
pub const TERMINATE: u8 = b'X';

#[derive(Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    /// Supports only UTF-8 encoding
    String(String),
}

#[derive(Debug)]
pub enum Inbound {
    Query {
        sql: String,
    },
    Bind {
        portal_name: String,
        statement_name: String,
        query_param_formats: Vec<i16>,
        query_params: Vec<Option<Vec<u8>>>,
        result_value_formats: Vec<i16>,
    },
    ClosePortal {
        name: String,
    },
    CloseStatement {
        name: String,
    },
    DescribePortal {
        name: String,
    },
    DescribeStatement {
        name: String,
    },
    Execute {
        portal_name: String,
        max_rows: i32,
    },
    Flush,
    Parse {
        statement_name: String,
        sql: String,
        param_types: Vec<u32>,
    },
    Sync,
    Terminate,
}

#[derive(Debug, PartialEq)]
pub enum Outbound {
    SchemaCreated,
    SchemaDropped,
    TableCreated,
    TableDropped,
    IndexCreated,
    VariableSet,
    TransactionBegin,
    RecordsInserted(usize),
    RowDescription(Vec<(String, u32)>),
    DataRow(Vec<String>),
    RecordsSelected(usize),
    RecordsUpdated(usize),
    RecordsDeleted(usize),
    StatementPrepared,
    StatementDeallocated,
    StatementParameters(Vec<u32>),
    StatementDescription(Vec<(String, u32)>),
    ReadyForQuery,
    ParseComplete,
    BindComplete,
    Error(String, String, String),
    TransactionCommit,
}

impl From<Outbound> for Vec<u8> {
    fn from(event: Outbound) -> Vec<u8> {
        fn command_complete(command: &str) -> Vec<u8> {
            let mut command_buff = Vec::new();
            command_buff.extend_from_slice(&[COMMAND_COMPLETE]);
            command_buff.extend_from_slice(&(4 + command.len() as i32 + 1).to_be_bytes());
            command_buff.extend_from_slice(command.as_bytes());
            command_buff.extend_from_slice(&[0]);
            command_buff
        }

        /// Returns PostgreSQL type length
        pub fn type_len(oid: u32) -> i16 {
            match oid {
                BOOL => 1,
                CHAR => 1,
                BIGINT => 8,
                SMALLINT => 2,
                INT => 4,
                VARCHAR => -1,
                _ => unimplemented!(),
            }
        }

        match event {
            Outbound::SchemaCreated => command_complete("CREATE SCHEMA"),
            Outbound::SchemaDropped => command_complete("DROP SCHEMA"),
            Outbound::TableCreated => command_complete("CREATE TABLE"),
            Outbound::TableDropped => command_complete("DROP TABLE"),
            Outbound::IndexCreated => command_complete("CREATE INDEX"),
            Outbound::VariableSet => command_complete("SET"),
            Outbound::TransactionBegin => command_complete("BEGIN"),
            Outbound::TransactionCommit => command_complete("COMMIT"),
            Outbound::RecordsInserted(records) => command_complete(format!("INSERT 0 {}", records).as_str()),
            Outbound::RowDescription(description) => {
                let mut buff = Vec::new();
                let len = description.len();
                for (name, oid) in description {
                    buff.extend_from_slice(name.as_bytes());
                    buff.extend_from_slice(&[0]); // end of c string
                    buff.extend_from_slice(&(0i32).to_be_bytes()); // table id
                    buff.extend_from_slice(&(0i16).to_be_bytes()); // column id
                    buff.extend_from_slice(&oid.to_be_bytes());
                    buff.extend_from_slice(&(type_len(oid)).to_be_bytes());
                    buff.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
                    buff.extend_from_slice(&0i16.to_be_bytes());
                }
                let mut len_buff = Vec::new();
                len_buff.extend_from_slice(&[ROW_DESCRIPTION]);
                len_buff.extend_from_slice(&(6 + buff.len() as i32).to_be_bytes());
                len_buff.extend_from_slice(&(len as i16).to_be_bytes());
                len_buff.extend_from_slice(&buff);
                len_buff
            }
            Outbound::DataRow(row) => {
                let mut row_buff = Vec::new();
                for field in row.iter() {
                    row_buff.extend_from_slice(&(field.len() as i32).to_be_bytes());
                    row_buff.extend_from_slice(field.as_str().as_bytes());
                }
                let mut len_buff = Vec::new();
                len_buff.extend_from_slice(&[DATA_ROW]);
                len_buff.extend_from_slice(&(6 + row_buff.len() as i32).to_be_bytes());
                len_buff.extend_from_slice(&(row.len() as i16).to_be_bytes());
                len_buff.extend_from_slice(&row_buff);
                len_buff
            }
            Outbound::RecordsSelected(records) => command_complete(format!("SELECT {}", records).as_str()),
            Outbound::RecordsUpdated(records) => command_complete(format!("UPDATE {}", records).as_str()),
            Outbound::RecordsDeleted(records) => command_complete(format!("DELETE {}", records).as_str()),
            Outbound::StatementPrepared => command_complete("PREPARE"),
            Outbound::StatementDeallocated => command_complete("DEALLOCATE"),
            Outbound::StatementParameters(param_types) => {
                let mut type_id_buff = Vec::new();
                for oid in param_types.iter() {
                    type_id_buff.extend_from_slice(&oid.to_be_bytes());
                }
                let mut buff = Vec::new();
                buff.extend_from_slice(&[PARAMETER_DESCRIPTION]);
                buff.extend_from_slice(&(6 + type_id_buff.len() as i32).to_be_bytes());
                buff.extend_from_slice(&(param_types.len() as i16).to_be_bytes());
                buff.extend_from_slice(&type_id_buff);
                buff
            }
            Outbound::StatementDescription(description) => {
                if description.is_empty() {
                    vec![NO_DATA, 0, 0, 0, 4]
                } else {
                    let mut buff = Vec::new();
                    let len = description.len();
                    for (name, oid) in description {
                        buff.extend_from_slice(name.as_bytes());
                        buff.extend_from_slice(&[0]); // end of c string
                        buff.extend_from_slice(&(0i32).to_be_bytes()); // table id
                        buff.extend_from_slice(&(0i16).to_be_bytes()); // column id
                        buff.extend_from_slice(&oid.to_be_bytes());
                        buff.extend_from_slice(&(type_len(oid)).to_be_bytes());
                        buff.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
                        buff.extend_from_slice(&0i16.to_be_bytes());
                    }
                    let mut len_buff = Vec::new();
                    len_buff.extend_from_slice(&[ROW_DESCRIPTION]);
                    len_buff.extend_from_slice(&(6 + buff.len() as i32).to_be_bytes());
                    len_buff.extend_from_slice(&(len as i16).to_be_bytes());
                    len_buff.extend_from_slice(&buff);
                    len_buff
                }
            }
            Outbound::ReadyForQuery => vec![READY_FOR_QUERY, 0, 0, 0, 5, EMPTY_QUERY_RESPONSE],
            Outbound::ParseComplete => vec![PARSE_COMPLETE, 0, 0, 0, 4],
            Outbound::BindComplete => vec![BIND_COMPLETE, 0, 0, 0, 4],
            Outbound::Error(severity, code, message) => {
                let mut error_response_buff = Vec::new();
                error_response_buff.extend_from_slice(&[ERROR_RESPONSE]);
                let mut message_buff = Vec::new();
                message_buff.extend_from_slice(&[SEVERITY]);
                message_buff.extend_from_slice(severity.as_bytes());
                message_buff.extend_from_slice(&[0]);
                message_buff.extend_from_slice(&[CODE]);
                message_buff.extend_from_slice(code.as_bytes());
                message_buff.extend_from_slice(&[0]);
                message_buff.extend_from_slice(&[MESSAGE]);
                message_buff.extend_from_slice(message.as_bytes());
                message_buff.extend_from_slice(&[0]);
                error_response_buff.extend_from_slice(&(message_buff.len() as i32 + 4 + 1).to_be_bytes());
                error_response_buff.extend_from_slice(message_buff.as_ref());
                error_response_buff.extend_from_slice(&[0]);
                error_response_buff.to_vec()
            }
        }
    }
}
