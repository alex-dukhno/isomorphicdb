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
