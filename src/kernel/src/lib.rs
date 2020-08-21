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

use std::fmt::{self, Display, Formatter};

pub type SystemResult<T> = std::result::Result<T, SystemError>;

#[derive(Debug)]
pub struct SystemError {
    message: String,
    backtrace: backtrace::Backtrace,
    kind: SystemErrorKind,
}

impl SystemError {
    pub fn bug_in_sql_engine(operation: Operation, object: Object) -> SystemError {
        fn context_message(operation: Operation, object: Object) -> String {
            match object {
                Object::Schema(schema_name) => format!(
                    "It does not check '{}' existence of SCHEMA before {} one",
                    schema_name, operation
                ),
                Object::Table(schema_name, table_name) => format!(
                    "It does not check '{}.{}' existence of TABLE before {} one",
                    schema_name, table_name, operation
                ),
            }
        }

        SystemError {
            message: format!(
                "This is most possibly a 🐛[BUG] in sql engine.\n{}",
                context_message(operation, object)
            ),
            backtrace: backtrace::Backtrace::new(),
            kind: SystemErrorKind::SqlEngineBug,
        }
    }

    pub fn runtime_check_failure(message: String) -> SystemError {
        SystemError {
            message,
            backtrace: backtrace::Backtrace::new(),
            kind: SystemErrorKind::RuntimeCheckFailure,
        }
    }

    pub fn unrecoverable(message: String) -> SystemError {
        SystemError {
            message,
            backtrace: backtrace::Backtrace::new(),
            kind: SystemErrorKind::Unrecoverable,
        }
    }

    pub fn io(io_error: std::io::Error) -> SystemError {
        SystemError {
            message: "IO error has happened".to_owned(),
            backtrace: backtrace::Backtrace::new(),
            kind: SystemErrorKind::Io(io_error),
        }
    }
}

impl PartialEq for SystemError {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message && self.kind == other.kind
    }
}

#[derive(Debug)]
pub enum SystemErrorKind {
    Unrecoverable,
    RuntimeCheckFailure,
    SqlEngineBug,
    Io(std::io::Error),
}

pub enum Operation {
    Create,
    Drop,
    Access,
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Create => write!(f, "creating"),
            Operation::Drop => write!(f, "dropping"),
            Operation::Access => write!(f, "accessing"),
        }
    }
}

pub enum Object<'o> {
    Table(&'o str, &'o str),
    Schema(&'o str),
}

impl PartialEq for SystemErrorKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SystemErrorKind::Io(_), SystemErrorKind::Io(_)) => true,
            (SystemErrorKind::Unrecoverable, SystemErrorKind::Unrecoverable) => true,
            (SystemErrorKind::RuntimeCheckFailure, SystemErrorKind::RuntimeCheckFailure) => true,
            _ => false,
        }
    }
}
