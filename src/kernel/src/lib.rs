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

pub type SystemResult<T> = std::result::Result<T, SystemError>;

#[derive(Debug)]
pub struct SystemError {
    message: String,
    backtrace: backtrace::Backtrace,
    cause: Option<backtrace::Backtrace>,
    kind: SystemErrorKind,
}

impl SystemError {
    pub fn unrecoverable(message: String) -> SystemError {
        Self {
            message,
            backtrace: backtrace::Backtrace::new(),
            cause: None,
            kind: SystemErrorKind::Unrecoverable,
        }
    }

    pub fn unrecoverable_with_cause(message: String, cause: backtrace::Backtrace) -> SystemError {
        Self {
            message,
            backtrace: backtrace::Backtrace::new(),
            cause: Some(cause),
            kind: SystemErrorKind::Unrecoverable,
        }
    }

    pub fn io(io_error: std::io::Error) -> SystemError {
        Self {
            message: "IO error has happened".to_owned(),
            backtrace: backtrace::Backtrace::new(),
            cause: None,
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
    Io(std::io::Error),
}

impl PartialEq for SystemErrorKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SystemErrorKind::Io(_), SystemErrorKind::Io(_)) => true,
            (SystemErrorKind::Unrecoverable, SystemErrorKind::Unrecoverable) => true,
            _ => false,
        }
    }
}
