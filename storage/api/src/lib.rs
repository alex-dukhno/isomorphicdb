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

use binary::Binary;
use std::fmt::{self, Debug, Formatter};
use std::iter::FromIterator;

pub type Key = Binary;
pub type Value = Binary;
pub type TransactionResult<R> = Result<R, TransactionError>;
pub type ConflictableTransactionResult<R> = Result<R, ConflictableTransactionError>;

#[derive(Debug, PartialEq)]
pub enum TransactionError {
    Abort,
    Storage,
}

#[derive(Debug, PartialEq)]
pub enum ConflictableTransactionError {
    Abort,
    Storage,
    Conflict,
}

pub struct Cursor {
    source: Box<dyn Iterator<Item = (Binary, Binary)>>,
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Data Cursor")
    }
}

impl FromIterator<(Binary, Binary)> for Cursor {
    fn from_iter<T: IntoIterator<Item = (Binary, Binary)>>(iter: T) -> Self {
        Self {
            source: Box::new(iter.into_iter().collect::<Vec<(Binary, Binary)>>().into_iter()),
        }
    }
}

impl Iterator for Cursor {
    type Item = (Binary, Binary);

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next()
    }
}
