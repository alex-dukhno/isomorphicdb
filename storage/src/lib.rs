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

use crate::in_memory::{InMemoryDatabase, InMemoryTree};
use binary::BinaryValue;
use std::{
    fmt::{self, Debug, Formatter},
    iter::FromIterator,
    rc::Rc,
    sync::{Arc, Mutex, MutexGuard},
};

mod in_memory;
mod pages;

pub type Key = Vec<BinaryValue>;
pub type Value = Vec<BinaryValue>;
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
    source: Box<dyn Iterator<Item = (Vec<BinaryValue>, Vec<BinaryValue>)>>,
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Data Cursor")
    }
}

impl FromIterator<(Vec<BinaryValue>, Vec<BinaryValue>)> for Cursor {
    fn from_iter<T: IntoIterator<Item = (Vec<BinaryValue>, Vec<BinaryValue>)>>(iter: T) -> Cursor {
        Cursor {
            source: Box::new(
                iter.into_iter()
                    .collect::<Vec<(Vec<BinaryValue>, Vec<BinaryValue>)>>()
                    .into_iter(),
            ),
        }
    }
}

impl Iterator for Cursor {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next()
    }
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<InMemoryDatabase>>,
}

impl Database {
    pub fn new(_path: &str) -> Database {
        Database {
            inner: Arc::new(Mutex::new(InMemoryDatabase::create())),
        }
    }

    pub fn transaction<F, R>(&self, mut f: F) -> TransactionResult<R>
    where
        // TODO: make it Fn otherwise it won't work with sled
        F: FnMut(TransactionalDatabase) -> ConflictableTransactionResult<R>,
    {
        loop {
            match f(self.transactional()) {
                Ok(result) => return Ok(result),
                Err(ConflictableTransactionError::Storage) => return Err(TransactionError::Storage),
                Err(ConflictableTransactionError::Abort) => return Err(TransactionError::Abort),
                Err(ConflictableTransactionError::Conflict) => {}
            }
        }
    }

    fn transactional(&self) -> TransactionalDatabase {
        TransactionalDatabase::from(self.inner.lock().unwrap())
    }
}

#[derive(Clone)]
pub struct TransactionalDatabase<'t> {
    inner: Rc<MutexGuard<'t, InMemoryDatabase>>,
}

impl<'t> TransactionalDatabase<'t> {
    pub fn table<T: Into<String>>(&self, full_table_name: T) -> Table {
        Table::from(self.inner.lookup_tree(full_table_name))
    }

    pub fn drop_tree<T: Into<String>>(&self, full_table_name: T) {
        self.inner.drop_tree(full_table_name)
    }

    pub fn create_tree<T: Into<String>>(&self, full_table_name: T) {
        self.inner.create_tree(full_table_name)
    }
}

impl<'t> From<MutexGuard<'t, InMemoryDatabase>> for TransactionalDatabase<'t> {
    fn from(guard: MutexGuard<'t, InMemoryDatabase>) -> TransactionalDatabase {
        TransactionalDatabase { inner: Rc::new(guard) }
    }
}

#[derive(Debug)]
pub struct Table {
    inner: InMemoryTree,
}

impl From<InMemoryTree> for Table {
    fn from(table: InMemoryTree) -> Table {
        Table { inner: table }
    }
}

impl Table {
    pub fn write(&self, row: Value) -> Key {
        self.inner.insert(vec![row]).remove(0)
    }

    pub fn write_key(&self, key: Vec<BinaryValue>, row: Option<Vec<BinaryValue>>) {
        match row {
            None => {
                let result = self.inner.remove(&key);
                debug_assert!(matches!(result, Some(_)), "nothing were found for {:?} key", key);
            }
            Some(row) => {
                let _result = self.inner.insert_key(key, row);
            }
        }
    }

    pub fn scan(&self) -> Cursor {
        self.inner.select()
    }
}
