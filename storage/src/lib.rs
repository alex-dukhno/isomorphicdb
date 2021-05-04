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

use binary::BinaryValue;
use dashmap::DashMap;
use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Formatter},
    iter::FromIterator,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, RwLock,
    },
};

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
    inner: Arc<Mutex<DatabaseInner>>,
}

impl Database {
    pub fn new(_path: &str) -> Database {
        Database {
            inner: Arc::new(Mutex::new(DatabaseInner::create())),
        }
    }

    pub fn old_transaction<F, R>(&self, mut f: F) -> TransactionResult<R>
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

    pub fn transaction(&self) -> Transaction {
        Transaction {
            guard: Rc::new(self.inner.lock().unwrap()),
        }
    }
}

#[derive(Clone)]
pub struct Transaction<'t> {
    guard: Rc<MutexGuard<'t, DatabaseInner>>,
}

impl<'t> Transaction<'t> {
    pub fn lookup_table_ref<T: Into<String>>(&self, full_table_name: T) -> TableRef {
        TableRef::from(self.guard.lookup_tree(full_table_name))
    }

    pub fn drop_tree<T: Into<String>>(&self, full_table_name: T) {
        self.guard.drop_tree(full_table_name)
    }

    pub fn create_tree<T: Into<String>>(&self, full_table_name: T) {
        self.guard.create_tree(full_table_name)
    }
}

#[derive(Clone)]
pub struct TransactionalDatabase<'t> {
    inner: Rc<MutexGuard<'t, DatabaseInner>>,
}

impl<'t> TransactionalDatabase<'t> {
    pub fn lookup_table_ref<T: Into<String>>(&self, full_table_name: T) -> TableRef {
        TableRef::from(self.inner.lookup_tree(full_table_name))
    }

    pub fn drop_tree<T: Into<String>>(&self, full_table_name: T) {
        self.inner.drop_tree(full_table_name)
    }

    pub fn create_tree<T: Into<String>>(&self, full_table_name: T) {
        self.inner.create_tree(full_table_name)
    }
}

impl<'t> From<MutexGuard<'t, DatabaseInner>> for TransactionalDatabase<'t> {
    fn from(guard: MutexGuard<'t, DatabaseInner>) -> TransactionalDatabase {
        TransactionalDatabase { inner: Rc::new(guard) }
    }
}

#[derive(Debug, Clone)]
pub struct TableRef {
    inner: TableInner,
}

impl From<TableInner> for TableRef {
    fn from(table: TableInner) -> TableRef {
        TableRef { inner: table }
    }
}

impl TableRef {
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

const DEFINITION_SCHEMA: &str = "DEFINITION_SCHEMA";
const SCHEMATA_TABLE: &str = "SCHEMATA";
const TABLES_TABLE: &str = "TABLES";
const INDEXES_TABLE: &str = "TABLES";
const COLUMNS_TABLE: &str = "COLUMNS";

pub struct DatabaseInner {
    trees: DashMap<String, TableInner>,
}

impl DatabaseInner {
    pub fn create() -> DatabaseInner {
        let this = DatabaseInner {
            trees: DashMap::default(),
        };

        // database bootstrap
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE));
        this.lookup_tree(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE))
            .insert(vec![vec![BinaryValue::from("IN_MEMORY"), BinaryValue::from("public")]]);
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE));
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE));
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, INDEXES_TABLE));

        this
    }

    pub fn lookup_tree<T: Into<String>>(&self, table: T) -> TableInner {
        let table = table.into();
        self.trees.get(&table).unwrap().clone()
    }

    pub fn drop_tree<T: Into<String>>(&self, table: T) {
        self.trees.remove(&table.into());
    }

    pub fn create_tree<T: Into<String>>(&self, table: T) {
        let name = table.into();
        self.trees.insert(name.clone(), TableInner::with_name(name));
    }
}

#[derive(Default, Debug, Clone)]
pub struct TableInner {
    name: String,
    inner: Arc<InMemoryTableHandleInner>,
}

impl TableInner {
    pub(crate) fn with_name(name: String) -> TableInner {
        TableInner {
            name,
            inner: Arc::new(InMemoryTableHandleInner::default()),
        }
    }

    pub fn remove(&self, key: &[BinaryValue]) -> Option<Vec<BinaryValue>> {
        self.inner.records.write().unwrap().remove(key)
    }

    pub fn insert_key(&self, key: Vec<BinaryValue>, row: Vec<BinaryValue>) -> Option<Vec<BinaryValue>> {
        self.inner.records.write().unwrap().insert(key, row)
    }

    pub fn select(&self) -> Cursor {
        self.inner
            .records
            .read()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Cursor>()
    }

    pub fn insert(&self, data: Vec<Value>) -> Vec<Key> {
        let mut rw = self.inner.records.write().unwrap();
        let mut keys = vec![];
        for value in data {
            let record_id = self.inner.record_ids.fetch_add(1, Ordering::SeqCst);
            let key = vec![BinaryValue::from_u64(record_id)];
            debug_assert!(
                matches!(rw.insert(key.clone(), value), None),
                "insert operation should insert nonexistent key"
            );
            keys.push(key);
        }

        keys
    }

    pub fn update(&self, data: Vec<(Key, Value)>) -> usize {
        let len = data.len();
        let mut rw = self.inner.records.write().unwrap();
        for (key, value) in data {
            debug_assert!(
                matches!(rw.insert(key, value), Some(_)),
                "update operation should change already existed key"
            );
        }
        len
    }

    pub fn delete(&self, data: Vec<Key>) -> usize {
        let mut rw = self.inner.records.write().unwrap();
        let mut size = 0;
        let keys = rw
            .iter()
            .filter(|(key, _value)| data.contains(key))
            .map(|(key, _value)| key.clone())
            .collect::<Vec<Vec<BinaryValue>>>();
        for key in keys.iter() {
            debug_assert!(matches!(rw.remove(key), Some(_)), "delete operation delete existed key");
            size += 1;
        }
        size
    }
}

#[derive(Default, Debug)]
struct InMemoryTableHandleInner {
    records: RwLock<BTreeMap<Vec<BinaryValue>, Vec<BinaryValue>>>,
    record_ids: AtomicU64,
    column_ords: AtomicU64,
}

#[derive(Debug)]
pub struct InMemoryIndex {
    records: RwLock<BTreeMap<Vec<BinaryValue>, Vec<BinaryValue>>>,
    column: usize,
}

impl InMemoryIndex {
    #[allow(dead_code)]
    pub(crate) fn new(column: usize) -> InMemoryIndex {
        InMemoryIndex {
            records: RwLock::default(),
            column,
        }
    }
}

impl PartialEq for TableInner {
    fn eq(&self, other: &TableInner) -> bool {
        self.name == other.name
    }
}
