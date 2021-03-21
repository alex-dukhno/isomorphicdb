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

use binary::{repr::Datum, Binary};
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use storage_api::*;

const DEFINITION_SCHEMA: &str = "DEFINITION_SCHEMA";
const SCHEMATA_TABLE: &str = "SCHEMATA";
const TABLES_TABLE: &str = "TABLES";
const INDEXES_TABLE: &str = "TABLES";
const COLUMNS_TABLE: &str = "COLUMNS";

pub struct InMemoryDatabase {
    trees: DashMap<String, InMemoryTree>,
}

impl InMemoryDatabase {
    pub fn create() -> InMemoryDatabase {
        let this = InMemoryDatabase {
            trees: DashMap::default(),
        };

        // database bootstrap
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE));
        this.lookup_tree(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE))
            .insert(vec![Binary::pack(&[
                Datum::from_string("IN_MEMORY".to_owned()),
                Datum::from_string("public".to_owned()),
            ])]);
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE));
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE));
        this.create_tree(format!("{}.{}", DEFINITION_SCHEMA, INDEXES_TABLE));

        this
    }

    pub fn lookup_tree<T: Into<String>>(&self, table: T) -> InMemoryTree {
        let table = table.into();
        println!("LOOKUP {:?}", table);
        self.trees.get(&table).unwrap().clone()
    }

    pub fn drop_tree<T: Into<String>>(&self, table: T) {
        self.trees.remove(&table.into());
    }

    pub fn create_tree<T: Into<String>>(&self, table: T) {
        let name = table.into();
        self.trees.insert(name.clone(), InMemoryTree::with_name(name));
    }

    pub fn scan<T: Into<String>>(&self, table: T) -> Cursor {
        let table = table.into();
        self.trees.get(&table).unwrap().select()
    }

    pub fn insert<T: Into<String>>(&self, table: T, key: Key, row: Value) -> Option<Binary> {
        let table = table.into();
        self.trees.get(&table).unwrap().insert_key(key, row)
    }

    pub fn delete<T: Into<String>>(&self, table: T, data: Vec<Key>) -> usize {
        let table = table.into();
        self.trees.get(&table).unwrap().delete(data)
    }
}

#[derive(Default, Debug, Clone)]
pub struct InMemoryTree {
    name: String,
    inner: Arc<InMemoryTableHandleInner>,
    indexes: Arc<DashMap<String, Arc<InMemoryIndex>>>,
}

impl InMemoryTree {
    pub(crate) fn with_name(name: String) -> InMemoryTree {
        InMemoryTree {
            name,
            inner: Arc::new(InMemoryTableHandleInner::default()),
            indexes: Arc::new(DashMap::default()),
        }
    }

    pub(crate) fn index(&self, index: &str) -> Arc<InMemoryIndex> {
        self.indexes.get(index).unwrap().clone()
    }

    pub fn remove(&self, key: &Binary) -> Option<Binary> {
        self.inner.records.write().unwrap().remove(&key)
    }

    pub fn insert_key(&self, key: Binary, row: Binary) -> Option<Binary> {
        self.inner.records.write().unwrap().insert(key, row)
    }

    pub fn select(&self) -> Cursor {
        log::debug!("[SCAN] TABLE NAME {:?}", self.name);
        self.inner
            .records
            .read()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                log::debug!("[SCAN] TABLE RECORD - ({:?}, {:?})", key, value);
                (key.clone(), value.clone())
            })
            .collect::<Cursor>()
    }

    pub fn insert(&self, data: Vec<Value>) -> Vec<Key> {
        let mut rw = self.inner.records.write().unwrap();
        let mut keys = vec![];
        for value in data {
            let record_id = self.inner.record_ids.fetch_add(1, Ordering::SeqCst);
            let key = Binary::pack(&[Datum::from_u64(record_id)]);
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
            .collect::<Vec<Binary>>();
        for key in keys.iter() {
            debug_assert!(matches!(rw.remove(key), Some(_)), "delete operation delete existed key");
            size += 1;
        }
        size
    }

    pub fn next_column_ord(&self) -> u64 {
        self.inner.column_ords.fetch_add(1, Ordering::SeqCst)
    }

    pub fn create_index(&self, index_name: &str, over_column: usize) {
        self.indexes
            .insert(index_name.to_owned(), Arc::new(InMemoryIndex::new(over_column)));
    }
}

#[derive(Default, Debug)]
struct InMemoryTableHandleInner {
    records: RwLock<BTreeMap<Binary, Binary>>,
    record_ids: AtomicU64,
    column_ords: AtomicU64,
}

#[derive(Debug)]
pub struct InMemoryIndex {
    records: RwLock<BTreeMap<Binary, Binary>>,
    column: usize,
}

impl InMemoryIndex {
    pub(crate) fn new(column: usize) -> InMemoryIndex {
        InMemoryIndex {
            records: RwLock::default(),
            column,
        }
    }
}

impl PartialEq for InMemoryTree {
    fn eq(&self, other: &InMemoryTree) -> bool {
        self.name == other.name
    }
}
