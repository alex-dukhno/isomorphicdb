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
use sled::{Db as SledDb, Tree as SledTree};
use std::{
    convert::TryInto,
    sync::atomic::{AtomicU64, Ordering},
};
use storage_api::{Cursor, Key, Storage, Tree, Value};

const DEFINITION_SCHEMA: &str = "DEFINITION_SCHEMA";
const SCHEMATA_TABLE: &str = "SCHEMATA";
const TABLES_TABLE: &str = "TABLES";
const INDEXES_TABLE: &str = "TABLES";
const COLUMNS_TABLE: &str = "COLUMNS";

pub struct PersistentDatabase {
    sled_db: SledDb,
}

impl PersistentDatabase {
    pub fn new(path: &str) -> PersistentDatabase {
        let sled_db = sled::open(path).unwrap();
        let new_database = !sled_db.was_recovered();

        let this = PersistentDatabase { sled_db };

        if new_database {
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
        }
        this
    }
}

impl Storage for PersistentDatabase {
    type Tree = PersistentTable;

    fn lookup_tree<T: Into<String>>(&self, table: T) -> Self::Tree {
        PersistentTable::from(self.sled_db.open_tree(table.into()).unwrap())
    }

    fn drop_tree<T: Into<String>>(&self, table: T) {
        self.sled_db.drop_tree(table.into()).unwrap();
    }

    fn create_tree<T: Into<String>>(&self, table: T) {
        self.sled_db.open_tree(table.into()).unwrap();
    }
}

#[derive(Debug)]
pub struct PersistentTable {
    sled_tree: SledTree,
    key_index: AtomicU64,
}

impl From<SledTree> for PersistentTable {
    fn from(sled_tree: SledTree) -> PersistentTable {
        let key_index = sled_tree.last();

        let key_index = key_index
            .map(|option| {
                option
                    .map(|(key, _value)| u64::from_be_bytes((&key[1..9]).try_into().unwrap()))
                    .unwrap_or_default()
                    + 1
            })
            .ok()
            .unwrap_or_default();
        log::debug!(
            "{:?} KEY INDEX INIT WITH {:?}",
            String::from_utf8(sled_tree.name().to_vec()).unwrap(),
            key_index
        );
        PersistentTable {
            sled_tree,
            key_index: AtomicU64::from(key_index),
        }
    }
}

impl Tree for PersistentTable {
    fn remove(&self, key: &Binary) -> Option<Binary> {
        self.sled_tree
            .remove(&key)
            .unwrap()
            .map(|v| Binary::with_data(v.to_vec()))
    }

    fn insert_key(&self, key: Binary, row: Binary) -> Option<Binary> {
        self.sled_tree
            .insert(key.as_ref(), row.as_ref())
            .unwrap()
            .map(|v| Binary::with_data(v.to_vec()))
    }

    fn select(&self) -> Cursor {
        self.sled_tree
            .iter()
            .map(Result::unwrap)
            .map(|(key, value)| (Binary::with_data(key.to_vec()), Binary::with_data(value.to_vec())))
            .collect()
    }

    fn insert(&self, data: Vec<Value>) -> Vec<Key> {
        let mut keys = vec![];
        for datum in data {
            let key_index = self.key_index.fetch_add(1, Ordering::SeqCst);
            log::debug!(
                "{:?} NEXT KEY ID {:?}",
                String::from_utf8(self.sled_tree.name().to_vec()).unwrap(),
                key_index
            );
            let key = Binary::pack(&[Datum::from_u64(key_index)]);
            self.sled_tree.insert(key.clone().as_ref(), datum.as_ref()).unwrap();
            keys.push(key);
        }
        keys
    }

    fn update(&self, data: Vec<(Key, Value)>) -> usize {
        let len = data.len();
        for (key, value) in data {
            debug_assert!(
                matches!(self.sled_tree.insert(key.as_ref(), value.as_ref()), Ok(Some(_))),
                "update operation should change already existed key"
            );
        }
        len
    }

    fn delete(&self, data: Vec<Key>) -> usize {
        let mut size = 0;
        let keys = self
            .select()
            .filter(|(key, _value)| data.contains(key))
            .map(|(key, _value)| key)
            .collect::<Vec<Binary>>();
        for key in keys.iter() {
            debug_assert!(
                matches!(self.sled_tree.remove(key), Ok(Some(_))),
                "delete operation delete existed key"
            );
            size += 1;
        }
        size
    }

    fn next_column_ord(&self) -> u64 {
        unimplemented!()
    }

    fn create_index(&self, _index_name: &str, _over_column: usize) {
        unimplemented!()
    }
}
