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

pub use binary::*;
#[cfg(feature = "in_memory")]
pub use in_memory::*;
#[cfg(feature = "persistent")]
pub use persistent::*;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
pub use storage_api::*;

#[derive(Clone)]
pub struct Database {
    inner: DatabaseInner,
}

impl Database {
    #[cfg(feature = "in_memory")]
    pub fn in_memory(_path: &str) -> Database {
        Database {
            inner: DatabaseInner::InMemory(Arc::new(Mutex::new(InMemoryDatabase::create()))),
        }
    }

    #[cfg(feature = "persistent")]
    pub fn persistent(path: &str) -> Database {
        Database {
            inner: DatabaseInner::Persistent(Arc::new(Mutex::new(PersistentDatabase::new(path)))),
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
        match &self.inner {
            #[cfg(feature = "in_memory")]
            DatabaseInner::InMemory(db) => TransactionalDatabase::from(db.lock().unwrap()),
            #[cfg(feature = "persistent")]
            DatabaseInner::Persistent(db) => TransactionalDatabase::from(db.lock().unwrap()),
        }
    }
}

#[derive(Clone)]
enum DatabaseInner {
    #[cfg(feature = "in_memory")]
    InMemory(Arc<Mutex<InMemoryDatabase>>),
    #[cfg(feature = "persistent")]
    Persistent(Arc<Mutex<PersistentDatabase>>),
}

#[derive(Clone)]
pub struct TransactionalDatabase<'t> {
    inner: Rc<TransactionalDatabaseInner<'t>>,
}

impl<'t> TransactionalDatabase<'t> {
    pub fn table<T: Into<String>>(&self, full_table_name: T) -> Table {
        match &*self.inner {
            #[cfg(feature = "in_memory")]
            TransactionalDatabaseInner::InMemory(database) => Table::from(database.lookup_tree(full_table_name)),
            #[cfg(feature = "persistent")]
            TransactionalDatabaseInner::Persistent(database) => Table::from(database.lookup_tree(full_table_name)),
        }
    }

    pub fn drop_tree<T: Into<String>>(&self, full_table_name: T) {
        match &*self.inner {
            #[cfg(feature = "in_memory")]
            TransactionalDatabaseInner::InMemory(database) => database.drop_tree(full_table_name),
            #[cfg(feature = "persistent")]
            TransactionalDatabaseInner::Persistent(database) => database.drop_tree(full_table_name),
        }
    }

    pub fn create_tree<T: Into<String>>(&self, full_table_name: T) {
        match &*self.inner {
            #[cfg(feature = "in_memory")]
            TransactionalDatabaseInner::InMemory(database) => database.create_tree(full_table_name),
            #[cfg(feature = "persistent")]
            TransactionalDatabaseInner::Persistent(database) => database.create_tree(full_table_name),
        }
    }
}

#[cfg(feature = "in_memory")]
impl<'t> From<MutexGuard<'t, InMemoryDatabase>> for TransactionalDatabase<'t> {
    fn from(guard: MutexGuard<'t, InMemoryDatabase>) -> TransactionalDatabase {
        TransactionalDatabase {
            inner: Rc::new(TransactionalDatabaseInner::InMemory(guard)),
        }
    }
}

#[cfg(feature = "persistent")]
impl<'t> From<MutexGuard<'t, PersistentDatabase>> for TransactionalDatabase<'t> {
    fn from(guard: MutexGuard<'t, PersistentDatabase>) -> TransactionalDatabase {
        TransactionalDatabase {
            inner: Rc::new(TransactionalDatabaseInner::Persistent(guard)),
        }
    }
}

enum TransactionalDatabaseInner<'t> {
    #[cfg(feature = "in_memory")]
    InMemory(MutexGuard<'t, InMemoryDatabase>),
    #[cfg(feature = "persistent")]
    Persistent(MutexGuard<'t, PersistentDatabase>),
}

#[derive(Debug)]
pub struct Table {
    inner: TableInner,
}

#[cfg(feature = "in_memory")]
impl From<InMemoryTree> for Table {
    fn from(table: InMemoryTree) -> Table {
        Table {
            inner: TableInner::InMemory(table),
        }
    }
}

#[cfg(feature = "persistent")]
impl From<PersistentTable> for Table {
    fn from(table: PersistentTable) -> Table {
        Table {
            inner: TableInner::Persistent(table),
        }
    }
}

#[derive(Debug)]
enum TableInner {
    #[cfg(feature = "in_memory")]
    InMemory(InMemoryTree),
    #[cfg(feature = "persistent")]
    Persistent(PersistentTable),
}

impl Table {
    pub fn write(&self, row: Value) -> Key {
        match &self.inner {
            #[cfg(feature = "in_memory")]
            TableInner::InMemory(table) => table.insert(vec![row]).remove(0),
            #[cfg(feature = "persistent")]
            TableInner::Persistent(table) => table.insert(vec![row]).remove(0),
        }
    }

    pub fn write_key(&self, key: Binary, row: Option<Binary>) {
        match &self.inner {
            #[cfg(feature = "in_memory")]
            TableInner::InMemory(table) => match row {
                None => {
                    let result = table.remove(&key);
                    debug_assert!(matches!(result, Some(_)), "nothing were found for {:?} key", key);
                }
                Some(row) => {
                    let _result = table.insert_key(key.clone(), row);
                    // debug_assert!(
                    //     matches!(result, None),
                    //     "old record {:?} was found for {:?} key",
                    //     result,
                    //     key
                    // );
                }
            },
            #[cfg(feature = "persistent")]
            TableInner::Persistent(table) => match row {
                None => {
                    let result = table.remove(&key);
                    debug_assert!(matches!(result, Some(_)), "nothing were found for {:?} key", key);
                }
                Some(row) => {
                    let _result = table.insert_key(key.clone(), row);
                    // debug_assert!(
                    //     matches!(result, None),
                    //     "old record {:?} was found for {:?} key",
                    //     result,
                    //     key
                    // );
                }
            },
        }
    }

    pub fn scan(&self) -> Cursor {
        match &self.inner {
            #[cfg(feature = "in_memory")]
            TableInner::InMemory(table) => table.select(),
            #[cfg(feature = "persistent")]
            TableInner::Persistent(table) => table.select(),
        }
    }
}
