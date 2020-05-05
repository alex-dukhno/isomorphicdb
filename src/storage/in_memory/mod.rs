use crate::storage::{Constraint, Predicate, SqlError, SqlResult, Storage, StorageType};
use crate::types::Type;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;

#[cfg(test)]
mod insertions;
#[cfg(test)]
mod selections;
#[cfg(test)]
mod table_creation;

#[derive(Default)]
pub struct InMemoryStorage {
    next_id: u32,
    tables: HashMap<String, u32>,
    metadata: HashMap<u32, TableDefinition>,
    data: HashMap<u32, BTreeMap<Type, Vec<Type>>>,
}

impl Storage for InMemoryStorage {
    fn create_table(
        &mut self,
        table_name: &String,
        columns: Vec<(String, StorageType, HashSet<Constraint>)>,
    ) -> Result<SqlResult, SqlError> {
        if self.tables.contains_key(table_name) {
            Err(SqlError::TableAlreadyExists)
        } else {
            let mut table_definition = TableDefinition {
                columns: HashMap::new(),
            };
            for (column_name, sql_type, _constraint) in columns {
                if table_definition.columns.contains_key(&column_name) {
                    return Err(SqlError::DuplicateColumnsName);
                }
                table_definition
                    .columns
                    .insert(column_name, ColumnDefinition { sql_type });
            }
            let id = self.next_id;
            self.next_id += 1;
            self.tables.insert(table_name.clone(), id);
            self.metadata.insert(id, table_definition);
            self.data.insert(id, BTreeMap::new());
            Ok(SqlResult::TableCreated)
        }
    }

    fn insert_into(
        &mut self,
        table_name: &String,
        values: Vec<(String, Type)>,
    ) -> Result<SqlResult, SqlError> {
        if !self.tables.contains_key(table_name) {
            Err(SqlError::TableDoesNotExists)
        } else {
            self.read_write(table_name).map(|data| {
                for (_, value) in values.into_iter() {
                    data.insert(value.clone(), vec![value]);
                }
            });
            Ok(SqlResult::RecordInserted)
        }
    }

    fn select(
        &mut self,
        table_name: &String,
        predicate: Option<Predicate>,
    ) -> Result<Vec<Vec<Type>>, ()> {
        self.read_only(table_name)
            .map(|data| match predicate {
                Some(Predicate::Equal(value)) => {
                    data.get(&value).cloned().map(|v| vec![v]).unwrap_or(vec![])
                }
                Some(Predicate::Between(low, high)) => data
                    .range(low..=high)
                    .map(|(_key, value)| value)
                    .cloned()
                    .collect(),
                Some(Predicate::In(values)) => data
                    .values()
                    .filter(|value| values.contains(&value[0]))
                    .cloned()
                    .collect(),
                Some(Predicate::Not(predicate)) => {
                    if let Predicate::Between(low, high) = predicate.deref() {
                        data.range(..low)
                            .chain(data.range(high..).skip(1))
                            .map(|(_key, value)| value)
                            .cloned()
                            .collect()
                    } else if let Predicate::In(values) = predicate.deref() {
                        data.values()
                            .filter(|value| !values.contains(&value[0]))
                            .cloned()
                            .collect()
                    } else {
                        vec![]
                    }
                }
                None => data.values().cloned().collect(),
            })
            .ok_or_else(|| ())
    }
}

impl InMemoryStorage {
    fn read_only(&self, table_name: &String) -> Option<&BTreeMap<Type, Vec<Type>>> {
        match self.tables.get(table_name) {
            Some(id) => self.data.get(id),
            None => None,
        }
    }

    fn read_write(&mut self, table_name: &String) -> Option<&mut BTreeMap<Type, Vec<Type>>> {
        match self.tables.get(table_name) {
            Some(id) => self.data.get_mut(id),
            None => None,
        }
    }
}

struct TableDefinition {
    columns: HashMap<String, ColumnDefinition>,
}

struct ColumnDefinition {
    sql_type: StorageType,
}
