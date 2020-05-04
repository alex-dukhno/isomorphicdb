use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;

use num_bigint::BigInt;

use crate::types::{Int, Type};

pub fn in_memory() -> impl Storage {
    InMemoryStorage::default()
}

pub trait Storage {
    fn create_table(
        &mut self,
        table_name: &String,
        columns: Vec<(String, StorageType, HashSet<Constraint>)>,
    ) -> Result<SqlResult, SqlError>;

    fn insert_into(&mut self, table_name: &String, values: Vec<(String, Type)>) -> Result<(), ()>;

    fn select(
        &mut self,
        table_name: &String,
        predicate: Option<Predicate>,
    ) -> Result<Vec<Vec<Type>>, ()>;
}

pub enum StorageType {
    // i16
    SmallInt,
    // i32
    Integer,
    // i64
    BigInt,
    // 131072, 16383
    Decimal,
    // 131072, 16383
    Numeric,
    // f32
    Real,
    // f64
    DoublePrecision,
    // 1 .. i16::MAX
    SmallSerial,
    // 1 .. i32::MAX
    Serial,
    // 1 .. i64::MAXs
    BigSerial,
}

#[derive(Hash, PartialEq, Eq)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
}

pub enum Predicate {
    Equal(Type),
    Between(Type, Type),
    In(Vec<Type>),
    Not(Box<Predicate>),
}

#[derive(Debug, PartialEq)]
pub enum SqlResult {
    TableCreated,
}

#[derive(Debug, PartialEq)]
pub enum SqlError {
    TableAlreadyExists,
    DuplicateColumnsName,
}

#[derive(Default)]
struct InMemoryStorage {
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

    fn insert_into(&mut self, table_name: &String, values: Vec<(String, Type)>) -> Result<(), ()> {
        if !self.tables.contains_key(table_name) {
            Err(())
        } else {
            self.read_write(table_name).map(|data| {
                for (_, value) in values.into_iter() {
                    data.insert(value.clone(), vec![value]);
                }
            });
            Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: Vec<Constraint>) -> HashSet<Constraint> {
        items.into_iter().collect()
    }

    #[cfg(test)]
    mod tables {
        use super::*;

        #[test]
        fn create_table_with_the_same_name() {
            let mut storage = in_memory();

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![("column_name".to_owned(), StorageType::Integer, set(vec![]))],
                ),
                Ok(SqlResult::TableCreated)
            );

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![("column_name".to_owned(), StorageType::Integer, set(vec![]))],
                ),
                Err(SqlError::TableAlreadyExists)
            );
        }

        #[test]
        fn create_table_with_many_columns() {
            let mut storage = in_memory();

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![
                        ("column_one".to_owned(), StorageType::SmallInt, set(vec![])),
                        ("column_two".to_owned(), StorageType::Integer, set(vec![]))
                    ],
                ),
                Ok(SqlResult::TableCreated)
            )
        }

        #[test]
        fn fails_to_create_table_with_duplicate_column_names() {
            let mut storage = in_memory();

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![
                        ("column_one".to_owned(), StorageType::SmallInt, set(vec![])),
                        ("column_one".to_owned(), StorageType::Integer, set(vec![]))
                    ],
                ),
                Err(SqlError::DuplicateColumnsName)
            )
        }

        #[test]
        fn create_table_with_primary_key_constraint() {
            let mut storage = in_memory();

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![(
                        "column_name".to_owned(),
                        StorageType::Integer,
                        set(vec![Constraint::PrimaryKey])
                    )],
                ),
                Ok(SqlResult::TableCreated)
            )
        }

        #[test]
        fn create_table_with_not_null_constraint() {
            let mut storage = in_memory();

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![(
                        "column_name".to_owned(),
                        StorageType::Integer,
                        set(vec![Constraint::NotNull])
                    )],
                ),
                Ok(SqlResult::TableCreated)
            )
        }

        #[test]
        fn create_table_with_unique_constraint() {
            let mut storage = in_memory();

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![(
                        "column_name".to_owned(),
                        StorageType::Integer,
                        set(vec![Constraint::Unique])
                    )],
                ),
                Ok(SqlResult::TableCreated)
            )
        }
    }

    #[cfg(test)]
    mod insertions {
        use num_traits::Zero;

        use super::*;

        fn zero() -> Type {
            Type::Int(Int::new(BigInt::zero()))
        }

        #[test]
        fn insert_row_into_nonexisting_table() {
            let mut storage = in_memory();

            assert_eq!(
                storage.insert_into(
                    &"table_name".to_owned(),
                    vec![("column_name".to_owned(), zero())],
                ),
                Err(())
            )
        }

        #[test]
        fn insert_row_into_table() {
            let mut storage = in_memory();

            assert_eq!(
                storage.create_table(
                    &"table_name".to_owned(),
                    vec![("column_name".to_owned(), StorageType::Integer, set(vec![]))],
                ),
                Ok(SqlResult::TableCreated)
            );

            assert_eq!(
                storage.insert_into(
                    &"table_name".to_owned(),
                    vec![("column_name".to_owned(), zero())],
                ),
                Ok(())
            )
        }
    }

    #[test]
    fn select_row_from_single_column_table() {
        let mut storage = in_memory();

        assert_eq!(
            storage.create_table(
                &"table_name".to_owned(),
                vec![(
                    "column_name".to_owned(),
                    StorageType::Integer,
                    set(vec![Constraint::PrimaryKey])
                )],
            ),
            Ok(SqlResult::TableCreated)
        );

        assert_eq!(
            storage.insert_into(
                &"table_name".to_owned(),
                vec![(
                    "column_name".to_owned(),
                    Type::Int(Int::new(BigInt::from(100)))
                )],
            ),
            Ok(())
        );

        assert_eq!(
            storage.select(
                &"table_name".to_owned(),
                Some(Predicate::Equal(Type::Int(Int::new(BigInt::from(100))))),
            ),
            Ok(vec![vec![Type::Int(Int::new(BigInt::from(100)))]])
        );
    }

    #[test]
    fn try_to_select_from_single_column_table_by_primary_key_when_value_was_not_inserted() {
        let mut storage = in_memory();

        assert_eq!(
            storage.create_table(
                &"table_name".to_owned(),
                vec![(
                    "column_name".to_owned(),
                    StorageType::Integer,
                    set(vec![Constraint::PrimaryKey])
                )],
            ),
            Ok(SqlResult::TableCreated)
        );

        assert_eq!(
            storage.select(
                &"table_name".to_owned(),
                Some(Predicate::Equal(Type::Int(Int::new(BigInt::from(100))))),
            ),
            Ok(vec![])
        );
    }
}
