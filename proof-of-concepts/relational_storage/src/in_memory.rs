use super::{Constraint, Predicate, SqlError, SqlResult, Storage, StorageType};
use crate::types::Type;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;

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
            let column_names = (&columns)
                .into_iter()
                .map(|(name, _, _)| name)
                .cloned()
                .collect::<Vec<String>>();
            for (column_name, storage_type, constraints) in columns {
                if table_definition.columns.contains_key(&column_name) {
                    return Err(SqlError::DuplicateColumnsName);
                }
                for constraint in &constraints {
                    if let Constraint::Check(ref_column, predicate) = constraint {
                        if !column_names.contains(&ref_column) {
                            return Err(SqlError::NotExistentColumnInConstrain);
                        }
                        if let Predicate::Equal(sql_type) = predicate {
                            if !storage_type.match_with(sql_type) {
                                return Err(SqlError::MismatchedConstraintType);
                            }
                        }
                    }
                }
                table_definition.columns.insert(
                    column_name,
                    ColumnDefinition {
                        sql_type: storage_type,
                        constraints,
                    },
                );
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
    constraints: HashSet<Constraint>,
}

#[cfg(test)]
mod table_creation {
    use super::*;

    fn set(items: Vec<Constraint>) -> HashSet<Constraint> {
        items.into_iter().collect()
    }

    fn table_one() -> String {
        "table_one".to_owned()
    }

    fn table_two() -> String {
        "table_two".to_owned()
    }

    fn column_one() -> String {
        "column_one".to_owned()
    }

    fn column_two() -> String {
        "column_two".to_owned()
    }

    fn not_existed_column() -> String {
        "not_existed_column".to_owned()
    }

    #[cfg(test)]
    mod without_constraints {
        use super::*;

        #[test]
        fn with_the_same_name() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![(column_one(), StorageType::Integer, set(vec![]))],
                ),
                Ok(SqlResult::TableCreated)
            );

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![(column_one(), StorageType::Integer, set(vec![]))],
                ),
                Err(SqlError::TableAlreadyExists)
            );
        }

        #[test]
        fn many_columns() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![
                        (column_one(), StorageType::SmallInt, set(vec![])),
                        (column_two(), StorageType::Integer, set(vec![]))
                    ],
                ),
                Ok(SqlResult::TableCreated)
            )
        }

        #[test]
        fn duplicate_column_names() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![
                        (column_one(), StorageType::SmallInt, set(vec![])),
                        (column_one(), StorageType::Integer, set(vec![]))
                    ],
                ),
                Err(SqlError::DuplicateColumnsName)
            )
        }
    }

    #[cfg(test)]
    mod column_constraints {
        use super::*;
        use num_bigint::BigInt;

        #[test]
        fn not_null() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![(
                        column_one(),
                        StorageType::Integer,
                        set(vec![Constraint::NotNull])
                    )],
                ),
                Ok(SqlResult::TableCreated)
            )
        }

        #[test]
        fn check() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![
                        (
                            column_one(),
                            StorageType::Integer,
                            set(vec![Constraint::Check(
                                column_two(),
                                Predicate::Equal(Type::Int(BigInt::from(100)))
                            )])
                        ),
                        (column_two(), StorageType::Integer, set(vec![]))
                    ],
                ),
                Ok(SqlResult::TableCreated)
            )
        }

        #[test]
        fn check_based_on_not_existed_column() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![(
                        column_one(),
                        StorageType::Integer,
                        set(vec![Constraint::Check(
                            not_existed_column(),
                            Predicate::Equal(Type::Int(BigInt::from(100)))
                        )])
                    )]
                ),
                Err(SqlError::NotExistentColumnInConstrain)
            )
        }

        #[test]
        fn check_based_on_not_matched_type() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_table(
                    &table_one(),
                    vec![(
                        column_one(),
                        StorageType::Integer,
                        set(vec![Constraint::Check(
                            column_one(),
                            Predicate::Equal(Type::VarChar("some string".to_owned()))
                        )])
                    )]
                ),
                Err(SqlError::MismatchedConstraintType)
            )
        }
    }

    #[test]
    fn create_table_with_primary_key_constraint() {
        let mut storage = InMemoryStorage::default();

        assert_eq!(
            storage.create_table(
                &table_one(),
                vec![(
                    column_one(),
                    StorageType::Integer,
                    set(vec![Constraint::PrimaryKey])
                )],
            ),
            Ok(SqlResult::TableCreated)
        )
    }

    #[test]
    fn create_table_with_unique_constraint() {
        let mut storage = InMemoryStorage::default();

        assert_eq!(
            storage.create_table(
                &table_one(),
                vec![(
                    column_one(),
                    StorageType::Integer,
                    set(vec![Constraint::Unique])
                )],
            ),
            Ok(SqlResult::TableCreated)
        )
    }

    #[test]
    fn create_with_foreign_key_to_primary_key() {
        let mut storage = InMemoryStorage::default();

        assert_eq!(
            storage.create_table(
                &table_one(),
                vec![(
                    column_one(),
                    StorageType::Integer,
                    set(vec![Constraint::PrimaryKey])
                )]
            ),
            Ok(SqlResult::TableCreated)
        );

        assert_eq!(
            storage.create_table(
                &table_two(),
                vec![(
                    column_two(),
                    StorageType::Integer,
                    set(vec![Constraint::ForeignKey(table_one(), column_one())])
                )]
            ),
            Ok(SqlResult::TableCreated)
        );
    }
}

#[cfg(test)]
mod selections {
    use super::*;

    use num_bigint::BigInt;

    fn set(items: Vec<Constraint>) -> HashSet<Constraint> {
        items.into_iter().collect()
    }

    #[test]
    fn select_row_from_single_column_table() {
        let mut storage = InMemoryStorage::default();

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
                vec![("column_name".to_owned(), Type::Int(BigInt::from(100)))],
            ),
            Ok(SqlResult::RecordInserted)
        );

        assert_eq!(
            storage.select(
                &"table_name".to_owned(),
                Some(Predicate::Equal(Type::Int(BigInt::from(100)))),
            ),
            Ok(vec![vec![Type::Int(BigInt::from(100))]])
        );
    }

    #[test]
    fn try_to_select_from_single_column_table_by_primary_key_when_value_was_not_inserted() {
        let mut storage = InMemoryStorage::default();

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
                Some(Predicate::Equal(Type::Int(BigInt::from(100)))),
            ),
            Ok(vec![])
        );
    }
}

#[cfg(test)]
mod insertions {
    use super::*;

    use num_bigint::BigInt;
    use num_traits::Zero;

    use super::super::*;

    fn zero() -> Type {
        Type::Int(BigInt::zero())
    }

    #[test]
    fn insert_row_into_nonexisting_table() {
        let mut storage = InMemoryStorage::default();

        assert_eq!(
            storage.insert_into(
                &"table_name".to_owned(),
                vec![("column_name".to_owned(), zero())],
            ),
            Err(SqlError::TableDoesNotExists)
        )
    }

    #[test]
    fn insert_row_into_table() {
        let mut storage = InMemoryStorage::default();

        assert_eq!(
            storage.create_table(
                &"table_name".to_owned(),
                vec![(
                    "column_name".to_owned(),
                    StorageType::Integer,
                    HashSet::new()
                )],
            ),
            Ok(SqlResult::TableCreated)
        );

        assert_eq!(
            storage.insert_into(
                &"table_name".to_owned(),
                vec![("column_name".to_owned(), zero())],
            ),
            Ok(SqlResult::RecordInserted)
        )
    }
}
