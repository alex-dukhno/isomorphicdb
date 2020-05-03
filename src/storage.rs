use std::collections::{HashMap, BTreeMap};

use num_bigint::BigInt;

use crate::types::{Int, Type};

pub fn in_memory() -> impl Storage {
  InMemoryStorage::default()
}

pub trait Storage {
  fn create_table(&mut self, table_name: &String, columns: Vec<(String, StorageType, Constraint)>) -> Result<(), ()>;

  fn insert_into(&mut self, table_name: &String, values: Vec<(String, Type)>) -> Result<(), ()>;

  fn select(&mut self, table_name: &String, predicate: Where) -> Result<Vec<Vec<Type>>, ()>;
}

pub enum StorageType {
  Int
}

pub enum Constraint {
  PrimaryKey,
  TypeConstraint
}

pub trait Predicate {

}

pub enum Where {
  Equal(Type),
  Between(Type, Type),
  In(Vec<Type>),
  Not(Box<Where>),
  None
}

impl Predicate for Where {

}

#[derive(Default)]
struct InMemoryStorage {
  metadata: HashMap<String, TableDefinition>,
  data: HashMap<String, BTreeMap<Type, Vec<Type>>>,
}

impl Storage for InMemoryStorage {
  fn create_table(&mut self, table_name: &String, columns: Vec<(String, StorageType, Constraint)>) -> Result<(), ()> {
    if self.metadata.contains_key(table_name) {
      Err(())
    } else {
      let mut table_definition = TableDefinition { columns: HashMap::new() };
      for (column_name, sql_type, _constraint) in columns {
        table_definition.columns.insert(column_name, ColumnDefinition{ sql_type });
      }
      self.metadata.insert(table_name.clone(), table_definition);
      self.data.insert(table_name.clone(), BTreeMap::new());
      Ok(())
    }
  }

  fn insert_into(&mut self, table_name: &String, values: Vec<(String, Type)>) -> Result<(), ()> {
    if !self.metadata.contains_key(table_name) {
      Err(())
    } else {
      self.data.get_mut(table_name)
          .map(|data| {
            for (_, value) in values.into_iter() {
              data.insert(
                value.clone(), vec![value]
              );
            }
          }
      );
      Ok(())
    }
  }

  fn select(&mut self, table_name: &String, predicate: Where) -> Result<Vec<Vec<Type>>, ()> {
    self.data.get(table_name)
        .map(|data| {
          match predicate {
            Where::Equal(value) => {
              data.get(&value).cloned().map(|v| vec![v]).unwrap_or(vec![])
            },
            Where::Between(left, right) => unimplemented!(),
            Where::In(values) => unimplemented!(),
            Where::Not(predicate) => unimplemented!(),
            Where::None => data.values().cloned().collect()
          }
        }).ok_or_else(|| ())
  }
}

struct TableDefinition {
  columns: HashMap<String, ColumnDefinition>
}

struct ColumnDefinition {
  sql_type: StorageType
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn create_table() {
    let mut storage = in_memory();

    assert_eq!(
      storage.create_table(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), StorageType::Int, Constraint::TypeConstraint)]
      ),
      Ok(())
    );
  }

  #[test]
  fn create_table_with_the_same_name() {
    let mut storage = in_memory();

    assert_eq!(
      storage.create_table(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), StorageType::Int, Constraint::TypeConstraint)]
      ),
      Ok(())
    );
    assert_eq!(
      storage.create_table(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), StorageType::Int, Constraint::TypeConstraint)]
      ),
      Err(())
    );
  }

  #[test]
  fn create_table_with_primary_key() {
      let mut storage = in_memory();

    assert_eq!(
      storage.create_table(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), StorageType::Int, Constraint::PrimaryKey)]
      ),
      Ok(())
    )
  }

  #[test]
  fn insert_single_row_into_nonexisting_table() {
    let mut storage = in_memory();

    assert_eq!(
      storage.insert_into(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), Type::Int(Int::new(BigInt::from(100))))]
      ),
      Err(())
    )
  }

  #[test]
  fn insert_single_row_into_table() {
    let mut storage = in_memory();

    assert_eq!(
      storage.create_table(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), StorageType::Int, Constraint::TypeConstraint)]
      ),
      Ok(())
    );

    assert_eq!(
      storage.insert_into(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), Type::Int(Int::new(BigInt::from(100))))]
      ),
      Ok(())
    )
  }

  #[test]
  fn select_row_from_single_column_table_by_primary_key() {
    let mut storage = in_memory();

    assert_eq!(
      storage.create_table(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), StorageType::Int, Constraint::PrimaryKey)]
      ),
      Ok(())
    );

    assert_eq!(
      storage.insert_into(
        &"table_name".to_owned(),
        vec![("column_name".to_owned(), Type::Int(Int::new(BigInt::from(100))))]
      ),
      Ok(())
    );

    assert_eq!(
      storage.select(
        &"table_name".to_owned(),
        Where::Equal(Type::Int(Int::new(BigInt::from(100))))
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
        vec![("column_name".to_owned(), StorageType::Int, Constraint::PrimaryKey)]
      ),
      Ok(())
    );

    assert_eq!(
      storage.select(
        &"table_name".to_owned(),
        Where::Equal(Type::Int(Int::new(BigInt::from(100))))
      ),
      Ok(vec![])
    );
  }
}
