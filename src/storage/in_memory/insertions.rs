use num_traits::Zero;

use super::*;
use crate::types::Int;
use num_bigint::BigInt;

fn zero() -> Type {
    Type::Int(Int::new(BigInt::zero()))
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
