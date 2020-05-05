use super::*;
use crate::types::Int;
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
            vec![(
                "column_name".to_owned(),
                Type::Int(Int::new(BigInt::from(100)))
            )],
        ),
        Ok(SqlResult::RecordInserted)
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
            Some(Predicate::Equal(Type::Int(Int::new(BigInt::from(100))))),
        ),
        Ok(vec![])
    );
}
