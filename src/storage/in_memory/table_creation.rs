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

#[test]
fn create_table_with_the_same_name() {
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
fn create_table_with_many_columns() {
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
fn fails_to_create_table_with_duplicate_column_names() {
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
fn create_table_with_not_null_constraint() {
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
