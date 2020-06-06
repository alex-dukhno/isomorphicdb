use std::borrow::ToOwned;
use std::collections::HashMap;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub type Projection = (Vec<String>, Vec<Vec<String>>);

pub trait Storage {
    fn create_schema(&mut self, schema_name: String) -> Result<()>;

    fn drop_schema(&mut self, schema_name: String) -> Result<()>;

    fn create_table(
        &mut self,
        schema_name: String,
        table_name: String,
        column_names: Vec<String>,
    ) -> Result<()>;

    fn drop_table(&mut self, schema_name: String, table_name: String) -> Result<()>;

    fn table_columns(&mut self, schema_name: String, table_name: String) -> Result<Vec<String>>;

    fn insert_into(
        &mut self,
        schema_name: String,
        table_name: String,
        values: Vec<Vec<String>>,
    ) -> Result<()>;

    fn select_all_from(
        &mut self,
        schema_name: String,
        table_name: String,
        columns: Vec<String>,
    ) -> Result<Projection>;

    fn update_all(
        &mut self,
        schema_name: String,
        table_name: String,
        value: String,
    ) -> Result<usize>;

    fn delete_all_from(&mut self, schema_name: String, table_name: String) -> Result<usize>;
}

#[derive(Default)]
pub struct SledStorage {
    key_id_generator: usize,
    schemas: HashMap<String, sled::Db>,
}

impl Storage for SledStorage {
    #[allow(clippy::match_wild_err_arm, clippy::map_entry)]
    fn create_schema(&mut self, schema_name: String) -> Result<()> {
        if self.schemas.contains_key(&schema_name) {
            Err(Error::SchemaAlreadyExists(schema_name))
        } else {
            match sled::Config::default().temporary(true).open() {
                Ok(schema) => {
                    self.schemas.insert(schema_name, schema);
                    Ok(())
                }
                Err(_) => unimplemented!(),
            }
        }
    }

    fn drop_schema(&mut self, schema_name: String) -> Result<()> {
        match self.schemas.remove(&schema_name) {
            Some(schema) => {
                drop(schema);
                Ok(())
            }
            None => Err(Error::SchemaDoesNotExist(schema_name)),
        }
    }

    fn create_table(
        &mut self,
        schema_name: String,
        table_name: String,
        column_names: Vec<String>,
    ) -> Result<()> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            Err(Error::TableAlreadyExists(
                schema_name + "." + table_name.as_str(),
            ))
        } else {
            if let Some(schema) = self.schemas.get_mut(&schema_name) {
                schema
                    .insert::<sled::IVec, sled::IVec>(
                        table_name.as_str().into(),
                        column_names.join("|").as_str().into(),
                    )
                    .unwrap();
                schema.open_tree(table_name).unwrap();
            }
            Ok(())
        }
    }

    fn table_columns(&mut self, schema_name: String, table_name: String) -> Result<Vec<String>> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            Ok(self
                .schemas
                .get(&schema_name)
                .map(|schema| {
                    schema
                        .iter()
                        .values()
                        .map(sled::Result::unwrap)
                        .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
                        .collect::<String>()
                        .split('|')
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap())
        } else {
            Err(Error::TableDoesNotExist(
                schema_name + "." + table_name.as_str(),
            ))
        }
    }

    fn drop_table(&mut self, schema_name: String, table_name: String) -> Result<()> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            self.schemas
                .get(&schema_name)
                .map(|schema| schema.drop_tree(table_name.as_bytes()));
            Ok(())
        } else {
            Err(Error::TableDoesNotExist(
                schema_name + "." + table_name.as_str(),
            ))
        }
    }

    fn insert_into(
        &mut self,
        schema_name: String,
        table_name: String,
        values: Vec<Vec<String>>,
    ) -> Result<()> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            let mut next_key_id = self.key_id_generator;
            self.schemas.get_mut(&schema_name).map(|schema| {
                schema.open_tree(&table_name).ok().map(|table| {
                    for record in values {
                        table
                            .insert::<[u8; 8], sled::IVec>(
                                next_key_id.to_be_bytes(),
                                record.join("|").as_str().into(),
                            )
                            .unwrap();
                        next_key_id += 1;
                    }
                })
            });
            self.key_id_generator = next_key_id;
            Ok(())
        } else {
            Err(Error::TableDoesNotExist(
                schema_name + "." + table_name.as_str(),
            ))
        }
    }

    fn select_all_from(
        &mut self,
        schema_name: String,
        table_name: String,
        columns: Vec<String>,
    ) -> Result<Projection> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            let all_columns = self.table_columns(schema_name.clone(), table_name.clone())?;
            let mut column_indexes = vec![];
            for (i, column) in columns.iter().enumerate() {
                for (index, name) in all_columns.iter().enumerate() {
                    if name == column {
                        column_indexes.push((index, i));
                    }
                }
            }
            Ok((
                columns,
                self.schemas
                    .get(&schema_name)
                    .and_then(|schema| {
                        schema.open_tree(&table_name).ok().map(|table| {
                            table
                                .iter()
                                .values()
                                .map(sled::Result::unwrap)
                                .map(|bytes| {
                                    let all_values = String::from_utf8(bytes.to_vec())
                                        .unwrap()
                                        .split('|')
                                        .map(ToOwned::to_owned)
                                        .collect::<Vec<String>>();
                                    let mut values = vec![];
                                    for (origin, ord) in &column_indexes {
                                        for (index, value) in all_values.iter().enumerate() {
                                            if index == *origin {
                                                values.push((ord, value.clone()))
                                            }
                                        }
                                    }
                                    values.iter().map(|(_, value)| value.clone()).collect()
                                })
                                .collect()
                        })
                    })
                    .unwrap(),
            ))
        } else {
            Err(Error::TableDoesNotExist(
                schema_name + "." + table_name.as_str(),
            ))
        }
    }

    fn update_all(
        &mut self,
        schema_name: String,
        table_name: String,
        value: String,
    ) -> Result<usize> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            let mut records_updated = 0;
            if let Some(schema) = self.schemas.get_mut(&schema_name) {
                let table = schema.open_tree(table_name).unwrap();
                for key in table.iter().keys() {
                    table
                        .fetch_and_update(key.unwrap(), |_old| Some(value.clone().into_bytes()))
                        .unwrap();
                    records_updated += 1;
                }
            }
            Ok(records_updated)
        } else {
            Err(Error::TableDoesNotExist(
                schema_name + "." + table_name.as_str(),
            ))
        }
    }

    fn delete_all_from(&mut self, schema_name: String, table_name: String) -> Result<usize> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            let mut deleted_records = 0;
            if let Some(schema) = self.schemas.get_mut(&schema_name) {
                let table = schema.open_tree(table_name).unwrap();
                for key in table.iter().keys() {
                    table.remove(key.unwrap()).unwrap();
                    deleted_records += 1;
                }
            };
            Ok(deleted_records)
        } else {
            Err(Error::TableDoesNotExist(
                schema_name + "." + table_name.as_str(),
            ))
        }
    }
}

#[derive(Debug, PartialEq, Error)]
pub enum Error {
    #[error("schema {0} already exists")]
    SchemaAlreadyExists(String),
    #[error("table {0} already exists")]
    TableAlreadyExists(String),
    #[error("schema {0} does not exist")]
    SchemaDoesNotExist(String),
    #[error("table {0} does not exist")]
    TableDoesNotExist(String),
    #[error("not supported operation")]
    NotSupportedOperation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_schemas_with_different_names() {
        let mut storage = SledStorage::default();

        assert_eq!(storage.create_schema("schema_1".to_owned()), Ok(()));
        assert_eq!(storage.create_schema("schema_2".to_owned()), Ok(()));
    }

    #[test]
    fn create_schema_with_existing_name() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(
            storage.create_schema("schema_name".to_owned()),
            Err(Error::SchemaAlreadyExists("schema_name".to_owned()))
        );

        Ok(())
    }

    #[test]
    fn drop_schema() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(storage.drop_schema("schema_name".to_owned()), Ok(()));
        assert_eq!(storage.create_schema("schema_name".to_owned()), Ok(()));

        Ok(())
    }

    #[test]
    fn drop_schema_that_was_not_created() {
        let mut storage = SledStorage::default();

        assert_eq!(
            storage.drop_schema("does_not_exists".to_owned()),
            Err(Error::SchemaDoesNotExist("does_not_exists".to_owned()))
        );
    }

    #[test]
    fn drop_schema_drops_tables_in_it() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table(
            "schema_name".to_owned(),
            "table_name_1".to_owned(),
            vec!["column_test".to_owned()],
        )?;
        storage.create_table(
            "schema_name".to_owned(),
            "table_name_2".to_owned(),
            vec!["column_test".to_owned()],
        )?;

        assert_eq!(storage.drop_schema("schema_name".to_owned()), Ok(()));
        assert_eq!(storage.create_schema("schema_name".to_owned()), Ok(()));
        assert_eq!(
            storage.create_table(
                "schema_name".to_owned(),
                "table_name_1".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Ok(())
        );
        assert_eq!(
            storage.create_table(
                "schema_name".to_owned(),
                "table_name_2".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Ok(())
        );

        Ok(())
    }

    #[test]
    fn create_tables_with_different_names() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(
            storage.create_table(
                "schema_name".to_owned(),
                "table_name_1".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Ok(())
        );
        assert_eq!(
            storage.create_table(
                "schema_name".to_owned(),
                "table_name_2".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Ok(())
        );

        Ok(())
    }

    #[test]
    fn create_table_with_the_same_name() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_test"],
        )?;

        assert_eq!(
            storage.create_table(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Err(Error::TableAlreadyExists(
                "schema_name.table_name".to_owned()
            ))
        );
        Ok(())
    }

    #[test]
    fn create_table_with_the_same_name_in_different_schemas() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name_1".to_owned())?;
        storage.create_schema("schema_name_2".to_owned())?;
        assert_eq!(
            storage.create_table(
                "schema_name_1".to_owned(),
                "table_name".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Ok(())
        );
        assert_eq!(
            storage.create_table(
                "schema_name_2".to_owned(),
                "table_name".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn drop_table() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_test"],
        )?;
        assert_eq!(
            storage.drop_table("schema_name".to_owned(), "table_name".to_owned()),
            Ok(())
        );
        assert_eq!(
            storage.create_table(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                vec!["column_test".to_owned()]
            ),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn drop_not_created_table() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;
        assert_eq!(
            storage.drop_table("schema_name".to_owned(), "not_existed_table".to_owned()),
            Err(Error::TableDoesNotExist(
                "schema_name.not_existed_table".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn insert_row_into_table() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_test"],
        )?;
        assert_eq!(
            storage.insert_into(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                vec![vec!["123".to_owned()]],
            ),
            Ok(())
        );

        let table_columns =
            storage.table_columns("schema_name".to_owned(), "table_name".to_owned())?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                table_columns
            ),
            Ok((vec!["column_test".to_owned()], vec![vec!["123".to_owned()]]))
        );

        Ok(())
    }

    #[test]
    fn insert_many_rows_into_table() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_test"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["123".to_owned()]],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["456".to_owned()]],
        )?;

        let table_columns =
            storage.table_columns("schema_name".to_owned(), "table_name".to_owned())?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                table_columns
            ),
            Ok((
                vec!["column_test".to_owned()],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            ))
        );

        Ok(())
    }

    #[test]
    fn insert_into_non_existent_table() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;
        assert_eq!(
            storage.insert_into(
                "schema_name".to_owned(),
                "not_existed".to_owned(),
                vec![vec!["123".to_owned()]],
            ),
            Err(Error::TableDoesNotExist(
                "schema_name.not_existed".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn select_from_table_that_does_not_exist() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;
        assert_eq!(
            storage.table_columns("schema_name".to_owned(), "not_existed".to_owned()),
            Err(Error::TableDoesNotExist(
                "schema_name.not_existed".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn update_all_records() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_test"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["123".to_owned()]],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["456".to_owned()]],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["789".to_owned()]],
        )?;

        assert_eq!(
            storage.update_all(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                "567".to_owned()
            ),
            Ok(3)
        );

        let table_columns =
            storage.table_columns("schema_name".to_owned(), "table_name".to_owned())?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                table_columns
            ),
            Ok((
                vec!["column_test".to_owned()],
                vec![
                    vec!["567".to_owned()],
                    vec!["567".to_owned()],
                    vec!["567".to_owned()]
                ]
            ))
        );

        Ok(())
    }

    #[test]
    fn update_not_existed_table() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;
        assert_eq!(
            storage.update_all(
                "schema_name".to_owned(),
                "not_existed".to_owned(),
                "123".to_owned()
            ),
            Err(Error::TableDoesNotExist(
                "schema_name.not_existed".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn delete_all_from_table() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_test"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["123".to_owned()]],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["456".to_owned()]],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["789".to_owned()]],
        )?;

        assert_eq!(
            storage.delete_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Ok(3)
        );

        let table_columns =
            storage.table_columns("schema_name".to_owned(), "table_name".to_owned())?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                table_columns
            ),
            Ok((vec!["column_test".to_owned()], vec![]))
        );

        Ok(())
    }

    #[test]
    fn delete_all_from_not_existed_table() -> Result<()> {
        let mut storage = SledStorage::default();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(
            storage.delete_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Err(Error::TableDoesNotExist(
                "schema_name.table_name".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn select_all_from_table_with_many_columns() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_1", "column_2", "column_3"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]],
        )?;

        let table_columns =
            storage.table_columns("schema_name".to_owned(), "table_name".to_owned())?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                table_columns
            ),
            Ok((
                vec![
                    "column_1".to_owned(),
                    "column_2".to_owned(),
                    "column_3".to_owned()
                ],
                vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]]
            ))
        );

        Ok(())
    }

    #[test]
    fn insert_multiple_rows() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_1", "column_2", "column_3"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
            ],
        )?;

        let table_columns =
            storage.table_columns("schema_name".to_owned(), "table_name".to_owned())?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                table_columns
            ),
            Ok((
                vec![
                    "column_1".to_owned(),
                    "column_2".to_owned(),
                    "column_3".to_owned()
                ],
                vec![
                    vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
                ],
            ))
        );

        Ok(())
    }

    #[test]
    fn select_first_and_last_columns_from_table_with_multiple_columns() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["first", "middle", "last"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
            ],
        )?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                vec!["first".to_owned(), "last".to_owned()]
            ),
            Ok((
                vec!["first".to_owned(), "last".to_owned(),],
                vec![
                    vec!["1".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "9".to_owned()],
                ],
            ))
        );

        Ok(())
    }

    #[test]
    fn select_all_columns_reordered_from_table_with_multiple_columns() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["first", "middle", "last"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
            ],
        )?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                vec!["last".to_owned(), "first".to_owned(), "middle".to_owned()]
            ),
            Ok((
                vec!["last".to_owned(), "first".to_owned(), "middle".to_owned()],
                vec![
                    vec!["3".to_owned(), "1".to_owned(), "2".to_owned()],
                    vec!["6".to_owned(), "4".to_owned(), "5".to_owned()],
                    vec!["9".to_owned(), "7".to_owned(), "8".to_owned()],
                ],
            ))
        );

        Ok(())
    }

    #[test]
    fn select_with_column_name_duplication() -> Result<()> {
        let mut storage = SledStorage::default();

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["first", "middle", "last"],
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            vec![
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
            ],
        )?;

        assert_eq!(
            storage.select_all_from(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                vec![
                    "last".to_owned(),
                    "middle".to_owned(),
                    "first".to_owned(),
                    "last".to_owned(),
                    "middle".to_owned()
                ]
            ),
            Ok((
                vec![
                    "last".to_owned(),
                    "middle".to_owned(),
                    "first".to_owned(),
                    "last".to_owned(),
                    "middle".to_owned()
                ],
                vec![
                    vec![
                        "3".to_owned(),
                        "2".to_owned(),
                        "1".to_owned(),
                        "3".to_owned(),
                        "2".to_owned()
                    ],
                    vec![
                        "6".to_owned(),
                        "5".to_owned(),
                        "4".to_owned(),
                        "6".to_owned(),
                        "5".to_owned()
                    ],
                    vec![
                        "9".to_owned(),
                        "8".to_owned(),
                        "7".to_owned(),
                        "9".to_owned(),
                        "8".to_owned()
                    ],
                ],
            ))
        );

        Ok(())
    }

    fn create_table(
        storage: &mut SledStorage,
        schema_name: &str,
        table_name: &str,
        column_names: Vec<&str>,
    ) -> Result<()> {
        storage.create_schema(schema_name.to_owned())?;
        storage.create_table(
            schema_name.to_owned(),
            table_name.to_owned(),
            column_names
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>(),
        )
    }
}
