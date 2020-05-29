use sled;
use std::collections::HashMap;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub trait Storage {
    fn create_schema(&mut self, schema_name: String) -> Result<()>;

    fn drop_schema(&mut self, schema_name: String) -> Result<()>;

    fn create_table(&mut self, schema_name: String, table_name: String) -> Result<()>;

    fn drop_table(&mut self, schema_name: String, table_name: String) -> Result<()>;

    fn insert_into(&mut self, schema_name: String, table_name: String, value: String)
        -> Result<()>;

    fn select_all_from(&mut self, schema_name: String, table_name: String) -> Result<Vec<String>>;

    fn update_all(
        &mut self,
        schema_name: String,
        table_name: String,
        value: String,
    ) -> Result<usize>;

    fn delete_all_from(&mut self, schema_name: String, table_name: String) -> Result<usize>;
}

pub struct SledStorage {
    key_id_generator: usize,
    schemas: HashMap<String, sled::Db>,
}

impl SledStorage {
    pub fn new() -> Self {
        Self {
            key_id_generator: 0,
            schemas: HashMap::new(),
        }
    }
}

impl Storage for SledStorage {
    fn create_schema(&mut self, schema_name: String) -> Result<()> {
        if self.schemas.contains_key(&schema_name) {
            Err(Error::SchemaAlreadyExists(schema_name))
        } else {
            match sled::Config::default().temporary(true).open() {
                Ok(db) => {
                    self.schemas.insert(schema_name.clone(), db);
                    Ok(())
                }
                Err(_) => unimplemented!(),
            }
        }
    }

    fn drop_schema(&mut self, schema_name: String) -> Result<()> {
        match self.schemas.remove(&schema_name) {
            Some(db) => {
                drop(db);
                Ok(())
            }
            None => Err(Error::SchemaDoesNotExist(schema_name)),
        }
    }

    fn create_table(&mut self, schema_name: String, table_name: String) -> Result<()> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|db| db.tree_names().contains(&(table_name.as_str().into())))
        {
            Err(Error::TableAlreadyExists(
                schema_name + "." + table_name.as_str(),
            ))
        } else {
            self.schemas
                .get_mut(&schema_name)
                .map(|db| db.open_tree(table_name));
            Ok(())
        }
    }

    fn drop_table(&mut self, schema_name: String, table_name: String) -> Result<()> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|db| db.tree_names().contains(&(table_name.as_str().into())))
        {
            self.schemas
                .get(&schema_name)
                .map(|db| db.drop_tree(table_name.as_bytes()));
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
        value: String,
    ) -> Result<()> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            let next_key_id = self.key_id_generator;
            self.key_id_generator += 1;
            self.schemas.get_mut(&schema_name).map(|schema| {
                schema.open_tree(&table_name).ok().map(|table| {
                    table.insert::<[u8; 8], sled::IVec>(
                        next_key_id.to_be_bytes(),
                        value.as_str().into(),
                    )
                })
            });
            Ok(())
        } else {
            Err(Error::TableDoesNotExist(
                schema_name + "." + table_name.as_str(),
            ))
        }
    }

    fn select_all_from(&mut self, schema_name: String, table_name: String) -> Result<Vec<String>> {
        if let Some(true) = self
            .schemas
            .get(&schema_name)
            .map(|schema| schema.tree_names().contains(&(table_name.as_str().into())))
        {
            Ok(self
                .schemas
                .get(&schema_name)
                .and_then(|schema| {
                    schema.open_tree(&table_name).ok().map(|table| {
                        table
                            .iter()
                            .values()
                            .map(sled::Result::unwrap)
                            .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
                            .collect()
                    })
                })
                .unwrap())
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
            self.schemas.get_mut(&schema_name).map(|schema| {
                let table = schema.open_tree(table_name).unwrap();
                for key in table.iter().keys() {
                    table.fetch_and_update(key.unwrap(), |old| Some(value.clone().into_bytes()));
                    records_updated += 1;
                }
            });
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
            self.schemas.get_mut(&schema_name).map(|schema| {
                let mut table = schema.open_tree(table_name).unwrap();
                for key in table.iter().keys() {
                    table.remove(key.unwrap());
                    deleted_records += 1;
                }
            });
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_schemas_with_different_names() {
        let mut storage = SledStorage::new();

        assert_eq!(storage.create_schema("schema_1".to_owned()), Ok(()));
        assert_eq!(storage.create_schema("schema_2".to_owned()), Ok(()));
    }

    #[test]
    fn create_schema_with_existing_name() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(
            storage.create_schema("schema_name".to_owned()),
            Err(Error::SchemaAlreadyExists("schema_name".to_owned()))
        );

        Ok(())
    }

    #[test]
    fn drop_schema() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(storage.drop_schema("schema_name".to_owned()), Ok(()));
        assert_eq!(storage.create_schema("schema_name".to_owned()), Ok(()));

        Ok(())
    }

    #[test]
    fn drop_schema_that_was_not_created() {
        let mut storage = SledStorage::new();

        assert_eq!(
            storage.drop_schema("does_not_exists".to_owned()),
            Err(Error::SchemaDoesNotExist("does_not_exists".to_owned()))
        );
    }

    #[test]
    fn drop_schema_drops_tables_in_it() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name_1".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name_2".to_owned())?;

        assert_eq!(storage.drop_schema("schema_name".to_owned()), Ok(()));
        assert_eq!(storage.create_schema("schema_name".to_owned()), Ok(()));
        assert_eq!(
            storage.create_table("schema_name".to_owned(), "table_name_1".to_owned()),
            Ok(())
        );
        assert_eq!(
            storage.create_table("schema_name".to_owned(), "table_name_2".to_owned()),
            Ok(())
        );

        Ok(())
    }

    #[test]
    fn create_tables_with_different_names() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(
            storage.create_table("schema_name".to_owned(), "table_name_1".to_owned()),
            Ok(())
        );
        assert_eq!(
            storage.create_table("schema_name".to_owned(), "table_name_2".to_owned()),
            Ok(())
        );

        Ok(())
    }

    #[test]
    fn create_table_with_the_same_name() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name".to_owned())?;

        assert_eq!(
            storage.create_table("schema_name".to_owned(), "table_name".to_owned()),
            Err(Error::TableAlreadyExists(
                "schema_name.table_name".to_owned()
            ))
        );
        Ok(())
    }

    #[test]
    fn create_table_with_the_same_name_in_different_schemas() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name_1".to_owned())?;
        storage.create_schema("schema_name_2".to_owned())?;
        assert_eq!(
            storage.create_table("schema_name_1".to_owned(), "table_name".to_owned()),
            Ok(())
        );
        assert_eq!(
            storage.create_table("schema_name_2".to_owned(), "table_name".to_owned()),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn drop_table() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name".to_owned())?;
        assert_eq!(
            storage.drop_table("schema_name".to_owned(), "table_name".to_owned()),
            Ok(())
        );
        assert_eq!(
            storage.create_table("schema_name".to_owned(), "table_name".to_owned()),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn drop_not_created_table() -> Result<()> {
        let mut storage = SledStorage::new();

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
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name".to_owned())?;
        assert_eq!(
            storage.insert_into(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                "123".to_owned()
            ),
            Ok(())
        );
        assert_eq!(
            storage.select_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Ok(vec!["123".to_owned()])
        );

        Ok(())
    }

    #[test]
    fn insert_many_rows_into_table() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name".to_owned())?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "123".to_owned(),
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "456".to_owned(),
        )?;

        assert_eq!(
            storage.select_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Ok(vec!["123".to_owned(), "456".to_owned()])
        );

        Ok(())
    }

    #[test]
    fn insert_into_non_existent_table() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        assert_eq!(
            storage.insert_into(
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
    fn select_from_table_that_does_not_exist() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        assert_eq!(
            storage.select_all_from("schema_name".to_owned(), "not_existed".to_owned()),
            Err(Error::TableDoesNotExist(
                "schema_name.not_existed".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn update_all_records() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name".to_owned())?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "123".to_owned(),
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "456".to_owned(),
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "789".to_owned(),
        )?;

        assert_eq!(
            storage.update_all(
                "schema_name".to_owned(),
                "table_name".to_owned(),
                "567".to_owned()
            ),
            Ok(3)
        );
        assert_eq!(
            storage.select_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Ok(vec!["567".to_owned(), "567".to_owned(), "567".to_owned()])
        );

        Ok(())
    }

    #[test]
    fn update_not_existed_table() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned());
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
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;
        storage.create_table("schema_name".to_owned(), "table_name".to_owned())?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "123".to_owned(),
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "456".to_owned(),
        )?;
        storage.insert_into(
            "schema_name".to_owned(),
            "table_name".to_owned(),
            "789".to_owned(),
        )?;

        assert_eq!(
            storage.delete_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Ok(3)
        );

        assert_eq!(
            storage.select_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Ok(vec![])
        );

        Ok(())
    }

    #[test]
    fn delete_all_from_not_existed_table() -> Result<()> {
        let mut storage = SledStorage::new();

        storage.create_schema("schema_name".to_owned())?;

        assert_eq!(
            storage.delete_all_from("schema_name".to_owned(), "table_name".to_owned()),
            Err(Error::TableDoesNotExist(
                "schema_name.table_name".to_owned()
            ))
        );

        Ok(())
    }
}
