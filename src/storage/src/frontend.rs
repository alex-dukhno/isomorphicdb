// Copyright 2020 Alex Dukhno
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

use crate::backend::{
    self, BackendStorage, CreateObjectError, DropObjectError, NamespaceAlreadyExists, NamespaceDoesNotExist,
    OperationOnObjectError, SledBackendStorage,
};
use crate::{
    CreateTableError, DropTableError, OperationOnTableError, Projection, SchemaAlreadyExists, SchemaDoesNotExist,
};
use core::{SystemError, SystemResult};

pub struct FrontendStorage<P: BackendStorage> {
    key_id_generator: usize,
    persistent: P,
}

impl FrontendStorage<SledBackendStorage> {
    pub fn default() -> SystemResult<Self> {
        Self::new(SledBackendStorage::default())
    }
}

impl<P: BackendStorage> FrontendStorage<P> {
    pub fn new(mut persistent: P) -> SystemResult<Self> {
        match persistent.create_namespace("system")? {
            Ok(()) => Ok(Self {
                key_id_generator: 0,
                persistent,
            }),
            Err(NamespaceAlreadyExists) => {
                Err(SystemError::unrecoverable("system namespace already exists".to_owned()))
            }
        }
    }

    #[allow(clippy::match_wild_err_arm, clippy::map_entry)]
    pub fn create_schema(&mut self, schema_name: &str) -> SystemResult<Result<(), SchemaAlreadyExists>> {
        match self.persistent.create_namespace(schema_name)? {
            Ok(()) => Ok(Ok(())),
            Err(NamespaceAlreadyExists) => Ok(Err(SchemaAlreadyExists)),
        }
    }

    pub fn drop_schema(&mut self, schema_name: &str) -> SystemResult<Result<(), SchemaDoesNotExist>> {
        match self.persistent.drop_namespace(schema_name)? {
            Ok(()) => Ok(Ok(())),
            Err(NamespaceDoesNotExist) => Ok(Err(SchemaDoesNotExist)),
        }
    }

    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_names: Vec<String>,
    ) -> SystemResult<Result<(), CreateTableError>> {
        match self.persistent.create_object(schema_name, table_name)? {
            Ok(()) => {
                match self
                    .persistent
                    .create_object("system", (schema_name.to_owned() + "." + table_name).as_str())?
                {
                    Ok(()) => {}
                    e => {
                        log::debug!("{:?}", e);
                        unimplemented!();
                    }
                }
                match self.persistent.write(
                    "system",
                    (schema_name.to_owned() + "." + table_name).as_str(),
                    vec![(
                        self.key_id_generator.to_be_bytes().to_vec(),
                        column_names.into_iter().map(|s| s.into_bytes()).collect(),
                    )],
                )? {
                    Ok(_written) => {}
                    _ => unimplemented!(),
                }
                self.key_id_generator += 1;
                Ok(Ok(()))
            }
            Err(CreateObjectError::ObjectAlreadyExists) => Ok(Err(CreateTableError::TableAlreadyExists)),
            Err(CreateObjectError::NamespaceDoesNotExist) => Ok(Err(CreateTableError::SchemaDoesNotExist)),
        }
    }

    pub fn table_columns(
        &mut self,
        schema_name: &str,
        table_name: &str,
    ) -> SystemResult<Result<Vec<String>, OperationOnTableError>> {
        let reads = self
            .persistent
            .read("system", (schema_name.to_owned() + "." + table_name).as_str())?;
        match reads {
            Ok(reads) => Ok(Ok(reads
                .map(backend::Result::unwrap)
                .map(|(_id, columns)| columns.iter().map(|c| String::from_utf8(c.to_vec()).unwrap()).collect())
                .next()
                .unwrap())),
            Err(OperationOnObjectError::ObjectDoesNotExist) => Ok(Err(OperationOnTableError::TableDoesNotExist)),
            Err(OperationOnObjectError::NamespaceDoesNotExist) => Ok(Err(OperationOnTableError::SchemaDoesNotExist)),
        }
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> SystemResult<Result<(), DropTableError>> {
        match self.persistent.drop_object(schema_name, table_name)? {
            Ok(()) => {
                match self
                    .persistent
                    .drop_object("system", (schema_name.to_owned() + "." + table_name).as_str())?
                {
                    Ok(()) => Ok(Ok(())),
                    _ => unimplemented!(),
                }
            }
            Err(DropObjectError::ObjectDoesNotExist) => Ok(Err(DropTableError::TableDoesNotExist)),
            Err(DropObjectError::NamespaceDoesNotExist) => Ok(Err(DropTableError::SchemaDoesNotExist)),
        }
    }

    pub fn insert_into(
        &mut self,
        schema_name: &str,
        table_name: &str,
        values: Vec<Vec<String>>,
    ) -> SystemResult<Result<(), OperationOnTableError>> {
        let mut to_write = vec![];
        for value in values {
            let key = self.key_id_generator.to_be_bytes().to_vec();
            to_write.push((key, value.iter().map(|s| s.clone().into_bytes()).collect()));
            self.key_id_generator += 1;
        }
        match self.persistent.write(schema_name, table_name, to_write)? {
            Ok(_size) => Ok(Ok(())),
            Err(OperationOnObjectError::ObjectDoesNotExist) => Ok(Err(OperationOnTableError::TableDoesNotExist)),
            Err(OperationOnObjectError::NamespaceDoesNotExist) => Ok(Err(OperationOnTableError::SchemaDoesNotExist)),
        }
    }

    pub fn select_all_from(
        &mut self,
        schema_name: &str,
        table_name: &str,
        columns: Vec<String>,
    ) -> SystemResult<Result<Projection, OperationOnTableError>> {
        match self.table_columns(schema_name, table_name)? {
            Ok(all_columns) => {
                let mut column_indexes = vec![];
                for (i, column) in columns.iter().enumerate() {
                    for (index, name) in all_columns.iter().enumerate() {
                        if name == column {
                            column_indexes.push((index, i));
                        }
                    }
                }
                Ok(Ok((
                    columns,
                    self.persistent
                        .read(schema_name, table_name)?
                        .unwrap()
                        .map(backend::Result::unwrap)
                        .map(|(_key, values)| values)
                        .map(|bytes| {
                            let all_values = bytes
                                .iter()
                                .map(|b| String::from_utf8(b.to_vec()).unwrap())
                                .collect::<Vec<String>>();
                            let mut values = vec![];
                            for (origin, ord) in &column_indexes {
                                for (index, value) in all_values.iter().enumerate() {
                                    if index == *origin {
                                        values.push((ord, value.clone()))
                                    }
                                }
                            }
                            values.into_iter().map(|(_, value)| value).collect()
                        })
                        .collect(),
                )))
            }
            Err(e) => Ok(Err(e)),
        }
    }

    pub fn update_all(
        &mut self,
        schema_name: &str,
        table_name: &str,
        value: String,
    ) -> SystemResult<Result<usize, OperationOnTableError>> {
        let reads = self.persistent.read(schema_name, table_name)?;
        match reads {
            Ok(reads) => {
                let to_update: Vec<(Vec<u8>, Vec<Vec<u8>>)> = reads
                    .map(backend::Result::unwrap)
                    .map(|(key, _)| (key, vec![value.clone().into_bytes()]))
                    .collect();

                let len = to_update.len();
                self.persistent.write(schema_name, table_name, to_update)?.unwrap();
                Ok(Ok(len))
            }
            Err(OperationOnObjectError::ObjectDoesNotExist) => Ok(Err(OperationOnTableError::TableDoesNotExist)),
            Err(OperationOnObjectError::NamespaceDoesNotExist) => Ok(Err(OperationOnTableError::SchemaDoesNotExist)),
        }
    }

    pub fn delete_all_from(
        &mut self,
        schema_name: &str,
        table_name: &str,
    ) -> SystemResult<Result<usize, OperationOnTableError>> {
        let reads = self.persistent.read(schema_name, table_name)?;

        let to_delete: Vec<Vec<u8>> = match reads {
            Ok(reads) => reads.map(backend::Result::unwrap).map(|(key, _)| key).collect(),
            Err(OperationOnObjectError::ObjectDoesNotExist) => {
                return Ok(Err(OperationOnTableError::TableDoesNotExist))
            }
            Err(OperationOnObjectError::NamespaceDoesNotExist) => {
                return Ok(Err(OperationOnTableError::SchemaDoesNotExist))
            }
        };

        match self.persistent.delete(schema_name, table_name, to_delete)? {
            Ok(len) => Ok(Ok(len)),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_schemas_with_different_names() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        assert_eq!(storage.create_schema("schema_1").expect("no system errors"), Ok(()));
        assert_eq!(storage.create_schema("schema_2").expect("no system errors"), Ok(()));
    }

    #[test]
    fn create_schema_with_existing_name() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");

        assert_eq!(
            storage.create_schema("schema_name").expect("no system errors"),
            Err(SchemaAlreadyExists)
        );
    }

    #[test]
    fn drop_schema() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");

        assert_eq!(storage.drop_schema("schema_name").expect("no system errors"), Ok(()));
        assert_eq!(storage.create_schema("schema_name").expect("no system errors"), Ok(()));
    }

    #[test]
    fn drop_schema_that_was_not_created() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        assert_eq!(
            storage.drop_schema("does_not_exists").expect("no system errors"),
            Err(SchemaDoesNotExist)
        );
    }

    #[test]
    #[ignore]
    // TODO store tables and columns into "system" schema
    //      but simple select by predicate has to be implemented
    fn drop_schema_drops_tables_in_it() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");
        storage
            .create_table("schema_name", "table_name_1", vec!["column_test".to_owned()])
            .expect("no system errors")
            .expect("values are inserted");
        storage
            .create_table("schema_name", "table_name_2", vec!["column_test".to_owned()])
            .expect("no system errors")
            .expect("values are inserted");

        assert_eq!(storage.drop_schema("schema_name").expect("no system errors"), Ok(()));
        assert_eq!(storage.create_schema("schema_name").expect("no system errors"), Ok(()));
        assert_eq!(
            storage
                .create_table("schema_name", "table_name_1", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Ok(())
        );
        assert_eq!(
            storage
                .create_table("schema_name", "table_name_2", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Ok(())
        );
    }

    #[test]
    fn create_tables_with_different_names() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");

        assert_eq!(
            storage
                .create_table("schema_name", "table_name_1", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Ok(())
        );
        assert_eq!(
            storage
                .create_table("schema_name", "table_name_2", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Ok(())
        );
    }

    #[test]
    fn create_table_with_the_same_name() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);

        assert_eq!(
            storage
                .create_table("schema_name", "table_name", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Err(CreateTableError::TableAlreadyExists)
        );
    }

    #[test]
    fn create_table_with_the_same_name_in_different_schemas() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name_1")
            .expect("no system errors")
            .expect("schema is created");
        storage
            .create_schema("schema_name_2")
            .expect("no system errors")
            .expect("schema is created");
        assert_eq!(
            storage
                .create_table("schema_name_1", "table_name", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Ok(())
        );
        assert_eq!(
            storage
                .create_table("schema_name_2", "table_name", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Ok(())
        );
    }

    #[test]
    fn drop_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);
        assert_eq!(
            storage
                .drop_table("schema_name", "table_name")
                .expect("no system errors"),
            Ok(())
        );
        assert_eq!(
            storage
                .create_table("schema_name", "table_name", vec!["column_test".to_owned()])
                .expect("no system errors"),
            Ok(())
        );
    }

    #[test]
    fn drop_not_created_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");
        assert_eq!(
            storage
                .drop_table("schema_name", "not_existed_table")
                .expect("no system errors"),
            Err(DropTableError::TableDoesNotExist)
        );
    }

    #[test]
    fn insert_row_into_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);
        assert_eq!(
            storage
                .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]],)
                .expect("no system errors"),
            Ok(())
        );

        let table_columns = storage
            .table_columns("schema_name", "table_name")
            .expect("no system errors")
            .expect("table has columns");

        assert_eq!(
            storage
                .select_all_from("schema_name", "table_name", table_columns)
                .expect("no system errors"),
            Ok((vec!["column_test".to_owned()], vec![vec!["123".to_owned()]]))
        );
    }

    #[test]
    fn insert_many_rows_into_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);
        storage
            .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");
        storage
            .insert_into("schema_name", "table_name", vec![vec!["456".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");

        let table_columns = storage
            .table_columns("schema_name", "table_name")
            .expect("no system errors")
            .expect("table has columns");

        assert_eq!(
            storage
                .select_all_from("schema_name", "table_name", table_columns)
                .expect("no system errors"),
            Ok((
                vec!["column_test".to_owned()],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            ))
        );
    }

    #[test]
    fn insert_into_non_existent_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");
        assert_eq!(
            storage
                .insert_into("schema_name", "not_existed", vec![vec!["123".to_owned()]],)
                .expect("no system errors"),
            Err(OperationOnTableError::TableDoesNotExist)
        );
    }

    #[test]
    fn select_from_table_that_does_not_exist() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");
        assert_eq!(
            storage
                .table_columns("schema_name", "not_existed")
                .expect("no system errors"),
            Err(OperationOnTableError::TableDoesNotExist)
        );
    }

    #[test]
    fn update_all_records() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);
        storage
            .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");
        storage
            .insert_into("schema_name", "table_name", vec![vec!["456".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");
        storage
            .insert_into("schema_name", "table_name", vec![vec!["789".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");

        assert_eq!(
            storage
                .update_all("schema_name", "table_name", "567".to_owned())
                .expect("no system errors"),
            Ok(3)
        );

        let table_columns = storage
            .table_columns("schema_name", "table_name")
            .expect("no system errors")
            .expect("table has columns");

        assert_eq!(
            storage
                .select_all_from("schema_name", "table_name", table_columns)
                .expect("no system errors"),
            Ok((
                vec!["column_test".to_owned()],
                vec![vec!["567".to_owned()], vec!["567".to_owned()], vec!["567".to_owned()]]
            ))
        );
    }

    #[test]
    fn update_not_existed_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");
        assert_eq!(
            storage
                .update_all("schema_name", "not_existed", "123".to_owned())
                .expect("no system errors"),
            Err(OperationOnTableError::TableDoesNotExist)
        );
    }

    #[test]
    fn delete_all_from_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);
        storage
            .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");
        storage
            .insert_into("schema_name", "table_name", vec![vec!["456".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");
        storage
            .insert_into("schema_name", "table_name", vec![vec!["789".to_owned()]])
            .expect("no system errors")
            .expect("values are inserted");

        assert_eq!(
            storage
                .delete_all_from("schema_name", "table_name")
                .expect("no system errors"),
            Ok(3)
        );

        let table_columns = storage
            .table_columns("schema_name", "table_name")
            .expect("no system errors")
            .expect("table has columns");

        assert_eq!(
            storage
                .select_all_from("schema_name", "table_name", table_columns)
                .expect("no system errors"),
            Ok((vec!["column_test".to_owned()], vec![]))
        );
    }

    #[test]
    fn delete_all_from_not_existed_table() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        storage
            .create_schema("schema_name")
            .expect("no system errors")
            .expect("schema is created");

        assert_eq!(
            storage
                .delete_all_from("schema_name", "table_name")
                .expect("no system errors"),
            Err(OperationOnTableError::TableDoesNotExist)
        );
    }

    #[test]
    fn select_all_from_table_with_many_columns() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_1", "column_2", "column_3"],
        );
        storage
            .insert_into(
                "schema_name",
                "table_name",
                vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]],
            )
            .expect("no system errors")
            .expect("values are inserted");

        let table_columns = storage
            .table_columns("schema_name", "table_name")
            .expect("no system errors")
            .expect("table has columns");

        assert_eq!(
            storage
                .select_all_from("schema_name", "table_name", table_columns)
                .expect("no system errors"),
            Ok((
                vec!["column_1".to_owned(), "column_2".to_owned(), "column_3".to_owned()],
                vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]]
            ))
        );
    }

    #[test]
    fn insert_multiple_rows() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["column_1", "column_2", "column_3"],
        );
        storage
            .insert_into(
                "schema_name",
                "table_name",
                vec![
                    vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
                ],
            )
            .expect("no system errors")
            .expect("values are inserted");

        let table_columns = storage
            .table_columns("schema_name", "table_name")
            .expect("no system errors")
            .expect("table has columns");

        assert_eq!(
            storage
                .select_all_from("schema_name", "table_name", table_columns)
                .expect("no system errors"),
            Ok((
                vec!["column_1".to_owned(), "column_2".to_owned(), "column_3".to_owned()],
                vec![
                    vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
                ],
            ))
        );
    }

    #[test]
    fn select_first_and_last_columns_from_table_with_multiple_columns() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["first", "middle", "last"],
        );
        storage
            .insert_into(
                "schema_name",
                "table_name",
                vec![
                    vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
                ],
            )
            .expect("no system errors")
            .expect("values are inserted");

        assert_eq!(
            storage
                .select_all_from("schema_name", "table_name", vec!["first".to_owned(), "last".to_owned()])
                .expect("no system errors"),
            Ok((
                vec!["first".to_owned(), "last".to_owned(),],
                vec![
                    vec!["1".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "9".to_owned()],
                ],
            ))
        );
    }

    #[test]
    fn select_all_columns_reordered_from_table_with_multiple_columns() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["first", "middle", "last"],
        );
        storage
            .insert_into(
                "schema_name",
                "table_name",
                vec![
                    vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
                ],
            )
            .expect("no system errors")
            .expect("values are inserted");

        assert_eq!(
            storage
                .select_all_from(
                    "schema_name",
                    "table_name",
                    vec!["last".to_owned(), "first".to_owned(), "middle".to_owned()]
                )
                .expect("no system errors"),
            Ok((
                vec!["last".to_owned(), "first".to_owned(), "middle".to_owned()],
                vec![
                    vec!["3".to_owned(), "1".to_owned(), "2".to_owned()],
                    vec!["6".to_owned(), "4".to_owned(), "5".to_owned()],
                    vec!["9".to_owned(), "7".to_owned(), "8".to_owned()],
                ],
            ))
        );
    }

    #[test]
    fn select_with_column_name_duplication() {
        let mut storage = FrontendStorage::default().expect("no system errors");

        create_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec!["first", "middle", "last"],
        );
        storage
            .insert_into(
                "schema_name",
                "table_name",
                vec![
                    vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
                ],
            )
            .expect("no system errors")
            .expect("values are inserted");

        assert_eq!(
            storage
                .select_all_from(
                    "schema_name",
                    "table_name",
                    vec![
                        "last".to_owned(),
                        "middle".to_owned(),
                        "first".to_owned(),
                        "last".to_owned(),
                        "middle".to_owned()
                    ]
                )
                .expect("no system errors"),
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
    }

    fn create_table<P: backend::BackendStorage>(
        storage: &mut FrontendStorage<P>,
        schema_name: &str,
        table_name: &str,
        column_names: Vec<&str>,
    ) {
        storage
            .create_schema(schema_name)
            .expect("no system errors")
            .expect("schema is created");
        storage
            .create_table(
                schema_name,
                table_name,
                column_names.into_iter().map(ToOwned::to_owned).collect::<Vec<String>>(),
            )
            .expect("no system errors")
            .expect("table is created");
    }
}
