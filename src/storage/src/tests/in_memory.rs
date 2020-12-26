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

use super::*;
use crate::InMemoryDatabase;

type Storage = InMemoryDatabase;

#[rstest::fixture]
fn storage() -> Storage {
    Storage::default()
}

#[rstest::fixture]
fn with_schema(storage: Storage, schema_name: SchemaName) -> Storage {
    storage
        .create_schema(schema_name)
        .expect("no io error")
        .expect("no platform errors")
        .expect("schema created");
    storage
}

#[rstest::fixture]
fn with_object(with_schema: Storage, schema_name: SchemaName, object_name: ObjectName) -> Storage {
    with_schema
        .create_object(schema_name, object_name)
        .expect("no io error")
        .expect("no storage error")
        .expect("object created");
    with_schema
}

#[cfg(test)]
mod sequences {
    use super::*;

    #[rstest::rstest]
    fn no_schema() {
        assert!(matches!(
            storage().create_sequence("not_existing_schema", "sequence"),
            Err(DefinitionError::SchemaDoesNotExist)
        ));
    }

    #[rstest::rstest]
    fn generate_identifier(with_schema: Storage, schema_name: SchemaName) {
        let sequence = with_schema
            .create_sequence(schema_name, "sequence")
            .expect("schema exists");

        assert_eq!(sequence.next(), 0);
    }

    #[rstest::rstest]
    fn generate_many_identifiers(with_schema: Storage, schema_name: SchemaName) {
        let _sequence = with_schema
            .create_sequence(schema_name, "sequence")
            .expect("schema exists");

        assert_eq!(with_schema.get_sequence(schema_name, "sequence").unwrap().next(), 0);
        assert_eq!(with_schema.get_sequence(schema_name, "sequence").unwrap().next(), 1);
        assert_eq!(with_schema.get_sequence(schema_name, "sequence").unwrap().next(), 2);
    }

    #[rstest::rstest]
    fn generate_many_identifiers_with_step(with_schema: Storage, schema_name: SchemaName) {
        let sequence = with_schema
            .create_sequence_with_step(schema_name, "sequence", 5)
            .expect("schema exists");

        assert_eq!(sequence.next(), 0);
        assert_eq!(with_schema.get_sequence(schema_name, "sequence").unwrap().next(), 5);
        assert_eq!(sequence.next(), 10);
    }

    #[rstest::rstest]
    fn overflow(with_schema: Storage, schema_name: SchemaName) {
        let sequence = with_schema
            .create_sequence_with_step(schema_name, "sequence", u64::MAX / 2)
            .expect("schema exists");

        assert_eq!(sequence.next(), 0);
        assert_eq!(sequence.next(), u64::MAX / 2);
        assert_eq!(sequence.next(), u64::MAX - 1);
        assert_eq!(sequence.next(), u64::MAX / 2 - 2);
    }

    #[rstest::rstest]
    fn step_should_be_non_zero(with_schema: Storage, schema_name: SchemaName) {
        assert!(matches!(
            with_schema.create_sequence_with_step(schema_name, "sequence", 0),
            Err(DefinitionError::ZeroStepSequence)
        ))
    }
}

#[cfg(test)]
mod schemas {
    use super::*;

    #[rstest::rstest]
    fn create_schemas_with_different_names(storage: Storage) {
        assert_eq!(storage.create_schema(SCHEMA_1).expect("no io error"), Ok(Ok(())));
        assert_eq!(storage.create_schema(SCHEMA_2).expect("no io error"), Ok(Ok(())));
    }

    #[rstest::rstest]
    fn drop_schema(with_schema: Storage, schema_name: SchemaName) {
        assert_eq!(with_schema.drop_schema(schema_name).expect("no io error"), Ok(Ok(())));
        assert_eq!(with_schema.create_schema(schema_name).expect("no io error"), Ok(Ok(())));
    }

    #[rstest::rstest]
    fn dropping_schema_drops_objects_in_it(with_schema: Storage, schema_name: SchemaName) {
        with_schema
            .create_object(schema_name, OBJECT_1)
            .expect("no io error")
            .expect("no storage error")
            .expect("object created");
        with_schema
            .create_object(schema_name, OBJECT_2)
            .expect("no io error")
            .expect("no storage error")
            .expect("object created");

        assert_eq!(with_schema.drop_schema(schema_name).expect("no io error"), Ok(Ok(())));
        assert_eq!(with_schema.create_schema(schema_name).expect("no io error"), Ok(Ok(())));
        assert_eq!(
            with_schema.create_object(schema_name, OBJECT_1).expect("no io error"),
            Ok(Ok(()))
        );
        assert_eq!(
            with_schema.create_object(schema_name, OBJECT_2).expect("no io error"),
            Ok(Ok(()))
        );
    }

    #[rstest::rstest]
    fn create_schema_with_the_same_name(with_schema: Storage, schema_name: SchemaName) {
        assert_eq!(
            with_schema.create_schema(schema_name).expect("no io error"),
            Ok(Err(DefinitionError::SchemaAlreadyExists))
        )
    }

    #[rstest::rstest]
    fn drop_schema_that_does_not_exist(storage: Storage, schema_name: SchemaName) {
        assert_eq!(
            storage.drop_schema(schema_name).expect("no io error"),
            Ok(Err(DefinitionError::SchemaDoesNotExist))
        )
    }
}

#[cfg(test)]
mod create_object {
    use super::*;

    #[rstest::rstest]
    fn create_objects_with_different_names(with_schema: Storage, schema_name: SchemaName) {
        assert_eq!(
            with_schema.create_object(schema_name, OBJECT_1).expect("no io error"),
            Ok(Ok(()))
        );
        assert_eq!(
            with_schema.create_object(schema_name, OBJECT_2).expect("no io error"),
            Ok(Ok(()))
        );
    }

    #[rstest::rstest]
    fn create_objects_with_the_same_name_in_the_same_schema(
        with_object: Storage,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) {
        assert_eq!(
            with_object
                .create_object(schema_name, object_name)
                .expect("no io error"),
            Ok(Err(DefinitionError::ObjectAlreadyExists))
        )
    }

    #[rstest::rstest]
    fn create_objects_in_non_existent_schema(storage: Storage, object_name: SchemaName) {
        assert_eq!(
            storage.create_object(DOES_NOT_EXIST, object_name).expect("no io error"),
            Ok(Err(DefinitionError::SchemaDoesNotExist))
        )
    }

    #[rstest::rstest]
    fn create_object_with_the_same_name_in_different_namespaces(storage: Storage) {
        storage
            .create_schema(SCHEMA_1)
            .expect("no io error")
            .expect("no platform errors")
            .expect("schema created");
        storage
            .create_schema(SCHEMA_2)
            .expect("no io error")
            .expect("no platform errors")
            .expect("schema created");
        assert_eq!(
            storage.create_object(SCHEMA_1, OBJECT).expect("no io error"),
            Ok(Ok(()))
        );
        assert_eq!(
            storage.create_object(SCHEMA_2, OBJECT).expect("no io error"),
            Ok(Ok(()))
        );
    }
}

#[cfg(test)]
mod drop_object {
    use super::*;

    #[rstest::rstest]
    fn drop_object(with_object: Storage, schema_name: SchemaName, object_name: ObjectName) {
        assert_eq!(
            with_object.drop_object(schema_name, object_name).expect("no io error"),
            Ok(Ok(()))
        );
        assert_eq!(
            with_object
                .create_object(schema_name, object_name)
                .expect("no io error"),
            Ok(Ok(()))
        );
    }

    #[rstest::rstest]
    fn drop_object_from_schema_that_does_not_exist(storage: Storage, object_name: ObjectName) {
        assert_eq!(
            storage.drop_object(DOES_NOT_EXIST, object_name).expect("no io error"),
            Ok(Err(DefinitionError::SchemaDoesNotExist))
        );
    }

    #[rstest::rstest]
    fn drop_object_that_does_not_exist(with_schema: Storage, schema_name: SchemaName, object_name: ObjectName) {
        assert_eq!(
            with_schema.drop_object(schema_name, object_name).expect("no io error"),
            Ok(Err(DefinitionError::ObjectDoesNotExist))
        );
    }
}

#[cfg(test)]
mod operations_on_object {
    use super::*;

    #[rstest::rstest]
    fn write_row_into_object_that_does_not_exist(
        with_schema: Storage,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) {
        assert_eq!(
            with_schema
                .write(schema_name, object_name, as_rows(vec![(1u8, vec!["123"])]))
                .expect("no io error"),
            Ok(Err(DefinitionError::ObjectDoesNotExist))
        );
    }

    #[rstest::rstest]
    fn write_row_into_object_in_schema_that_does_not_exist(
        storage: Storage,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) {
        assert_eq!(
            storage
                .write(schema_name, object_name, as_rows(vec![(1u8, vec!["123"])]))
                .expect("no io error"),
            Ok(Err(DefinitionError::SchemaDoesNotExist))
        );
    }

    #[rstest::rstest]
    fn write_read_row_into_object(with_object: Storage, schema_name: SchemaName, object_name: ObjectName) {
        assert_eq!(
            with_object
                .write(schema_name, object_name, as_rows(vec![(1u8, vec!["123"])]))
                .expect("no io error"),
            Ok(Ok(1))
        );

        assert_eq!(
            with_object
                .read(schema_name, object_name)
                .expect("no io error")
                .expect("no platform error")
                .map(|iter| iter
                    .map(|ok| ok.expect("no io error"))
                    .collect::<Vec<Result<Row, StorageError>>>()),
            Ok(as_read_cursor(vec![(1u8, vec!["123"])])
                .map(|ok| ok.expect("no io error"))
                .collect())
        );
    }

    #[rstest::rstest]
    fn write_read_many_rows_into_object(with_object: Storage, schema_name: SchemaName, object_name: ObjectName) {
        with_object
            .write(schema_name, object_name, as_rows(vec![(1u8, vec!["123"])]))
            .expect("no io error")
            .expect("no platform error")
            .expect("values are written");
        with_object
            .write(schema_name, object_name, as_rows(vec![(2u8, vec!["456"])]))
            .expect("no io error")
            .expect("no platform error")
            .expect("values are written");

        assert_eq!(
            with_object
                .read(schema_name, object_name)
                .expect("no io error")
                .expect("no platform error")
                .map(|iter| iter
                    .map(|ok| ok.expect("no io error"))
                    .collect::<Vec<Result<Row, StorageError>>>()),
            Ok(as_read_cursor(vec![(1u8, vec!["123"]), (2u8, vec!["456"])])
                .map(|ok| ok.expect("no io error"))
                .collect())
        );
    }

    #[rstest::rstest]
    fn delete_from_object_that_does_not_exist(with_schema: Storage, schema_name: SchemaName, object_name: ObjectName) {
        assert_eq!(
            with_schema
                .delete(schema_name, object_name, vec![])
                .expect("no io error"),
            Ok(Err(DefinitionError::ObjectDoesNotExist))
        );
    }

    #[rstest::rstest]
    fn delete_from_object_that_in_schema_that_does_not_exist(
        storage: Storage,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) {
        assert_eq!(
            storage.delete(schema_name, object_name, vec![]).expect("no io error"),
            Ok(Err(DefinitionError::SchemaDoesNotExist))
        );
    }

    #[rstest::rstest]
    fn write_delete_read_records_from_object(with_object: Storage, schema_name: SchemaName, object_name: ObjectName) {
        with_object
            .write(
                schema_name,
                object_name,
                as_rows(vec![(1u8, vec!["123"]), (2u8, vec!["456"]), (3u8, vec!["789"])]),
            )
            .expect("no io error")
            .expect("no platform error")
            .expect("values are written");

        assert_eq!(
            with_object
                .delete(schema_name, object_name, as_keys(vec![2u8]))
                .expect("no io error"),
            Ok(Ok(1))
        );

        assert_eq!(
            with_object
                .read(schema_name, object_name)
                .expect("no io error")
                .expect("no platform error")
                .map(|iter| iter
                    .map(|ok| ok.expect("no io error"))
                    .collect::<Vec<Result<Row, StorageError>>>()),
            Ok(as_read_cursor(vec![(1u8, vec!["123"]), (3u8, vec!["789"])])
                .map(|ok| ok.expect("no io error"))
                .collect())
        );
    }

    #[rstest::rstest]
    fn read_from_object_that_does_not_exist(with_schema: Storage, schema_name: SchemaName, object_name: ObjectName) {
        assert!(matches!(
            with_schema.read(schema_name, object_name).expect("no io error"),
            Ok(Err(DefinitionError::ObjectDoesNotExist))
        ));
    }

    #[rstest::rstest]
    fn read_from_object_that_in_schema_that_does_not_exist(
        storage: Storage,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) {
        assert!(matches!(
            storage.read(schema_name, object_name).expect("no io error"),
            Ok(Err(DefinitionError::SchemaDoesNotExist))
        ));
    }

    #[rstest::rstest]
    fn read_all_from_object_with_many_columns(with_object: Storage, schema_name: SchemaName, object_name: ObjectName) {
        with_object
            .write(schema_name, object_name, as_rows(vec![(1u8, vec!["1", "2", "3"])]))
            .expect("no io error")
            .expect("no platform error")
            .expect("values are written");

        assert_eq!(
            with_object
                .read(schema_name, object_name)
                .expect("no io error")
                .expect("no platform error")
                .map(|iter| iter
                    .map(|ok| ok.expect("no io error"))
                    .collect::<Vec<Result<Row, StorageError>>>()),
            Ok(as_read_cursor(vec![(1u8, vec!["1", "2", "3"])])
                .map(|ok| ok.expect("no io error"))
                .collect())
        );
    }

    #[rstest::rstest]
    fn write_read_multiple_columns(with_object: Storage, schema_name: SchemaName, object_name: ObjectName) {
        with_object
            .write(
                schema_name,
                object_name,
                as_rows(vec![
                    (1u8, vec!["1", "2", "3"]),
                    (2u8, vec!["4", "5", "6"]),
                    (3u8, vec!["7", "8", "9"]),
                ]),
            )
            .expect("no io error")
            .expect("no platform error")
            .expect("values are written");

        assert_eq!(
            with_object
                .read(schema_name, object_name)
                .expect("no io error")
                .expect("no platform error")
                .map(|iter| iter
                    .map(|ok| ok.expect("no io error"))
                    .collect::<Vec<Result<Row, StorageError>>>()),
            Ok(as_read_cursor(vec![
                (1u8, vec!["1", "2", "3"]),
                (2u8, vec!["4", "5", "6"]),
                (3u8, vec!["7", "8", "9"])
            ])
            .map(|ok| ok.expect("no io error"))
            .collect()),
        );
    }
}
