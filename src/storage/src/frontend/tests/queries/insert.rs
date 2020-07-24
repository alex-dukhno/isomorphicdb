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

#[rstest::rstest]
fn insert_into_non_existent_schema(mut storage: PersistentStorage) {
    assert_eq!(
        storage
            .insert_into(
                "non_existent",
                "not_existed",
                vec![(1usize.to_be_bytes().to_vec(), vec!["123".as_bytes()].join(&b'|'))]
            )
            .expect("no system errors"),
        Err(OperationOnTableError::SchemaDoesNotExist)
    );
}

#[rstest::rstest]
fn insert_into_non_existent_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    assert_eq!(
        storage_with_schema
            .insert_into(
                default_schema_name,
                "not_existed",
                vec![(1usize.to_be_bytes().to_vec(), vec!["123".as_bytes()].join(&b'|'))]
            )
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}
