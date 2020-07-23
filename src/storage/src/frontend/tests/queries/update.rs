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
use sql_types::SqlType;

// #[rstest::rstest]
// #[ignore]
// fn update_all_records(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
//     );
//
//     let row1 = vec![Datum::from_i16(123)];
//     let row2 = vec![Datum::from_i16(456)];
//     let row3 = vec![Datum::from_i16(789)];
//     insert_into(&mut storage_with_schema, default_schema_name, "table_name", row1);
//     insert_into(&mut storage_with_schema, default_schema_name, "table_name", row2);
//     insert_into(&mut storage_with_schema, default_schema_name, "table_name", row3);
//
//     assert_eq!(
//         storage_with_schema
//             .update_all(
//                 default_schema_name,
//                 "table_name",
//                 vec![("column_test".to_owned(), "567".to_owned())]
//             )
//             .expect("no system errors"),
//         Ok(3)
//     );
//
//     let table_columns = storage_with_schema
//         .table_columns(default_schema_name, "table_name")
//         .expect("no system errors")
//         .into_iter()
//         .map(|column_definition| column_definition.name())
//         .collect();
//
//     let new_row = Row::pack(&[Datum::from_i16(567)]).to_bytes();
//     assert_eq!(
//         storage_with_schema
//             .select_all_from(default_schema_name, "table_name", table_columns)
//             .expect("no system errors"),
//         Ok((
//             vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
//             vec![new_row.clone(), new_row.clone(), new_row],
//         ))
//     );
// }
//
// #[rstest::rstest]
// #[ignore]
// fn update_not_existed_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     assert_eq!(
//         storage_with_schema
//             .update_all(default_schema_name, "not_existed", vec![])
//             .expect("no system errors"),
//         Err(OperationOnTableError::TableDoesNotExist)
//     );
// }
//
// #[rstest::rstest]
// #[ignore]
// fn update_non_existent_schema(mut storage: PersistentStorage) {
//     assert_eq!(
//         storage
//             .update_all("non_existent", "not_existed", vec![])
//             .expect("no system errors"),
//         Err(OperationOnTableError::SchemaDoesNotExist)
//     );
// }
// /*
// #[cfg(test)]
// mod constraints {
//     use super::*;
//     use sql_types::ConstraintError;
//
//     #[rstest::fixture]
//     fn storage_with_ints_table(
//         default_schema_name: &str,
//         mut storage_with_schema: PersistentStorage,
//     ) -> PersistentStorage {
//         create_table(
//             &mut storage_with_schema,
//             default_schema_name,
//             "table_name",
//             vec![
//                 column_definition("column_si", SqlType::SmallInt(i16::min_value())),
//                 column_definition("column_i", SqlType::Integer(i32::min_value())),
//                 column_definition("column_bi", SqlType::BigInt(i64::min_value())),
//             ],
//         );
//         storage_with_schema
//     }
//
//     #[rstest::fixture]
//     fn storage_with_chars_table(
//         default_schema_name: &str,
//         mut storage_with_schema: PersistentStorage,
//     ) -> PersistentStorage {
//         create_table(
//             &mut storage_with_schema,
//             default_schema_name,
//             "table_name",
//             vec![
//                 column_definition("column_c", SqlType::Char(10)),
//                 column_definition("column_vc", SqlType::VarChar(10)),
//             ],
//         );
//         storage_with_schema
//     }
//
//     #[rstest::rstest]
//     fn out_of_range_violation(default_schema_name: &str, mut storage_with_ints_table: PersistentStorage) {
//         let row = Row::pack(&["100".to_owned(), "100".to_owned(), "100".to_owned()]).to_bytes();
//         storage_with_ints_table
//             .insert_into(
//                 default_schema_name,
//                 "table_name",
//                 vec![],
//                 vec![],
//             )
//             .expect("no system errors")
//             .expect("record inserted");
//         assert_eq!(
//             storage_with_ints_table
//                 .update_all(
//                     default_schema_name,
//                     "table_name",
//                     vec![
//                         ("column_si".to_owned(), "-32769".to_owned()),
//                         ("column_i".to_owned(), "100".to_owned()),
//                         ("column_bi".to_owned(), "100".to_owned())
//                     ]
//                 )
//                 .expect("no system errors"),
//             Err(OperationOnTableError::ConstraintViolations(
//                 vec![(
//                     ConstraintError::OutOfRange,
//                     column_definition("column_si", SqlType::SmallInt(i16::min_value()))
//                 )],
//                 1
//             ))
//         );
//     }
//
//     #[rstest::rstest]
//     fn not_an_int_violation(default_schema_name: &str, mut storage_with_ints_table: PersistentStorage) {
//         storage_with_ints_table
//             .insert_into(
//                 default_schema_name,
//                 "table_name",
//                 vec![],
//                 vec![vec!["100".to_owned(), "100".to_owned(), "100".to_owned()]],
//             )
//             .expect("no system errors")
//             .expect("record inserted");
//         assert_eq!(
//             storage_with_ints_table
//                 .update_all(
//                     default_schema_name,
//                     "table_name",
//                     vec![
//                         ("column_si".to_owned(), "abc".to_owned()),
//                         ("column_i".to_owned(), "100".to_owned()),
//                         ("column_bi".to_owned(), "100".to_owned())
//                     ]
//                 )
//                 .expect("no system errors"),
//             Err(OperationOnTableError::ConstraintViolations(
//                 vec![(
//                     ConstraintError::TypeMismatch("abc".to_owned()),
//                     column_definition("column_si", SqlType::SmallInt(i16::min_value()))
//                 )],
//                 1
//             ))
//         );
//     }
//
//     #[rstest::rstest]
//     fn value_too_long_violation(default_schema_name: &str, mut storage_with_chars_table: PersistentStorage) {
//         storage_with_chars_table
//             .insert_into(
//                 default_schema_name,
//                 "table_name",
//                 vec![],
//                 vec![vec!["100".to_owned(), "100".to_owned()]],
//             )
//             .expect("no system errors")
//             .expect("record inserted");
//         assert_eq!(
//             storage_with_chars_table
//                 .update_all(
//                     default_schema_name,
//                     "table_name",
//                     vec![
//                         ("column_c".to_owned(), "12345678901".to_owned()),
//                         ("column_vc".to_owned(), "100".to_owned())
//                     ]
//                 )
//                 .expect("no system errors"),
//             Err(OperationOnTableError::ConstraintViolations(
//                 vec![(
//                     ConstraintError::ValueTooLong(10),
//                     column_definition("column_c", SqlType::Char(10))
//                 )],
//                 1
//             ))
//         );
//     }
//
//     #[rstest::rstest]
//     fn multiple_columns_violation(default_schema_name: &str, mut storage_with_ints_table: PersistentStorage) {
//         storage_with_ints_table
//             .insert_into(
//                 default_schema_name,
//                 "table_name",
//                 vec![],
//                 vec![vec!["100".to_owned(), "100".to_owned(), "100".to_owned()]],
//             )
//             .expect("no system errors")
//             .expect("records inserted");
//
//         assert_eq!(
//             storage_with_ints_table
//                 .update_all(
//                     default_schema_name,
//                     "table_name",
//                     vec![
//                         ("column_si".to_owned(), "-32769".to_owned()),
//                         ("column_i".to_owned(), "-2147483649".to_owned()),
//                         ("column_bi".to_owned(), "100".to_owned())
//                     ]
//                 )
//                 .expect("no system errors"),
//             Err(OperationOnTableError::ConstraintViolations(
//                 vec![
//                     (
//                         ConstraintError::OutOfRange,
//                         column_definition("column_si", SqlType::SmallInt(i16::min_value()))
//                     ),
//                     (
//                         ConstraintError::OutOfRange,
//                         column_definition("column_i", SqlType::Integer(i32::min_value()))
//                     )
//                 ],
//                 1
//             ))
//         )
//     }
// }
//  */
