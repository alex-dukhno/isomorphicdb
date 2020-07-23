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

// /* this will not happen anymore, these error will be caught by the QueryProcessor and
//    should not be expected to be caught later on.
//
// #[rstest::rstest]
// fn insert_into_non_existent_schema(mut storage: PersistentStorage) {
//     assert_eq!(
//         storage
//             .insert_into("non_existent", "not_existed", vec![vec!["123".to_owned()]])
//             .expect("no system errors"),
//         Err(OperationOnTableError::SchemaDoesNotExist)
//     );
// }
//
// #[rstest::rstest]
// fn insert_into_non_existent_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     assert_eq!(
//         storage_with_schema
//             .insert_into(default_schema_name, "not_existed", vec![], vec![vec!["123".to_owned()]])
//             .expect("no system errors"),
//         Err(OperationOnTableError::TableDoesNotExist)
//     );
// }
//  */
//
// #[rstest::rstest]
// #[ignore]
// fn insert_many_rows_into_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
//     );
//     let row1 = vec![Datum::from_i16(123)];
//     let row2 = vec![Datum::from_i16(456)];
//
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         row1.clone(),
//     );
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         row2.clone(),
//     );
//
//     let table_columns = storage_with_schema
//         .table_columns(default_schema_name, "table_name")
//         .expect("no system errors")
//         .into_iter()
//         .map(|column_definition| column_definition.name())
//         .collect();
//
//     assert_eq!(
//         storage_with_schema
//             .select_all_from(default_schema_name, "table_name", table_columns)
//             .expect("no system errors"),
//         Ok((
//             vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
//             vec![
//                 Row::pack(row1.as_slice()).to_bytes(),
//                 Row::pack(row2.as_slice()).to_bytes()
//             ]
//         ))
//     );
// }
//
// #[rstest::rstest]
// #[ignore]
// fn insert_multiple_values_rows(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![
//             column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//             column_definition("column_2", SqlType::SmallInt(i16::min_value())),
//             column_definition("column_3", SqlType::SmallInt(i16::min_value())),
//         ],
//     );
//     let row1 = vec![Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16(3)];
//     let row2 = vec![Datum::from_i16(4), Datum::from_i16(5), Datum::from_i16(6)];
//     let row3 = vec![Datum::from_i16(7), Datum::from_i16(8), Datum::from_i16(9)];
//
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         row1.clone(),
//     );
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         row2.clone(),
//     );
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         row3.clone(),
//     );
//
//     let table_columns = storage_with_schema
//         .table_columns(default_schema_name, "table_name")
//         .expect("no system errors")
//         .into_iter()
//         .map(|column_definition| column_definition.name())
//         .collect();
//
//     assert_eq!(
//         storage_with_schema
//             .select_all_from(default_schema_name, "table_name", table_columns)
//             .expect("no system errors"),
//         Ok((
//             vec![
//                 column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//                 column_definition("column_2", SqlType::SmallInt(i16::min_value())),
//                 column_definition("column_3", SqlType::SmallInt(i16::min_value()))
//             ],
//             vec![
//                 Row::pack(&row1).to_bytes(),
//                 Row::pack(&row2).to_bytes(),
//                 Row::pack(&row3).to_bytes(),
//             ],
//         ))
//     );
// }
//
// /* named columns and associated errors are handled by the QueryProcessor
// #[rstest::rstest]
// fn insert_named_columns(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![
//             column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//             column_definition("column_2", SqlType::Char(10)),
//             column_definition("column_3", SqlType::BigInt(i64::min_value())),
//         ],
//     );
//
//     let columns = vec!["column_3", "column_2", "column_1"];
//
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         columns.clone(),
//         vec!["1", "2", "3"],
//     );
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         columns.clone(),
//         vec!["4", "5", "6"],
//     );
//     insert_into(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         columns.clone(),
//         vec!["7", "8", "9"],
//     );
//
//     let table_columns = storage_with_schema
//         .table_columns(default_schema_name, "table_name")
//         .expect("no system errors")
//         .into_iter()
//         .map(|column_definition| column_definition.name())
//         .collect();
//
//     assert_eq!(
//         storage_with_schema
//             .select_all_from(default_schema_name, "table_name", table_columns)
//             .expect("no system errors"),
//         Ok((
//             vec![
//                 column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//                 column_definition("column_2", SqlType::Char(10)),
//                 column_definition("column_3", SqlType::BigInt(i64::min_value()))
//             ],
//             vec![
//                 vec!["3".to_owned(), "2".to_owned(), "1".to_owned()],
//                 vec!["6".to_owned(), "5".to_owned(), "4".to_owned()],
//                 vec!["9".to_owned(), "8".to_owned(), "7".to_owned()],
//             ],
//         ))
//     );
// }
//
// #[rstest::rstest]
// fn insert_named_not_existed_column(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![
//             column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//             column_definition("column_2", SqlType::Char(10)),
//             column_definition("column_3", SqlType::BigInt(i64::min_value())),
//         ],
//     );
//
//     let columns = vec![
//         "column_3".to_owned(),
//         "column_2".to_owned(),
//         "column_1".to_owned(),
//         "not_existed".to_owned(),
//     ];
//
//     assert_eq!(
//         storage_with_schema
//             .insert_into(
//                 default_schema_name,
//                 "table_name",
//                 columns,
//                 vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned(), "4".to_owned()]],
//             )
//             .expect("no system errors"),
//         Err(OperationOnTableError::ColumnDoesNotExist(
//             vec!["not_existed".to_owned()]
//         ))
//     )
// }
//  */
//
// #[rstest::rstest]
// #[ignore]
// fn insert_row_into_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
//     );
//
//     let row = Row::pack(&[Datum::from_i16(123)]).to_bytes();
//
//     assert_eq!(
//         storage_with_schema
//             .insert_into(default_schema_name, "table_name", vec![row.clone()])
//             .expect("no system errors"),
//         Ok(())
//     );
//
//     let table_columns = storage_with_schema
//         .table_columns(default_schema_name, "table_name")
//         .expect("no system errors")
//         .into_iter()
//         .map(|column_definition| column_definition.name())
//         .collect();
//
//     assert_eq!(
//         storage_with_schema
//             .select_all_from(default_schema_name, "table_name", table_columns)
//             .expect("no system errors"),
//         Ok((
//             vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
//             vec![row]
//         ))
//     );
// }
//
// /* this will be handled by the QueryProcessor.
// #[rstest::rstest]
// fn insert_too_many_expressions(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![
//             column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//             column_definition("column_2", SqlType::Char(10)),
//             column_definition("column_3", SqlType::BigInt(i64::min_value())),
//         ],
//     );
//
//     let columns = vec![];
//
//     let row = Row::pack(&[Datum::from_i16(1), Datum::String("2"), "3".to_owned(), "4".to_owned()]
//
//     assert_eq!(
//         storage_with_schema
//             .insert_into(
//                 default_schema_name,
//                 "table_name",
//                 columns,
//                 vec![],
//             )
//             .expect("no system errors"),
//         Err(OperationOnTableError::InsertTooManyExpressions)
//     );
//
//     let table_columns = storage_with_schema
//         .table_columns(default_schema_name, "table_name")
//         .expect("no system errors")
//         .into_iter()
//         .map(|column_definition| column_definition.name())
//         .collect();
//
//     assert_eq!(
//         storage_with_schema
//             .select_all_from(default_schema_name, "table_name", table_columns)
//             .expect("no system errors"),
//         Ok((
//             vec![
//                 column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//                 column_definition("column_2", SqlType::Char(10)),
//                 column_definition("column_3", SqlType::BigInt(i64::min_value())),
//             ],
//             vec![]
//         ))
//     );
// }
//
// #[rstest::rstest]
// fn insert_too_many_expressions_labeled(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
//     create_table(
//         &mut storage_with_schema,
//         default_schema_name,
//         "table_name",
//         vec![
//             column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//             column_definition("column_2", SqlType::Char(10)),
//             column_definition("column_3", SqlType::BigInt(i64::min_value())),
//         ],
//     );
//
//     let columns = vec!["column_3".to_owned(), "column_2".to_owned(), "column_1".to_owned()];
//
//     assert_eq!(
//         storage_with_schema
//             .insert_into(
//                 default_schema_name,
//                 "table_name",
//                 columns,
//                 vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned(), "4".to_owned()]],
//             )
//             .expect("no system errors"),
//         Err(OperationOnTableError::InsertTooManyExpressions)
//     );
//
//     let table_columns = storage_with_schema
//         .table_columns(default_schema_name, "table_name")
//         .expect("no system errors")
//         .into_iter()
//         .map(|column_definition| column_definition.name())
//         .collect();
//
//     assert_eq!(
//         storage_with_schema
//             .select_all_from(default_schema_name, "table_name", table_columns)
//             .expect("no system errors"),
//         Ok((
//             vec![
//                 column_definition("column_1", SqlType::SmallInt(i16::min_value())),
//                 column_definition("column_2", SqlType::Char(10)),
//                 column_definition("column_3", SqlType::BigInt(i64::min_value())),
//             ],
//             vec![]
//         ))
//     );
// }
//
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
//         assert_eq!(
//             storage_with_ints_table
//                 .insert_into(
//                     default_schema_name,
//                     "table_name",
//                     vec![],
//                     vec![vec!["-32769".to_owned(), "100".to_owned(), "100".to_owned()]],
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
//         assert_eq!(
//             storage_with_ints_table
//                 .insert_into(
//                     default_schema_name,
//                     "table_name",
//                     vec![],
//                     vec![vec!["abc".to_owned(), "100".to_owned(), "100".to_owned()]],
//                 )
//                 .expect("no system errors"),
//             Err(OperationOnTableError::ConstraintViolations(
//                 vec![(
//                     ConstraintError::TypeMismatch("abc".to_owned()),
//                     column_definition("column_si", SqlType::SmallInt(i16::min_value()))
//                 )],
//                 1
//             ))
//         )
//     }
//
//     #[rstest::rstest]
//     fn value_too_long_violation(default_schema_name: &str, mut storage_with_chars_table: PersistentStorage) {
//         assert_eq!(
//             storage_with_chars_table
//                 .insert_into(
//                     default_schema_name,
//                     "table_name",
//                     vec![],
//                     vec![vec!["12345678901".to_owned(), "100".to_owned()]],
//                 )
//                 .expect("no system errors"),
//             Err(OperationOnTableError::ConstraintViolations(
//                 vec![(
//                     ConstraintError::ValueTooLong(10),
//                     column_definition("column_c", SqlType::Char(10))
//                 )],
//                 1
//             ))
//         )
//     }
//
//     #[rstest::rstest]
//     fn multiple_columns_single_row_violation(
//         default_schema_name: &str,
//         mut storage_with_ints_table: PersistentStorage,
//     ) {
//         assert_eq!(
//             storage_with_ints_table
//                 .insert_into(
//                     default_schema_name,
//                     "table_name",
//                     vec![],
//                     vec![vec!["-32769".to_owned(), "-2147483649".to_owned(), "100".to_owned()]],
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
//
//     #[rstest::rstest]
//     fn multiple_columns_multiple_row_violation(
//         default_schema_name: &str,
//         mut storage_with_ints_table: PersistentStorage,
//     ) {
//         assert_eq!(
//             storage_with_ints_table
//                 .insert_into(
//                     default_schema_name,
//                     "table_name",
//                     vec![],
//                     vec![
//                         vec!["-32769".to_owned(), "-2147483649".to_owned(), "100".to_owned()],
//                         vec![
//                             "100".to_owned(),
//                             "-2147483649".to_owned(),
//                             "-9223372036854775809".to_owned()
//                         ],
//                     ],
//                 )
//                 .expect("no system errors"),
//             // we should only get the errors from the first row.
//             Err(OperationOnTableError::ConstraintViolations(
//                 vec![
//                     (
//                         ConstraintError::OutOfRange,
//                         column_definition("column_si", SqlType::SmallInt(i16::min_value()))
//                     ),
//                     (
//                         ConstraintError::OutOfRange,
//                         column_definition("column_i", SqlType::Integer(i32::min_value()))
//                     ),
//                 ],
//                 1
//             ))
//         )
//     }
// }
//  */
