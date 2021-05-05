// Copyright 2020 - 2021 Alex Dukhno
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
fn delete_all_records(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        Ok(QueryEvent::TableCreated),
    );

    assert_query(
        &txn,
        "insert into schema_name.table_name values (123), (456);",
        Ok(QueryExecutionResult::Inserted(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name",
        Ok(QueryExecutionResult::Selected((
            vec![("column_test".to_owned(), SMALLINT)],
            vec![vec![small_int(123)], vec![small_int(456)]],
        ))),
    );

    assert_query(
        &txn,
        "delete from schema_name.table_name;",
        Ok(QueryExecutionResult::Deleted(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name",
        Ok(QueryExecutionResult::Selected((
            vec![("column_test".to_owned(), SMALLINT)],
            vec![],
        ))),
    );

    txn.commit();
}

#[rstest::rstest]
fn delete_value_by_predicate_on_single_field(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);",
        Ok(QueryEvent::TableCreated),
    );

    assert_query(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);",
        Ok(QueryExecutionResult::Inserted(3)),
    );
    assert_query(
        &txn,
        "delete from schema_name.table_name where col2 = 5;",
        Ok(QueryExecutionResult::Deleted(1)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(1), small_int(2), small_int(3)],
                vec![small_int(7), small_int(8), small_int(9)],
            ],
        ))),
    );
}
