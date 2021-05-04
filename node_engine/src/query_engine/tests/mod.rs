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
use bigdecimal::BigDecimal;
use data_manipulation::QueryExecutionResult;
use data_repr::scalar::ScalarValue;
use postgre_sql::{
    query_response::QueryEvent,
    wire_protocol::payload::{BIGINT, CHAR, INT, SMALLINT, VARCHAR},
};
use types::SqlTypeFamily;

#[cfg(test)]
mod delete;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod select;
#[cfg(test)]
mod table;
#[cfg(test)]
mod type_constraints;
#[cfg(test)]
mod update;

#[allow(dead_code)]
fn setup_logger() {
    if let Ok(()) = simple_logger::SimpleLogger::new().init() {};
}

fn small_int(value: i16) -> ScalarValue {
    ScalarValue::Num {
        value: BigDecimal::from(value),
        type_family: SqlTypeFamily::SmallInt,
    }
}

fn integer(value: i32) -> ScalarValue {
    ScalarValue::Num {
        value: BigDecimal::from(value),
        type_family: SqlTypeFamily::Integer,
    }
}

fn big_int(value: i64) -> ScalarValue {
    ScalarValue::Num {
        value: BigDecimal::from(value),
        type_family: SqlTypeFamily::BigInt,
    }
}

fn string(value: &str) -> ScalarValue {
    ScalarValue::String(value.to_owned())
}

fn assert_definition(txn: &TransactionContext, sql: &str, expected: Result<QueryEvent, QueryError>) {
    match txn.parse(sql).expect("query parsed").pop() {
        Some(Statement::Definition(definition)) => {
            assert_eq!(txn.execute_ddl(definition), expected);
        }
        other => panic!("expected DDL query but was {:?}", other),
    }
}

fn assert_query(txn: &TransactionContext, sql: &str, expected: Result<QueryExecutionResult, QueryError>) {
    match txn.parse(sql).expect("query parsed").pop() {
        Some(Statement::Query(query)) => {
            let query_result = txn
                .process(query, vec![])
                .map(|typed_query| txn.plan(typed_query))
                .and_then(|plan| Ok(plan.execute(vec![])?));
            assert_eq!(query_result, expected);
        }
        other => panic!("expected Statement::Query but was {:?}", other),
    }
}

#[rstest::fixture]
fn with_schema() -> QueryEngine {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_definition(&txn, "create schema schema_name", Ok(QueryEvent::SchemaCreated));
    txn.commit();
    query_engine
}
