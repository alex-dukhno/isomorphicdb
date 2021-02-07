// Copyright 2020 - present Alex Dukhno
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

#[test]
fn boolean() {
    let pg_type: PgType = (&SqlType::bool()).into();
    assert_eq!(pg_type, PgType::Bool);
}

#[test]
fn small_int() {
    let pg_type: PgType = (&SqlType::small_int()).into();
    assert_eq!(pg_type, PgType::SmallInt);
}

#[test]
fn integer() {
    let pg_type: PgType = (&SqlType::integer()).into();
    assert_eq!(pg_type, PgType::Integer);
}

#[test]
fn big_int() {
    let pg_type: PgType = (&SqlType::big_int()).into();
    assert_eq!(pg_type, PgType::BigInt);
}

#[test]
fn char() {
    let pg_type: PgType = (&SqlType::char(0)).into();
    assert_eq!(pg_type, PgType::Char);
}

#[test]
fn var_char() {
    let pg_type: PgType = (&SqlType::var_char(0)).into();
    assert_eq!(pg_type, PgType::VarChar);
}
