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

#[test]
fn boolean() {
    let oid: u32 = (&SqlType::bool()).into();
    assert_eq!(oid, BOOL);
}

#[test]
fn small_int() {
    let oid: u32 = (&SqlType::small_int()).into();
    assert_eq!(oid, SMALLINT);
}

#[test]
fn integer() {
    let oid: u32 = (&SqlType::integer()).into();
    assert_eq!(oid, INT);
}

#[test]
fn big_int() {
    let oid: u32 = (&SqlType::big_int()).into();
    assert_eq!(oid, BIGINT);
}

#[test]
fn char() {
    let oid: u32 = (&SqlType::char(0)).into();
    assert_eq!(oid, CHAR);
}

#[test]
fn var_char() {
    let oid: u32 = (&SqlType::var_char(0)).into();
    assert_eq!(oid, VARCHAR);
}
