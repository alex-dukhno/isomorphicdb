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
fn true_values() {
    assert_eq!("t".parse(), Ok(Bool(true)));
    assert_eq!("TrUe".parse(), Ok(Bool(true)));
    assert_eq!("YeS".parse(), Ok(Bool(true)));
    assert_eq!("y".parse(), Ok(Bool(true)));
    assert_eq!("on".parse(), Ok(Bool(true)));
    assert_eq!("1".parse(), Ok(Bool(true)));
}

#[test]
fn false_values() {
    assert_eq!("f".parse(), Ok(Bool(false)));
    assert_eq!("FalSe".parse(), Ok(Bool(false)));
    assert_eq!("nO".parse(), Ok(Bool(false)));
    assert_eq!("N".parse(), Ok(Bool(false)));
    assert_eq!("OfF".parse(), Ok(Bool(false)));
    assert_eq!("0".parse(), Ok(Bool(false)));
}

#[test]
fn not_a_boolean_value() {
    assert_eq!(Bool::from_str("not a boolean"), Err(ParseBoolError("not a boolean".to_lowercase())))
}
