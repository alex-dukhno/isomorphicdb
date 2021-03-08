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

#[cfg(test)]
mod delete;
#[cfg(test)]
mod extended;
#[cfg(test)]
mod index;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod select;
#[cfg(test)]
mod table;
#[cfg(test)]
mod update;

const QUERY_PARSER: QueryParser = QueryParser::new();

#[test]
fn set_variable() {
    let statements = QUERY_PARSER.parse("set variable=value;");

    assert_eq!(
        statements,
        Ok(vec![Statement::Config(Set {
            variable: "variable".to_owned(),
            value: "value".to_owned()
        })])
    );
}
