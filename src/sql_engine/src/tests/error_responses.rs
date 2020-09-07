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
fn parse_wrong_select_syntax(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine
        .execute("selec col from schema_name.table_name")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Err(QueryError::syntax_error(
            "\"selec col from schema_name.table_name\" can\'t be parsed",
        )),
        Ok(QueryEvent::QueryComplete),
    ]);
}
