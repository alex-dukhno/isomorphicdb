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

use data_manipulation_query_plan::Write;
use catalog::Database;
use std::sync::Arc;

#[derive(Clone)]
pub struct Executor<D: Database> {
    database: Arc<D>,
}

impl<D: Database> Executor<D> {
    pub fn new(database: Arc<D>) -> Executor<D> {
        Executor { database }
    }

    pub fn execute(&self, _write_query: Write) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_manipulation_query_plan::InsertQuery;
    use catalog::InMemoryDatabase;
    use definition::FullTableName;

    #[test]
    fn it_works() {
        let executor = Executor::new(InMemoryDatabase::new());

        executor.execute(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&"schema", &"table")),
            column_types: vec![],
            values: vec![],
        }))
    }
}
