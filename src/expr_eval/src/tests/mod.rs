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

#[cfg(test)]
mod dynamic_expressions;
#[cfg(test)]
mod static_expressions;

use ast::operations::BinaryOp;
use ast::operations::ScalarOp;
use ast::values::ScalarValue;
use ast::Datum;
use bigdecimal::BigDecimal;
use protocol::results::QueryError;
use protocol::results::QueryResult;
use protocol::Sender;
use std::io;
use std::sync::{Arc, Mutex};

struct Collector(Mutex<Vec<QueryResult>>);

impl Sender for Collector {
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn send(&self, query_result: QueryResult) -> io::Result<()> {
        self.0.lock().expect("locked").push(query_result);
        Ok(())
    }
}

impl Collector {
    fn assert_content(&self, expected: Vec<QueryResult>) {
        let result = self.0.lock().expect("locked");
        assert_eq!(&*result, &expected)
    }
}

type ResultCollector = Arc<Collector>;

fn sender() -> ResultCollector {
    Arc::new(Collector(Mutex::new(vec![])))
}
