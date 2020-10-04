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

use data_manager::{DataManager, MetadataView};
use description::{Description, FullTableName, InsertStatement, TableId};
use sqlparser::ast::Statement;
use std::convert::TryFrom;
use std::sync::Arc;
use storage::Database;

pub struct Analyzer<D: Database> {
    metadata: Arc<DataManager<D>>,
}

impl<D: Database> Analyzer<D> {
    pub fn new(metadata: Arc<DataManager<D>>) -> Analyzer<D> {
        Analyzer { metadata }
    }

    pub fn describe(&self, statement: &Statement) -> Description {
        match statement {
            Statement::Insert { table_name, .. } => {
                let full_table_name = FullTableName::try_from(table_name).unwrap();
                let (schema_name, table_name) = full_table_name.as_tuple();
                match self.metadata.table_exists(schema_name, table_name) {
                    Some((schema_id, Some(table_id))) => Description::Insert(InsertStatement {
                        table_id: TableId::from((schema_id, table_id)),
                    }),
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use description::TableId;
    use sqlparser::ast::{Ident, ObjectName, Query, SetExpr, Values};
    use std::sync::Arc;

    const SCHEMA: &str = "schema_name";
    const TABLE: &str = "table_name";

    fn ident<S: ToString>(name: S) -> Ident {
        Ident {
            value: name.to_string(),
            quote_style: None,
        }
    }

    #[test]
    fn insert_into_existing_empty_table() {
        let data_manager = Arc::new(DataManager::default());
        let schema_id = data_manager.create_schema(SCHEMA).expect("ok");
        data_manager.create_table(schema_id, TABLE, &[]).expect("ok");
        let analyzer = Analyzer::new(data_manager);
        let description = analyzer.describe(&Statement::Insert {
            table_name: ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            columns: vec![],
            source: Box::new(Query {
                ctes: vec![],
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            }),
        });

        assert_eq!(
            description,
            Description::Insert(InsertStatement {
                table_id: TableId::from((0, 0))
            })
        );
    }
}
