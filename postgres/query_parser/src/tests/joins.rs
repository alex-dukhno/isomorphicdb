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

#[ignore]
#[test]
fn inner_join() {
    let statements = QUERY_PARSER.parse("select t_1_col_2, t_1_col_3, t2_col_2, t2_col_3 from schema_1.table1 join schema_1.table2 on t_1_col_1 = t_2_col2");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Select(SelectStatement {
            projection_items: vec![
                SelectItem::UnnamedExpr(Expr::Column("t_1_col_2".to_owned())),
                SelectItem::UnnamedExpr(Expr::Column("t_1_col_3".to_owned())),
                SelectItem::UnnamedExpr(Expr::Column("t_2_col_2".to_owned())),
                SelectItem::UnnamedExpr(Expr::Column("t_2_col_3".to_owned()))
            ],
            relations: Some(vec![Relation {
                schema: "schema_name".to_owned(),
                table: "table_name".to_owned()
            }]),
            where_clause: None,
        })))])
    );
}
