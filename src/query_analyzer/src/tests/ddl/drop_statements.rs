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

fn inner_drop(
    names: Vec<Vec<&'static str>>,
    object_type: sql_ast::ObjectType,
    if_exists: bool,
    cascade: bool,
) -> sql_ast::Statement {
    sql_ast::Statement::Drop {
        object_type,
        if_exists,
        names: names
            .into_iter()
            .map(|name| sql_ast::ObjectName(name.into_iter().map(ident).collect()))
            .collect(),
        cascade,
    }
}

fn drop_statement(names: Vec<Vec<&'static str>>, object_type: sql_ast::ObjectType) -> sql_ast::Statement {
    inner_drop(names, object_type, false, false)
}

fn drop_if_exists(names: Vec<Vec<&'static str>>, object_type: sql_ast::ObjectType) -> sql_ast::Statement {
    inner_drop(names, object_type, true, false)
}

fn drop_cascade(names: Vec<Vec<&'static str>>, object_type: sql_ast::ObjectType) -> sql_ast::Statement {
    inner_drop(names, object_type, false, true)
}

#[cfg(test)]
mod schema {
    use super::*;

    const SCHEMA_TYPE: sql_ast::ObjectType = sql_ast::ObjectType::Schema;

    #[test]
    fn drop_non_existent_schema() {
        let analyzer = Analyzer::new(InMemoryDatabase::new());
        assert_eq!(
            analyzer.analyze(drop_statement(vec![vec!["non_existent"]], SCHEMA_TYPE)),
            Ok(QueryAnalysis::DDL(SchemaChange::DropSchemas(DropSchemasQuery {
                schema_names: vec![SchemaName::from(&"non_existent")],
                cascade: false,
                if_exists: false,
            })))
        );
    }

    #[test]
    fn drop_schema_with_unqualified_name() {
        let analyzer = Analyzer::new(InMemoryDatabase::new());
        assert_eq!(
            analyzer.analyze(drop_statement(
                vec![vec!["first_part", "second_part", "third_part", "fourth_part"]],
                SCHEMA_TYPE,
            )),
            Err(AnalysisError::schema_naming_error(
                &"Only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'"
            ))
        );
    }

    #[test]
    fn drop_schema() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(drop_statement(vec![vec![SCHEMA]], SCHEMA_TYPE)),
            Ok(QueryAnalysis::DDL(SchemaChange::DropSchemas(DropSchemasQuery {
                schema_names: vec![SchemaName::from(&SCHEMA)],
                cascade: false,
                if_exists: false,
            })))
        );
    }

    #[test]
    fn drop_schema_cascade() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(drop_cascade(vec![vec![SCHEMA]], SCHEMA_TYPE)),
            Ok(QueryAnalysis::DDL(SchemaChange::DropSchemas(DropSchemasQuery {
                schema_names: vec![SchemaName::from(&SCHEMA)],
                cascade: true,
                if_exists: false,
            })))
        );
    }

    #[test]
    fn drop_schema_if_exists() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(drop_if_exists(vec![vec![SCHEMA], vec!["schema_1"]], SCHEMA_TYPE)),
            Ok(QueryAnalysis::DDL(SchemaChange::DropSchemas(DropSchemasQuery {
                schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&"schema_1")],
                cascade: false,
                if_exists: true,
            })))
        );
    }
}

#[cfg(test)]
mod table {
    use super::*;

    const TABLE_TYPE: sql_ast::ObjectType = sql_ast::ObjectType::Table;

    #[test]
    fn drop_table_from_nonexistent_schema() {
        let analyzer = Analyzer::new(InMemoryDatabase::new());
        assert_eq!(
            analyzer.analyze(drop_statement(vec![vec!["non_existent_schema", TABLE]], TABLE_TYPE)),
            Err(AnalysisError::schema_does_not_exist(&"non_existent_schema"))
        );
    }

    #[test]
    fn drop_table_with_unqualified_name() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        let analyzer = Analyzer::new(database);
        assert_eq!(
            analyzer.analyze(drop_statement(vec![vec!["only_table_in_the_name"]], TABLE_TYPE)),
            Ok(QueryAnalysis::DDL(SchemaChange::DropTables(DropTablesQuery {
                full_table_names: vec![FullTableName::from("only_table_in_the_name")],
                cascade: false,
                if_exists: false
            })))
        );
    }

    #[test]
    fn drop_table_with_unsupported_name() {
        let analyzer = Analyzer::new(InMemoryDatabase::new());
        assert_eq!(
            analyzer.analyze(drop_statement(
                vec![vec!["first_part", "second_part", "third_part", "fourth_part"]],
                TABLE_TYPE,
            )),
            Err(AnalysisError::table_naming_error(
                &"Unable to process table name 'first_part.second_part.third_part.fourth_part'",
            ))
        );
    }

    #[test]
    fn drop_table() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = Analyzer::new(database);
        assert_eq!(
            analyzer.analyze(drop_statement(vec![vec![SCHEMA, TABLE]], TABLE_TYPE)),
            Ok(QueryAnalysis::DDL(SchemaChange::DropTables(DropTablesQuery {
                full_table_names: vec![FullTableName::from((&SCHEMA, &TABLE))],
                cascade: false,
                if_exists: false
            })))
        );
    }

    #[test]
    fn drop_nonexistent_table() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        let analyzer = Analyzer::new(database);
        assert_eq!(
            analyzer.analyze(drop_statement(vec![vec![SCHEMA, "non_existent_table"]], TABLE_TYPE)),
            Ok(QueryAnalysis::DDL(SchemaChange::DropTables(DropTablesQuery {
                full_table_names: vec![FullTableName::from((&SCHEMA, &"non_existent_table"))],
                cascade: false,
                if_exists: false
            })))
        );
    }

    #[test]
    fn drop_table_if_exists() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        database
            .execute(create_table_ops(SCHEMA, "table_1", vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = Analyzer::new(database);
        assert_eq!(
            analyzer.analyze(drop_if_exists(
                vec![vec![SCHEMA, TABLE], vec![SCHEMA, "table_1"]],
                TABLE_TYPE,
            )),
            Ok(QueryAnalysis::DDL(SchemaChange::DropTables(DropTablesQuery {
                full_table_names: vec![
                    FullTableName::from((&SCHEMA, &TABLE)),
                    FullTableName::from((&SCHEMA, &"table_1"))
                ],
                cascade: false,
                if_exists: true
            })))
        );
    }

    #[test]
    fn drop_table_cascade() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        database
            .execute(create_table_ops(SCHEMA, "table_1", vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = Analyzer::new(database);
        assert_eq!(
            analyzer.analyze(drop_cascade(
                vec![vec![SCHEMA, TABLE], vec![SCHEMA, "table_1"]],
                TABLE_TYPE,
            )),
            Ok(QueryAnalysis::DDL(SchemaChange::DropTables(DropTablesQuery {
                full_table_names: vec![
                    FullTableName::from((&SCHEMA, &TABLE)),
                    FullTableName::from((&SCHEMA, &"table_1"))
                ],
                cascade: true,
                if_exists: false
            })))
        );
    }
}
