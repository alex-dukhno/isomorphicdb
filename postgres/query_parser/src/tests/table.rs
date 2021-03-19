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
fn create_ints_table() {
    let statements = QUERY_PARSER.parse("create table table_name (col_si smallint, col_i int, col_bi bigint);");

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::CreateTable {
            if_not_exists: false,
            schema_name: "public".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "col_si".to_owned(),
                    data_type: DataType::SmallInt,
                },
                ColumnDef {
                    name: "col_i".to_owned(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "col_bi".to_owned(),
                    data_type: DataType::BigInt,
                }
            ],
        })])
    );
}

#[test]
fn create_strings_table() {
    let statements = QUERY_PARSER.parse(
        "\
            create table schema_name.table_name (\
                col_c char,\
                col_cs char(255),\
                col_cl character,\
                col_cls character(255),\
                col_v varchar,\
                col_vs varchar(255),\
                col_vl character varying,\
                col_vls character varying(255)\
            );\
            ",
    );

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::CreateTable {
            if_not_exists: false,
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "col_c".to_owned(),
                    data_type: DataType::Char(1),
                },
                ColumnDef {
                    name: "col_cs".to_owned(),
                    data_type: DataType::Char(255),
                },
                ColumnDef {
                    name: "col_cl".to_owned(),
                    data_type: DataType::Char(1),
                },
                ColumnDef {
                    name: "col_cls".to_owned(),
                    data_type: DataType::Char(255),
                },
                ColumnDef {
                    name: "col_v".to_owned(),
                    data_type: DataType::VarChar(None),
                },
                ColumnDef {
                    name: "col_vs".to_owned(),
                    data_type: DataType::VarChar(Some(255)),
                },
                ColumnDef {
                    name: "col_vl".to_owned(),
                    data_type: DataType::VarChar(None),
                },
                ColumnDef {
                    name: "col_vls".to_owned(),
                    data_type: DataType::VarChar(Some(255)),
                }
            ],
        })])
    );
}

#[test]
fn create_float_table() {
    let statements = QUERY_PARSER.parse("create table table_name (col_r real, col_d double precision);");

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::CreateTable {
            if_not_exists: false,
            schema_name: "public".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "col_r".to_owned(),
                    data_type: DataType::Real,
                },
                ColumnDef {
                    name: "col_d".to_owned(),
                    data_type: DataType::Double,
                }
            ],
        })])
    );
}

#[test]
fn create_boolean_table() {
    let statements = QUERY_PARSER.parse("create table table_name (col_b boolean);");

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::CreateTable {
            if_not_exists: false,
            schema_name: "public".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![ColumnDef {
                name: "col_b".to_owned(),
                data_type: DataType::Bool,
            }],
        })])
    );
}

#[test]
fn drop_table() {
    let statements = QUERY_PARSER.parse("drop table table_name;");

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::DropTables {
            names: vec![("public".to_owned(), "table_name".to_owned())],
            if_exists: false,
            cascade: false
        })])
    );
}

#[test]
fn drop_tables() {
    let statements = QUERY_PARSER.parse("drop table table_name_1, table_name_2;");

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::DropTables {
            names: vec![
                ("public".to_owned(), "table_name_1".to_owned()),
                ("public".to_owned(), "table_name_2".to_owned())
            ],
            if_exists: false,
            cascade: false
        })])
    );
}

#[test]
fn drop_table_cascade() {
    let statements = QUERY_PARSER.parse("drop table table_name_1, table_name_2 cascade;");

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::DropTables {
            names: vec![
                ("public".to_owned(), "table_name_1".to_owned()),
                ("public".to_owned(), "table_name_2".to_owned())
            ],
            if_exists: false,
            cascade: true
        })])
    );
}

#[test]
fn drop_table_if_exists() {
    let statements = QUERY_PARSER.parse("drop table if exists table_name;");

    assert_eq!(
        statements,
        Ok(vec![Statement::DDL(Definition::DropTables {
            names: vec![("public".to_owned(), "table_name".to_owned())],
            if_exists: true,
            cascade: false
        })])
    );
}
