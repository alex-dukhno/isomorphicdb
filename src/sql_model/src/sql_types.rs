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

use std::convert::TryFrom;

use protocol::pgsql_types::PostgreSqlType;
use sqlparser::ast::DataType;
use std::fmt::{self, Display, Formatter};

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum SqlType {
    Bool,
    Char(u64),
    VarChar(u64),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real,
    DoublePrecision,
}

impl TryFrom<&DataType> for SqlType {
    type Error = NotSupportedType;

    fn try_from(data_type: &DataType) -> Result<Self, Self::Error> {
        match data_type {
            DataType::SmallInt => Ok(SqlType::SmallInt(i16::min_value())),
            DataType::Int => Ok(SqlType::Integer(i32::min_value())),
            DataType::BigInt => Ok(SqlType::BigInt(i64::min_value())),
            DataType::Char(len) => Ok(SqlType::Char(len.unwrap_or(255))),
            DataType::Varchar(len) => Ok(SqlType::VarChar(len.unwrap_or(255))),
            DataType::Boolean => Ok(SqlType::Bool),
            DataType::Custom(name) => {
                let name = name.to_string();
                match name.as_str() {
                    "serial" => Ok(SqlType::Integer(1)),
                    "smallserial" => Ok(SqlType::SmallInt(1)),
                    "bigserial" => Ok(SqlType::BigInt(1)),
                    _other_type => Err(NotSupportedType(data_type.clone())),
                }
            }
            other_type => Err(NotSupportedType(other_type.clone())),
        }
    }
}

pub struct NotSupportedType(DataType);

impl Display for NotSupportedType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "'{}' type is not supported", self.0)
    }
}

impl Display for SqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SqlType::Bool => write!(f, "bool"),
            SqlType::Char(len) => write!(f, "char({})", len),
            SqlType::VarChar(len) => write!(f, "varchar({})", len),
            SqlType::SmallInt(_) => write!(f, "smallint"),
            SqlType::Integer(_) => write!(f, "integer"),
            SqlType::BigInt(_) => write!(f, "bigint"),
            SqlType::Real => write!(f, "real"),
            SqlType::DoublePrecision => write!(f, "double precision"),
        }
    }
}

impl Into<PostgreSqlType> for &SqlType {
    fn into(self) -> PostgreSqlType {
        match self {
            SqlType::Bool => PostgreSqlType::Bool,
            SqlType::Char(_) => PostgreSqlType::Char,
            SqlType::VarChar(_) => PostgreSqlType::VarChar,
            SqlType::SmallInt(_) => PostgreSqlType::SmallInt,
            SqlType::Integer(_) => PostgreSqlType::Integer,
            SqlType::BigInt(_) => PostgreSqlType::BigInt,
            SqlType::Real => PostgreSqlType::Real,
            SqlType::DoublePrecision => PostgreSqlType::DoublePrecision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod to_postgresql_type_conversion {
        use protocol::pgsql_types::PostgreSqlType;

        use super::*;

        #[test]
        fn boolean() {
            let pg_type: PostgreSqlType = (&SqlType::Bool).into();
            assert_eq!(pg_type, PostgreSqlType::Bool);
        }

        #[test]
        fn small_int() {
            let pg_type: PostgreSqlType = (&SqlType::SmallInt(i16::min_value())).into();
            assert_eq!(pg_type, PostgreSqlType::SmallInt);
        }

        #[test]
        fn integer() {
            let pg_type: PostgreSqlType = (&SqlType::Integer(i32::min_value())).into();
            assert_eq!(pg_type, PostgreSqlType::Integer);
        }

        #[test]
        fn big_int() {
            let pg_type: PostgreSqlType = (&SqlType::BigInt(i64::min_value())).into();
            assert_eq!(pg_type, PostgreSqlType::BigInt);
        }

        #[test]
        fn char() {
            let pg_type: PostgreSqlType = (&SqlType::Char(0)).into();
            assert_eq!(pg_type, PostgreSqlType::Char);
        }

        #[test]
        fn var_char() {
            let pg_type: PostgreSqlType = (&SqlType::VarChar(0)).into();
            assert_eq!(pg_type, PostgreSqlType::VarChar);
        }

        #[test]
        fn real() {
            let pg_type: PostgreSqlType = (&SqlType::Real).into();
            assert_eq!(pg_type, PostgreSqlType::Real);
        }

        #[test]
        fn double_precision() {
            let pg_type: PostgreSqlType = (&SqlType::DoublePrecision).into();
            assert_eq!(pg_type, PostgreSqlType::DoublePrecision);
        }
    }
}
