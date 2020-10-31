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

use pg_model::pg_types::PgType;
use sqlparser::ast::DataType;
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
};

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum SqlType {
    Bool,
    Char(u64),
    VarChar(u64),
    SmallInt,
    Integer,
    BigInt,
    Real,
    DoublePrecision,
}

impl TryFrom<&DataType> for SqlType {
    type Error = NotSupportedType;

    fn try_from(data_type: &DataType) -> Result<Self, Self::Error> {
        match data_type {
            DataType::SmallInt => Ok(SqlType::SmallInt),
            DataType::Int => Ok(SqlType::Integer),
            DataType::BigInt => Ok(SqlType::BigInt),
            DataType::Char(len) => Ok(SqlType::Char(len.unwrap_or(255))),
            DataType::Varchar(len) => Ok(SqlType::VarChar(len.unwrap_or(255))),
            DataType::Boolean => Ok(SqlType::Bool),
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
            SqlType::SmallInt => write!(f, "smallint"),
            SqlType::Integer => write!(f, "integer"),
            SqlType::BigInt => write!(f, "bigint"),
            SqlType::Real => write!(f, "real"),
            SqlType::DoublePrecision => write!(f, "double precision"),
        }
    }
}

impl Into<PgType> for &SqlType {
    fn into(self) -> PgType {
        match self {
            SqlType::Bool => PgType::Bool,
            SqlType::Char(_) => PgType::Char,
            SqlType::VarChar(_) => PgType::VarChar,
            SqlType::SmallInt => PgType::SmallInt,
            SqlType::Integer => PgType::Integer,
            SqlType::BigInt => PgType::BigInt,
            SqlType::Real => PgType::Real,
            SqlType::DoublePrecision => PgType::DoublePrecision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod to_postgresql_type_conversion {
        use super::*;

        #[test]
        fn boolean() {
            let pg_type: PgType = (&SqlType::Bool).into();
            assert_eq!(pg_type, PgType::Bool);
        }

        #[test]
        fn small_int() {
            let pg_type: PgType = (&SqlType::SmallInt).into();
            assert_eq!(pg_type, PgType::SmallInt);
        }

        #[test]
        fn integer() {
            let pg_type: PgType = (&SqlType::Integer).into();
            assert_eq!(pg_type, PgType::Integer);
        }

        #[test]
        fn big_int() {
            let pg_type: PgType = (&SqlType::BigInt).into();
            assert_eq!(pg_type, PgType::BigInt);
        }

        #[test]
        fn char() {
            let pg_type: PgType = (&SqlType::Char(0)).into();
            assert_eq!(pg_type, PgType::Char);
        }

        #[test]
        fn var_char() {
            let pg_type: PgType = (&SqlType::VarChar(0)).into();
            assert_eq!(pg_type, PgType::VarChar);
        }

        #[test]
        fn real() {
            let pg_type: PgType = (&SqlType::Real).into();
            assert_eq!(pg_type, PgType::Real);
        }

        #[test]
        fn double_precision() {
            let pg_type: PgType = (&SqlType::DoublePrecision).into();
            assert_eq!(pg_type, PgType::DoublePrecision);
        }
    }
}
