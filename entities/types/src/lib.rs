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

use pg_wire::PgType;
use sql_ast::DataType;
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
};

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum SqlType {
    Bool,
    Str { len: u64, kind: Str },
    Num(Num),
}

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum Num {
    SmallInt,
    Integer,
    BigInt,
    Real,
    DoublePrecision,
}

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum Str {
    Const,
    Var,
}

impl SqlType {
    pub fn small_int() -> SqlType {
        SqlType::Num(Num::SmallInt)
    }

    pub fn integer() -> SqlType {
        SqlType::Num(Num::Integer)
    }

    pub fn big_int() -> SqlType {
        SqlType::Num(Num::BigInt)
    }

    pub fn real() -> SqlType {
        SqlType::Num(Num::Real)
    }

    pub fn double_precision() -> SqlType {
        SqlType::Num(Num::DoublePrecision)
    }

    pub fn bool() -> SqlType {
        SqlType::Bool
    }

    pub fn char(len: u64) -> SqlType {
        SqlType::Str { len, kind: Str::Const }
    }

    pub fn var_char(len: u64) -> SqlType {
        SqlType::Str { len, kind: Str::Var }
    }

    pub fn type_id(&self) -> u64 {
        match self {
            SqlType::Bool => 0,
            SqlType::Str { kind: Str::Const, .. } => 1,
            SqlType::Str { kind: Str::Var, .. } => 2,
            SqlType::Num(Num::SmallInt) => 3,
            SqlType::Num(Num::Integer) => 4,
            SqlType::Num(Num::BigInt) => 5,
            SqlType::Num(Num::Real) => 6,
            SqlType::Num(Num::DoublePrecision) => 7,
        }
    }

    pub fn from_type_id(type_id: u64, chars_len: u64) -> SqlType {
        match type_id {
            0 => SqlType::Bool,
            1 => SqlType::char(chars_len),
            2 => SqlType::var_char(chars_len),
            3 => SqlType::small_int(),
            4 => SqlType::integer(),
            5 => SqlType::big_int(),
            6 => SqlType::real(),
            7 => SqlType::double_precision(),
            _ => unreachable!(),
        }
    }

    pub fn chars_len(&self) -> Option<u64> {
        match self {
            SqlType::Str { len, .. } | SqlType::Str { len, .. } => Some(*len),
            _ => None,
        }
    }
}

impl TryFrom<&DataType> for SqlType {
    type Error = NotSupportedType;

    fn try_from(data_type: &DataType) -> Result<Self, Self::Error> {
        match data_type {
            DataType::SmallInt => Ok(SqlType::small_int()),
            DataType::Int => Ok(SqlType::integer()),
            DataType::BigInt => Ok(SqlType::big_int()),
            DataType::Char(len) => Ok(SqlType::char(len.unwrap_or(255))),
            DataType::Varchar(len) => Ok(SqlType::var_char(len.unwrap_or(255))),
            DataType::Boolean => Ok(SqlType::Bool),
            _other_type => Err(NotSupportedType),
        }
    }
}

pub struct NotSupportedType;

impl Display for SqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SqlType::Bool => write!(f, "bool"),
            SqlType::Str { len, kind: Str::Const } => write!(f, "char({})", len),
            SqlType::Str { len, kind: Str::Var } => write!(f, "varchar({})", len),
            SqlType::Num(Num::SmallInt) => write!(f, "smallint"),
            SqlType::Num(Num::Integer) => write!(f, "integer"),
            SqlType::Num(Num::BigInt) => write!(f, "bigint"),
            SqlType::Num(Num::Real) => write!(f, "real"),
            SqlType::Num(Num::DoublePrecision) => write!(f, "double precision"),
        }
    }
}

impl Into<PgType> for &SqlType {
    fn into(self) -> PgType {
        match self {
            SqlType::Bool => PgType::Bool,
            SqlType::Str { kind: Str::Const, .. } => PgType::Char,
            SqlType::Str { kind: Str::Var, .. } => PgType::VarChar,
            SqlType::Num(Num::SmallInt) => PgType::SmallInt,
            SqlType::Num(Num::Integer) => PgType::Integer,
            SqlType::Num(Num::BigInt) => PgType::BigInt,
            SqlType::Num(Num::Real) | SqlType::Num(Num::DoublePrecision) => unreachable!(),
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
            let pg_type: PgType = (&SqlType::bool()).into();
            assert_eq!(pg_type, PgType::Bool);
        }

        #[test]
        fn small_int() {
            let pg_type: PgType = (&SqlType::small_int()).into();
            assert_eq!(pg_type, PgType::SmallInt);
        }

        #[test]
        fn integer() {
            let pg_type: PgType = (&SqlType::integer()).into();
            assert_eq!(pg_type, PgType::Integer);
        }

        #[test]
        fn big_int() {
            let pg_type: PgType = (&SqlType::big_int()).into();
            assert_eq!(pg_type, PgType::BigInt);
        }

        #[test]
        fn char() {
            let pg_type: PgType = (&SqlType::char(0)).into();
            assert_eq!(pg_type, PgType::Char);
        }

        #[test]
        fn var_char() {
            let pg_type: PgType = (&SqlType::var_char(0)).into();
            assert_eq!(pg_type, PgType::VarChar);
        }
    }
}
