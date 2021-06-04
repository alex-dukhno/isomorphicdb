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

use query_ast::DataType;
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use wire_protocol_payload::*;

#[derive(Debug, PartialEq)]
pub struct IncomparableSqlTypeFamilies {
    left: SqlTypeFamilyOld,
    right: SqlTypeFamilyOld,
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum SqlTypeFamilyOld {
    Bool,
    String,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
}

impl Display for SqlTypeFamilyOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SqlTypeFamilyOld::Bool => write!(f, "bool"),
            SqlTypeFamilyOld::String => write!(f, "string"),
            SqlTypeFamilyOld::SmallInt => write!(f, "smallint"),
            SqlTypeFamilyOld::Integer => write!(f, "integer"),
            SqlTypeFamilyOld::BigInt => write!(f, "bigint"),
            SqlTypeFamilyOld::Real => write!(f, "real"),
            SqlTypeFamilyOld::Double => write!(f, "double precision"),
        }
    }
}

impl SqlTypeFamilyOld {
    pub fn compare(&self, other: &SqlTypeFamilyOld) -> Result<SqlTypeFamilyOld, IncomparableSqlTypeFamilies> {
        if self.is_float() && other.is_float() {
            if self == other {
                Ok(*self)
            } else if self == &SqlTypeFamilyOld::Real && other == &SqlTypeFamilyOld::Double {
                Ok(*other)
            } else {
                Ok(*self)
            }
        } else if self.is_int() && other.is_int() {
            if self == other {
                Ok(*self)
            } else if self == &SqlTypeFamilyOld::SmallInt && other == &SqlTypeFamilyOld::Integer || other == &SqlTypeFamilyOld::BigInt {
                Ok(*other)
            } else {
                Ok(*self)
            }
        } else if self.is_float() && other.is_int() {
            Ok(*self)
        } else if self.is_int() && other.is_float() {
            Ok(*other)
        } else if self != other {
            Err(IncomparableSqlTypeFamilies { left: *self, right: *other })
        } else {
            Ok(*self)
        }
    }

    fn is_float(&self) -> bool {
        self == &SqlTypeFamilyOld::Real || self == &SqlTypeFamilyOld::Double
    }

    fn is_int(&self) -> bool {
        self == &SqlTypeFamilyOld::SmallInt || self == &SqlTypeFamilyOld::Integer || self == &SqlTypeFamilyOld::BigInt
    }
}

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum SqlTypeOld {
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
    Double,
}

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum Str {
    Const,
    Var,
}

impl SqlTypeOld {
    pub fn family(&self) -> SqlTypeFamilyOld {
        match self {
            SqlTypeOld::Bool => SqlTypeFamilyOld::Bool,
            SqlTypeOld::Str { .. } => SqlTypeFamilyOld::String,
            SqlTypeOld::Num(Num::SmallInt) => SqlTypeFamilyOld::SmallInt,
            SqlTypeOld::Num(Num::Integer) => SqlTypeFamilyOld::Integer,
            SqlTypeOld::Num(Num::BigInt) => SqlTypeFamilyOld::BigInt,
            SqlTypeOld::Num(Num::Real) | SqlTypeOld::Num(Num::Double) => SqlTypeFamilyOld::Real,
        }
    }

    pub fn small_int() -> SqlTypeOld {
        SqlTypeOld::Num(Num::SmallInt)
    }

    pub fn integer() -> SqlTypeOld {
        SqlTypeOld::Num(Num::Integer)
    }

    pub fn big_int() -> SqlTypeOld {
        SqlTypeOld::Num(Num::BigInt)
    }

    pub fn real() -> SqlTypeOld {
        SqlTypeOld::Num(Num::Real)
    }

    pub fn double_precision() -> SqlTypeOld {
        SqlTypeOld::Num(Num::Double)
    }

    pub fn bool() -> SqlTypeOld {
        SqlTypeOld::Bool
    }

    pub fn char(len: u64) -> SqlTypeOld {
        SqlTypeOld::Str { len, kind: Str::Const }
    }

    pub fn var_char(len: u64) -> SqlTypeOld {
        SqlTypeOld::Str { len, kind: Str::Var }
    }

    pub fn type_id(&self) -> u64 {
        match self {
            SqlTypeOld::Bool => 0,
            SqlTypeOld::Str { kind: Str::Const, .. } => 1,
            SqlTypeOld::Str { kind: Str::Var, .. } => 2,
            SqlTypeOld::Num(Num::SmallInt) => 3,
            SqlTypeOld::Num(Num::Integer) => 4,
            SqlTypeOld::Num(Num::BigInt) => 5,
            SqlTypeOld::Num(Num::Real) => 6,
            SqlTypeOld::Num(Num::Double) => 7,
        }
    }

    pub fn from_type_id(type_id: u64, chars_len: u64) -> SqlTypeOld {
        match type_id {
            0 => SqlTypeOld::Bool,
            1 => SqlTypeOld::char(chars_len),
            2 => SqlTypeOld::var_char(chars_len),
            3 => SqlTypeOld::small_int(),
            4 => SqlTypeOld::integer(),
            5 => SqlTypeOld::big_int(),
            6 => SqlTypeOld::real(),
            7 => SqlTypeOld::double_precision(),
            _ => unreachable!(),
        }
    }

    pub fn chars_len(&self) -> Option<u64> {
        match self {
            SqlTypeOld::Str { len, .. } => Some(*len),
            _ => None,
        }
    }
}

impl From<DataType> for SqlTypeOld {
    fn from(data_type: DataType) -> SqlTypeOld {
        match data_type {
            DataType::SmallInt => SqlTypeOld::small_int(),
            DataType::Int => SqlTypeOld::integer(),
            DataType::BigInt => SqlTypeOld::big_int(),
            DataType::Char(len) => SqlTypeOld::char(len as u64),
            DataType::VarChar(len) => SqlTypeOld::var_char(len.unwrap_or(255) as u64),
            DataType::Bool => SqlTypeOld::Bool,
            DataType::Real => SqlTypeOld::real(),
            DataType::Double => SqlTypeOld::double_precision(),
        }
    }
}

impl Display for SqlTypeOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SqlTypeOld::Bool => write!(f, "bool"),
            SqlTypeOld::Str { len, kind: Str::Const } => write!(f, "char({})", len),
            SqlTypeOld::Str { len, kind: Str::Var } => write!(f, "varchar({})", len),
            SqlTypeOld::Num(Num::SmallInt) => write!(f, "smallint"),
            SqlTypeOld::Num(Num::Integer) => write!(f, "integer"),
            SqlTypeOld::Num(Num::BigInt) => write!(f, "bigint"),
            SqlTypeOld::Num(Num::Real) => write!(f, "real"),
            SqlTypeOld::Num(Num::Double) => write!(f, "double precision"),
        }
    }
}

impl From<&u32> for SqlTypeFamilyOld {
    fn from(pg_type: &u32) -> SqlTypeFamilyOld {
        match pg_type {
            &SMALLINT => SqlTypeFamilyOld::SmallInt,
            &INT => SqlTypeFamilyOld::Integer,
            &BIGINT => SqlTypeFamilyOld::BigInt,
            &CHAR | &VARCHAR => SqlTypeFamilyOld::String,
            &BOOL => SqlTypeFamilyOld::Bool,
            _ => unimplemented!(),
        }
    }
}

impl From<&SqlTypeOld> for u32 {
    fn from(sql_type: &SqlTypeOld) -> u32 {
        match sql_type {
            SqlTypeOld::Bool => BOOL,
            SqlTypeOld::Str { kind: Str::Const, .. } => CHAR,
            SqlTypeOld::Str { kind: Str::Var, .. } => VARCHAR,
            SqlTypeOld::Num(Num::SmallInt) => SMALLINT,
            SqlTypeOld::Num(Num::Integer) => INT,
            SqlTypeOld::Num(Num::BigInt) => BIGINT,
            SqlTypeOld::Num(Num::Real) | SqlTypeOld::Num(Num::Double) => unreachable!(),
        }
    }
}

#[derive(PartialEq, Debug, Copy, Clone, Eq)]
pub struct Bool(pub bool);

impl FromStr for Bool {
    type Err = ParseBoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let val = s.to_lowercase();
        match val.as_str() {
            "t" | "true" | "on" | "yes" | "y" | "1" => Ok(Bool(true)),
            "f" | "false" | "off" | "no" | "n" | "0" => Ok(Bool(false)),
            _ => Err(ParseBoolError(val)),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct ParseBoolError(String);

impl Display for ParseBoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "error to parse {:?} into boolean", self.0)
    }
}

#[cfg(test)]
mod tests;
