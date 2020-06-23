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

mod types;

use crate::types::SQLType;
use kernel::SystemError;
use std::convert::TryFrom;

pub type TypeId = u32;

pub struct Constraint;

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum SqlType {
    Bool,
    Char,
    VarChar,
    Decimal,
    SmallInt,
    Integer,
    BigInt,
    Real,
    DoublePrecision,
    Time,
    TimeWithTimeZone,
    Timestamp,
    TimestampWithTimeZone,
    Date,
    Interval,
}

impl SqlType {
    pub fn id(&self) -> TypeId {
        match *self {
            SqlType::Bool => 0,
            SqlType::Char => 1,
            SqlType::VarChar => 2,
            SqlType::Decimal => 3,
            SqlType::SmallInt => 4,
            SqlType::Integer => 5,
            SqlType::BigInt => 6,
            SqlType::Real => 7,
            SqlType::DoublePrecision => 8,
            SqlType::Time => 9,
            SqlType::TimeWithTimeZone => 10,
            SqlType::Timestamp => 11,
            SqlType::TimestampWithTimeZone => 12,
            SqlType::Date => 13,
            SqlType::Interval => 14,
        }
    }

    pub fn sql_type(&self) -> Box<dyn SQLType> {
        match *self {
            SqlType::SmallInt => Box::new(crate::types::SmallIntSqlType),
            SqlType::Integer => Box::new(crate::types::IntegerSqlType),
            SqlType::BigInt => Box::new(crate::types::BigIntSqlType),
            _ => unimplemented!(),
        }
    }
}

impl TryFrom<TypeId> for SqlType {
    type Error = SystemError;

    fn try_from(id: TypeId) -> Result<Self, Self::Error> {
        match id {
            0 => Ok(SqlType::Bool),
            1 => Ok(SqlType::Char),
            2 => Ok(SqlType::VarChar),
            3 => Ok(SqlType::Decimal),
            4 => Ok(SqlType::SmallInt),
            5 => Ok(SqlType::Integer),
            6 => Ok(SqlType::BigInt),
            7 => Ok(SqlType::Real),
            8 => Ok(SqlType::DoublePrecision),
            9 => Ok(SqlType::Time),
            10 => Ok(SqlType::TimeWithTimeZone),
            11 => Ok(SqlType::Timestamp),
            12 => Ok(SqlType::TimestampWithTimeZone),
            13 => Ok(SqlType::Date),
            14 => Ok(SqlType::Interval),
            id => Err(SystemError::unrecoverable(format!(
                "trying to use unsupported type id {}",
                id
            ))),
        }
    }
}
