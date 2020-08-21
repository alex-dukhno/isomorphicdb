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

use std::fmt::{self, Display, Formatter};

/// Represents PostgreSQL data type and methods to send over wire
#[allow(missing_docs)]
#[derive(PartialEq, Debug, Copy, Clone, PartialOrd, Eq)]
pub enum PostgreSqlType {
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

impl PostgreSqlType {
    /// Returns the type corresponding to the provided OID, if the OID is known.
    pub fn from_oid(oid: u32) -> Option<Self> {
        match oid {
            16 => Some(Self::Bool),
            18 => Some(Self::Char),
            20 => Some(Self::BigInt),
            21 => Some(Self::SmallInt),
            23 => Some(Self::Integer),
            700 => Some(Self::Real),
            701 => Some(Self::DoublePrecision),
            1043 => Some(Self::VarChar),
            1082 => Some(Self::Date),
            1083 => Some(Self::Time),
            1114 => Some(Self::Timestamp),
            1184 => Some(Self::TimestampWithTimeZone),
            1186 => Some(Self::Interval),
            1266 => Some(Self::TimeWithTimeZone),
            1700 => Some(Self::Decimal),
            _ => None,
        }
    }

    /// PostgreSQL type OID
    pub fn pg_oid(&self) -> i32 {
        match self {
            Self::Bool => 16,
            Self::Char => 18,
            Self::BigInt => 20,           // PG int8
            Self::SmallInt => 21,         // PG int2
            Self::Integer => 23,          // PG int4
            Self::Real => 700,            // PG float4
            Self::DoublePrecision => 701, // PG float8
            Self::VarChar => 1043,
            Self::Date => 1082,
            Self::Time => 1083,
            Self::Timestamp => 1114,
            Self::TimestampWithTimeZone => 1184, // PG Timestamptz
            Self::Interval => 1186,
            Self::TimeWithTimeZone => 1266, // PG Timetz
            Self::Decimal => 1700,          // PG Numeric & Decimal
        }
    }

    /// PostgreSQL type length
    pub fn pg_len(&self) -> i16 {
        match self {
            Self::Bool => 1,
            Self::Char => 1,
            Self::BigInt => 8,
            Self::SmallInt => 2,
            Self::Integer => 4,
            Self::Real => 4,
            Self::DoublePrecision => 8,
            Self::VarChar => -1,
            Self::Date => 4,
            Self::Time => 8,
            Self::Timestamp => 8,
            Self::TimestampWithTimeZone => 8,
            Self::Interval => 16,
            Self::TimeWithTimeZone => 12,
            Self::Decimal => -1,
        }
    }
}

impl Display for PostgreSqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool => write!(f, "bool"),
            Self::Char => write!(f, "character"),
            Self::BigInt => write!(f, "bigint"),
            Self::SmallInt => write!(f, "smallint"),
            Self::Integer => write!(f, "integer"),
            Self::Real => write!(f, "real"),
            Self::DoublePrecision => write!(f, "double"),
            Self::VarChar => write!(f, "variable character"),
            Self::Date => write!(f, "date"),
            Self::Time => write!(f, "time"),
            Self::TimeWithTimeZone => write!(f, "time with timezone"),
            Self::Timestamp => write!(f, "timestamp"),
            Self::TimestampWithTimeZone => write!(f, "timestamp with timezone"),
            Self::Interval => write!(f, "interval"),
            Self::Decimal => write!(f, "decimal"),
        }
    }
}
