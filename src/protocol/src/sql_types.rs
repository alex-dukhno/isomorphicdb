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

impl std::string::ToString for PostgreSqlType {
    fn to_string(&self) -> String {
        match self {
            Self::Bool => "bool".to_string(),
            Self::Char => "char".to_string(),
            Self::BigInt => "bigint".to_string(),
            Self::SmallInt => "smallint".to_string(),
            Self::Integer => "integer".to_string(),
            Self::Real => "real".to_string(),
            Self::DoublePrecision => "double".to_string(),
            Self::VarChar => "varchar".to_string(),
            Self::Date => "date".to_string(),
            Self::Time => "time".to_string(),
            Self::Timestamp => "timestamp".to_string(),
            Self::TimestampWithTimeZone => "timestampwithtimezone".to_string(),
            Self::Interval => "interval".to_string(),
            Self::TimeWithTimeZone => "datewithtimezone".to_string(),
            Self::Decimal => "decimal".to_string(),
        }
    }
}