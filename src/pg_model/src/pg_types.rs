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

use bigdecimal::BigDecimal;
use byteorder::{BigEndian, ReadBytesExt};
use pg_wire::{ColumnMetadata, PgFormat};
use sqlparser::ast::{DataType, Expr, Value};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Display, Formatter},
    str,
};

/// PostgreSQL Object Identifier
pub type Oid = u32;

/// Represents PostgreSQL data type and methods to send over wire
#[allow(missing_docs)]
#[derive(PartialEq, Debug, Copy, Clone, PartialOrd, Eq)]
pub enum PgType {
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

/// Not supported data type of sqlparser
pub struct NotSupportedDataType(DataType);

/// Not supported OID
pub struct NotSupportedOid(pub(crate) Oid);

impl TryFrom<&DataType> for PgType {
    type Error = NotSupportedDataType;

    /// Returns the type corresponding to the provided data type, if the data
    /// type is known.
    fn try_from(data_type: &DataType) -> Result<Self, Self::Error> {
        match data_type {
            DataType::SmallInt => Ok(PgType::SmallInt),
            DataType::Int => Ok(PgType::Integer),
            DataType::BigInt => Ok(PgType::BigInt),
            DataType::Char(_) => Ok(PgType::Char),
            DataType::Varchar(_) => Ok(PgType::VarChar),
            DataType::Boolean => Ok(PgType::Bool),
            other_type => Err(NotSupportedDataType(other_type.clone())),
        }
    }
}

impl TryFrom<Oid> for PgType {
    type Error = NotSupportedOid;

    /// Returns the type corresponding to the provided OID, if the OID is known.
    fn try_from(oid: Oid) -> Result<Self, Self::Error> {
        match oid {
            16 => Ok(PgType::Bool),
            18 => Ok(PgType::Char),
            20 => Ok(PgType::BigInt),
            21 => Ok(PgType::SmallInt),
            23 => Ok(PgType::Integer),
            700 => Ok(PgType::Real),
            701 => Ok(PgType::DoublePrecision),
            1043 => Ok(PgType::VarChar),
            1082 => Ok(PgType::Date),
            1083 => Ok(PgType::Time),
            1114 => Ok(PgType::Timestamp),
            1184 => Ok(PgType::TimestampWithTimeZone),
            1186 => Ok(PgType::Interval),
            1266 => Ok(PgType::TimeWithTimeZone),
            1700 => Ok(PgType::Decimal),
            _ => Err(NotSupportedOid(oid)),
        }
    }
}

impl PgType {
    /// PostgreSQL type OID
    pub fn pg_oid(&self) -> Oid {
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

    pub fn as_column_metadata<S: ToString>(&self, name: S) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            type_id: self.pg_oid(),
            type_size: self.pg_len(),
        }
    }

    /// Deserializes a value of this type from `raw` using the specified `format`.
    pub fn decode(&self, format: &PostgreSqlFormat, raw: &[u8]) -> Result<PostgreSqlValue, String> {
        match format {
            PostgreSqlFormat::Binary => self.decode_binary(raw),
            PostgreSqlFormat::Text => self.decode_text(raw),
        }
    }

    fn decode_binary(&self, raw: &[u8]) -> Result<PostgreSqlValue, String> {
        match self {
            Self::Bool => parse_bool_from_binary(raw),
            Self::Char => parse_char_from_binary(raw),
            Self::VarChar => parse_varchar_from_binary(raw),
            Self::SmallInt => parse_smallint_from_binary(raw),
            Self::Integer => parse_integer_from_binary(raw),
            Self::BigInt => parse_bigint_from_binary(raw),
            other => Err(format!("Unsupported Postgres type: {:?}", other)),
        }
    }

    fn decode_text(&self, raw: &[u8]) -> Result<PostgreSqlValue, String> {
        let s = match str::from_utf8(raw) {
            Ok(s) => s,
            Err(_) => return Err(format!("Failed to parse UTF8 from: {:?}", raw)),
        };

        match self {
            Self::Bool => parse_bool_from_text(s),
            Self::Char => parse_char_from_text(s),
            Self::VarChar => parse_varchar_from_text(s),
            Self::SmallInt => parse_smallint_from_text(s),
            Self::Integer => parse_integer_from_text(s),
            Self::BigInt => parse_bigint_from_text(s),
            other => Err(format!("Unsupported Postgres type: {:?}", other)),
        }
    }
}

impl Display for PgType {
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

fn parse_bigint_from_binary(mut buf: &[u8]) -> Result<PostgreSqlValue, String> {
    let v = match buf.read_i64::<BigEndian>() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse BigInt from: {:?}", buf)),
    };

    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }

    Ok(PostgreSqlValue::Int64(v))
}

fn parse_bigint_from_text(s: &str) -> Result<PostgreSqlValue, String> {
    let v: i64 = match s.trim().parse() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {}", s)),
    };

    Ok(PostgreSqlValue::Int64(v))
}

fn parse_bool_from_binary(buf: &[u8]) -> Result<PostgreSqlValue, String> {
    let len = buf.len();
    if len != 1 {
        return Err("invalid buffer size".into());
    }

    let v = if buf[0] == 0 {
        PostgreSqlValue::False
    } else {
        PostgreSqlValue::True
    };

    Ok(v)
}

fn parse_bool_from_text(s: &str) -> Result<PostgreSqlValue, String> {
    match s.trim().to_lowercase().as_str() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Ok(PostgreSqlValue::True),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Ok(PostgreSqlValue::False),
        _ => Err(format!("Failed to parse Bool from: {}", s)),
    }
}

fn parse_char_from_binary(buf: &[u8]) -> Result<PostgreSqlValue, String> {
    let s = match str::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => return Err(format!("Failed to parse UTF8 from: {:?}", buf)),
    };

    Ok(PostgreSqlValue::String(s.into()))
}

fn parse_char_from_text(s: &str) -> Result<PostgreSqlValue, String> {
    Ok(PostgreSqlValue::String(s.into()))
}

fn parse_integer_from_binary(mut buf: &[u8]) -> Result<PostgreSqlValue, String> {
    let v = match buf.read_i32::<BigEndian>() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse Integer from: {:?}", buf)),
    };

    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }

    Ok(PostgreSqlValue::Int32(v))
}

fn parse_integer_from_text(s: &str) -> Result<PostgreSqlValue, String> {
    let v: i32 = match s.trim().parse() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {}", s)),
    };

    Ok(PostgreSqlValue::Int32(v))
}

fn parse_smallint_from_binary(mut buf: &[u8]) -> Result<PostgreSqlValue, String> {
    let v = match buf.read_i32::<BigEndian>() {
        Ok(v) => v as i16,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {:?}", buf)),
    };

    Ok(PostgreSqlValue::Int16(v))
}

fn parse_smallint_from_text(s: &str) -> Result<PostgreSqlValue, String> {
    let v: i16 = match s.trim().parse() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {}", s)),
    };

    Ok(PostgreSqlValue::Int16(v))
}

fn parse_varchar_from_binary(buf: &[u8]) -> Result<PostgreSqlValue, String> {
    let s = match str::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => return Err(format!("Failed to parse UTF8 from: {:?}", buf)),
    };

    Ok(PostgreSqlValue::String(s.into()))
}

fn parse_varchar_from_text(s: &str) -> Result<PostgreSqlValue, String> {
    Ok(PostgreSqlValue::String(s.into()))
}

/// Represents PostgreSQL data values sent and received over wire
#[allow(missing_docs)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PostgreSqlValue {
    Null,
    True,
    False,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(String),
}

impl TryInto<Expr> for PostgreSqlValue {
    type Error = ();

    fn try_into(self) -> Result<Expr, Self::Error> {
        match self {
            Self::Null => Ok(Expr::Value(Value::Null)),
            Self::True => Ok(Expr::Value(Value::Boolean(true))),
            Self::False => Ok(Expr::Value(Value::Boolean(false))),
            Self::Int16(i) => Ok(Expr::Value(Value::Number(BigDecimal::from(i)))),
            Self::Int32(i) => Ok(Expr::Value(Value::Number(BigDecimal::from(i)))),
            Self::Int64(i) => Ok(Expr::Value(Value::Number(BigDecimal::from(i)))),
            Self::String(s) => Ok(Expr::Value(Value::SingleQuotedString(s))),
        }
    }
}

/// Represents PostgreSQL data format
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PostgreSqlFormat {
    /// Text encoding.
    Text,
    /// Binary encoding.
    Binary,
}

impl From<PgFormat> for PostgreSqlFormat {
    fn from(pg_format: PgFormat) -> Self {
        match pg_format {
            PgFormat::Text => PostgreSqlFormat::Text,
            PgFormat::Binary => PostgreSqlFormat::Binary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod binary_decoding {
        use super::*;

        #[test]
        fn decode_true() {
            assert_eq!(
                PgType::Bool.decode(&PostgreSqlFormat::Binary, &[1]),
                Ok(PostgreSqlValue::True)
            );
        }

        #[test]
        fn decode_false() {
            assert_eq!(
                PgType::Bool.decode(&PostgreSqlFormat::Binary, &[0]),
                Ok(PostgreSqlValue::False)
            );
        }

        #[test]
        fn decode_char() {
            assert_eq!(
                PgType::Char.decode(&PostgreSqlFormat::Binary, &[97, 98, 99]),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_varchar() {
            assert_eq!(
                PgType::VarChar.decode(&PostgreSqlFormat::Binary, &[97, 98, 99]),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_smallint() {
            assert_eq!(
                PgType::SmallInt.decode(&PostgreSqlFormat::Binary, &[0, 0, 0, 1]),
                Ok(PostgreSqlValue::Int16(1))
            );
        }

        #[test]
        fn decode_integer() {
            assert_eq!(
                PgType::Integer.decode(&PostgreSqlFormat::Binary, &[0, 0, 0, 1]),
                Ok(PostgreSqlValue::Int32(1))
            );
        }

        #[test]
        fn decode_bigint() {
            assert_eq!(
                PgType::BigInt.decode(&PostgreSqlFormat::Binary, &[0, 0, 0, 0, 0, 0, 0, 1]),
                Ok(PostgreSqlValue::Int64(1))
            );
        }
    }

    #[cfg(test)]
    mod text_decoding {
        use super::*;

        #[test]
        fn decode_true() {
            assert_eq!(
                PgType::Bool.decode(&PostgreSqlFormat::Text, b"true"),
                Ok(PostgreSqlValue::True)
            );
        }

        #[test]
        fn decode_false() {
            assert_eq!(
                PgType::Bool.decode(&PostgreSqlFormat::Text, b"0"),
                Ok(PostgreSqlValue::False)
            );
        }

        #[test]
        fn decode_char() {
            assert_eq!(
                PgType::Char.decode(&PostgreSqlFormat::Text, b"abc"),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_varchar() {
            assert_eq!(
                PgType::VarChar.decode(&PostgreSqlFormat::Text, b"abc"),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_smallint() {
            assert_eq!(
                PgType::SmallInt.decode(&PostgreSqlFormat::Text, b"1"),
                Ok(PostgreSqlValue::Int16(1))
            );
        }

        #[test]
        fn decode_integer() {
            assert_eq!(
                PgType::Integer.decode(&PostgreSqlFormat::Text, b"123"),
                Ok(PostgreSqlValue::Int32(123))
            );
        }

        #[test]
        fn decode_bigint() {
            assert_eq!(
                PgType::BigInt.decode(&PostgreSqlFormat::Text, b"123456"),
                Ok(PostgreSqlValue::Int64(123456))
            );
        }
    }
}
