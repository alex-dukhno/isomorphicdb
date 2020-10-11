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

use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    str,
};

use byteorder::{BigEndian, ReadBytesExt};

/// PostgreSQL Object Identifier
pub type Oid = u32;

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

impl TryFrom<Oid> for PostgreSqlType {
    type Error = ();

    /// Returns the type corresponding to the provided OID, if the OID is known.
    fn try_from(oid: Oid) -> Result<Self, Self::Error> {
        match oid {
            16 => Ok(PostgreSqlType::Bool),
            18 => Ok(PostgreSqlType::Char),
            20 => Ok(PostgreSqlType::BigInt),
            21 => Ok(PostgreSqlType::SmallInt),
            23 => Ok(PostgreSqlType::Integer),
            700 => Ok(PostgreSqlType::Real),
            701 => Ok(PostgreSqlType::DoublePrecision),
            1043 => Ok(PostgreSqlType::VarChar),
            1082 => Ok(PostgreSqlType::Date),
            1083 => Ok(PostgreSqlType::Time),
            1114 => Ok(PostgreSqlType::Timestamp),
            1184 => Ok(PostgreSqlType::TimestampWithTimeZone),
            1186 => Ok(PostgreSqlType::Interval),
            1266 => Ok(PostgreSqlType::TimeWithTimeZone),
            1700 => Ok(PostgreSqlType::Decimal),
            _ => Err(()),
        }
    }
}

impl PostgreSqlType {
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

    /// Deserializes a value of this type from `raw` using the specified `format`.
    pub fn decode(&self, format: &PostgreSqlFormat, raw: &[u8]) -> Result<PostgreSqlValue, String> {
        log::debug!("raw data - {:#?}", raw);
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
    log::debug!("parsing SmallInt");
    let v = match buf.read_i32::<BigEndian>() {
        Ok(v) => v as i16,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {:?}", buf)),
    };

    log::debug!("Value to insert {:?}", v);

    // if !buf.is_empty() {
    //     return Err("invalid buffer size".into());
    // }

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

/// Represents PostgreSQL data format
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PostgreSqlFormat {
    /// Text encoding.
    Text,
    /// Binary encoding.
    Binary,
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
                PostgreSqlType::Bool.decode(&PostgreSqlFormat::Binary, &[1]),
                Ok(PostgreSqlValue::True)
            );
        }

        #[test]
        fn decode_false() {
            assert_eq!(
                PostgreSqlType::Bool.decode(&PostgreSqlFormat::Binary, &[0]),
                Ok(PostgreSqlValue::False)
            );
        }

        #[test]
        fn decode_char() {
            assert_eq!(
                PostgreSqlType::Char.decode(&PostgreSqlFormat::Binary, &[97, 98, 99]),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_varchar() {
            assert_eq!(
                PostgreSqlType::VarChar.decode(&PostgreSqlFormat::Binary, &[97, 98, 99]),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_smallint() {
            assert_eq!(
                PostgreSqlType::SmallInt.decode(&PostgreSqlFormat::Binary, &[0, 0, 0, 1]),
                Ok(PostgreSqlValue::Int16(1))
            );
        }

        #[test]
        fn decode_integer() {
            assert_eq!(
                PostgreSqlType::Integer.decode(&PostgreSqlFormat::Binary, &[0, 0, 0, 1]),
                Ok(PostgreSqlValue::Int32(1))
            );
        }

        #[test]
        fn decode_bigint() {
            assert_eq!(
                PostgreSqlType::BigInt.decode(&PostgreSqlFormat::Binary, &[0, 0, 0, 0, 0, 0, 0, 1]),
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
                PostgreSqlType::Bool.decode(&PostgreSqlFormat::Text, b"true"),
                Ok(PostgreSqlValue::True)
            );
        }

        #[test]
        fn decode_false() {
            assert_eq!(
                PostgreSqlType::Bool.decode(&PostgreSqlFormat::Text, b"0"),
                Ok(PostgreSqlValue::False)
            );
        }

        #[test]
        fn decode_char() {
            assert_eq!(
                PostgreSqlType::Char.decode(&PostgreSqlFormat::Text, b"abc"),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_varchar() {
            assert_eq!(
                PostgreSqlType::VarChar.decode(&PostgreSqlFormat::Text, b"abc"),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_smallint() {
            assert_eq!(
                PostgreSqlType::SmallInt.decode(&PostgreSqlFormat::Text, b"1"),
                Ok(PostgreSqlValue::Int16(1))
            );
        }

        #[test]
        fn decode_integer() {
            assert_eq!(
                PostgreSqlType::Integer.decode(&PostgreSqlFormat::Text, b"123"),
                Ok(PostgreSqlValue::Int32(123))
            );
        }

        #[test]
        fn decode_bigint() {
            assert_eq!(
                PostgreSqlType::BigInt.decode(&PostgreSqlFormat::Text, b"123456"),
                Ok(PostgreSqlValue::Int64(123456))
            );
        }
    }
}
