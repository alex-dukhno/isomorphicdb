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

use crate::{sql_formats::PostgreSqlFormat, sql_types::PostgreSqlType};
use byteorder::{BigEndian, ReadBytesExt};
use std::str;

type Result = std::result::Result<PostgreSqlValue, String>;

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

impl PostgreSqlValue {
    /// Deserializes a value of type `typ` from `raw` using the specified
    /// `format`.
    pub fn decode(format: &PostgreSqlFormat, typ: &PostgreSqlType, raw: &[u8]) -> Result {
        match format {
            PostgreSqlFormat::Binary => decode_binary(typ, raw),
            PostgreSqlFormat::Text => decode_text(typ, raw),
        }
    }
}

fn decode_binary(typ: &PostgreSqlType, raw: &[u8]) -> Result {
    match typ {
        PostgreSqlType::Bool => parse_bool_from_binary(raw),
        PostgreSqlType::Char => parse_char_from_binary(raw),
        PostgreSqlType::VarChar => parse_varchar_from_binary(raw),
        PostgreSqlType::SmallInt => parse_smallint_from_binary(raw),
        PostgreSqlType::Integer => parse_integer_from_binary(raw),
        PostgreSqlType::BigInt => parse_bigint_from_binary(raw),
        other => Err(format!("Unsupported Postgres type: {:?}", other)),
    }
}

fn decode_text(typ: &PostgreSqlType, raw: &[u8]) -> Result {
    let s = match str::from_utf8(raw) {
        Ok(s) => s,
        Err(_) => return Err(format!("Failed to parse UTF8 from: {:?}", raw)),
    };

    match typ {
        PostgreSqlType::Bool => parse_bool_from_text(s),
        PostgreSqlType::Char => parse_char_from_text(s),
        PostgreSqlType::VarChar => parse_varchar_from_text(s),
        PostgreSqlType::SmallInt => parse_smallint_from_text(s),
        PostgreSqlType::Integer => parse_integer_from_text(s),
        PostgreSqlType::BigInt => parse_bigint_from_text(s),
        other => Err(format!("Unsupported Postgres type: {:?}", other)),
    }
}

fn parse_bigint_from_binary(mut buf: &[u8]) -> Result {
    let v = match buf.read_i64::<BigEndian>() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse BigInt from: {:?}", buf)),
    };

    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }

    Ok(PostgreSqlValue::Int64(v))
}

fn parse_bigint_from_text(s: &str) -> Result {
    let v: i64 = match s.trim().parse() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {}", s)),
    };

    Ok(PostgreSqlValue::Int64(v))
}

fn parse_bool_from_binary(buf: &[u8]) -> Result {
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

fn parse_bool_from_text(s: &str) -> Result {
    match s.trim().to_lowercase().as_str() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Ok(PostgreSqlValue::True),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Ok(PostgreSqlValue::False),
        _ => Err(format!("Failed to parse Bool from: {}", s)),
    }
}

fn parse_char_from_binary(buf: &[u8]) -> Result {
    let len = buf.len();
    if len != 1 {
        return Err("invalid buffer size".into());
    }

    let s = match str::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => return Err(format!("Failed to parse UTF8 from: {:?}", buf)),
    };

    Ok(PostgreSqlValue::String(s.into()))
}

fn parse_char_from_text(s: &str) -> Result {
    if s.len() != 1 {
        return Err("invalid buffer size".into());
    }
    Ok(PostgreSqlValue::String(s.into()))
}

fn parse_integer_from_binary(mut buf: &[u8]) -> Result {
    let v = match buf.read_i32::<BigEndian>() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse Integer from: {:?}", buf)),
    };

    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }

    Ok(PostgreSqlValue::Int32(v))
}

fn parse_integer_from_text(s: &str) -> Result {
    let v: i32 = match s.trim().parse() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {}", s)),
    };

    Ok(PostgreSqlValue::Int32(v))
}

fn parse_smallint_from_binary(mut buf: &[u8]) -> Result {
    let v = match buf.read_i16::<BigEndian>() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {:?}", buf)),
    };

    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }

    Ok(PostgreSqlValue::Int16(v))
}

fn parse_smallint_from_text(s: &str) -> Result {
    let v: i16 = match s.trim().parse() {
        Ok(v) => v,
        Err(_) => return Err(format!("Failed to parse SmallInt from: {}", s)),
    };

    Ok(PostgreSqlValue::Int16(v))
}

fn parse_varchar_from_binary(buf: &[u8]) -> Result {
    let s = match str::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => return Err(format!("Failed to parse UTF8 from: {:?}", buf)),
    };

    Ok(PostgreSqlValue::String(s.into()))
}

fn parse_varchar_from_text(s: &str) -> Result {
    Ok(PostgreSqlValue::String(s.into()))
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
                PostgreSqlValue::decode(&PostgreSqlFormat::Binary, &PostgreSqlType::Bool, &[1]),
                Ok(PostgreSqlValue::True)
            );
        }

        #[test]
        fn decode_false() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Binary, &PostgreSqlType::Bool, &[0]),
                Ok(PostgreSqlValue::False)
            );
        }

        #[test]
        fn decode_char() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Binary, &PostgreSqlType::Char, &[99]),
                Ok(PostgreSqlValue::String("c".into()))
            );
        }

        #[test]
        fn decode_varchar() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Binary, &PostgreSqlType::VarChar, &[97, 98, 99]),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_smallint() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Binary, &PostgreSqlType::SmallInt, &[0, 1]),
                Ok(PostgreSqlValue::Int16(1))
            );
        }

        #[test]
        fn decode_integer() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Binary, &PostgreSqlType::Integer, &[0, 0, 0, 1]),
                Ok(PostgreSqlValue::Int32(1))
            );
        }

        #[test]
        fn decode_bigint() {
            assert_eq!(
                PostgreSqlValue::decode(
                    &PostgreSqlFormat::Binary,
                    &PostgreSqlType::BigInt,
                    &[0, 0, 0, 0, 0, 0, 0, 1]
                ),
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
                PostgreSqlValue::decode(&PostgreSqlFormat::Text, &PostgreSqlType::Bool, b"true"),
                Ok(PostgreSqlValue::True)
            );
        }

        #[test]
        fn decode_false() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Text, &PostgreSqlType::Bool, b"0"),
                Ok(PostgreSqlValue::False)
            );
        }

        #[test]
        fn decode_char() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Text, &PostgreSqlType::Char, b"c"),
                Ok(PostgreSqlValue::String("c".into()))
            );
        }

        #[test]
        fn decode_varchar() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Text, &PostgreSqlType::VarChar, b"abc"),
                Ok(PostgreSqlValue::String("abc".into()))
            );
        }

        #[test]
        fn decode_smallint() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Text, &PostgreSqlType::SmallInt, b"1"),
                Ok(PostgreSqlValue::Int16(1))
            );
        }

        #[test]
        fn decode_integer() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Text, &PostgreSqlType::Integer, b"123"),
                Ok(PostgreSqlValue::Int32(123))
            );
        }

        #[test]
        fn decode_bigint() {
            assert_eq!(
                PostgreSqlValue::decode(&PostgreSqlFormat::Text, &PostgreSqlType::BigInt, b"123456"),
                Ok(PostgreSqlValue::Int64(123456))
            );
        }
    }
}
