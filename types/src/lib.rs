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

use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SqlTypeFamily {
    Int(IntNumFamily),
    Float(FloatNumFamily),
    String(StringFamily),
    Numeric,
    Bool,
    Unknown,
    Temporal(TemporalFamily),
}

impl Display for SqlTypeFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SqlTypeFamily::Int(int) => write!(f, "{}", int),
            SqlTypeFamily::Float(float) => write!(f, "{}", float),
            SqlTypeFamily::String(string) => write!(f, "{}", string),
            SqlTypeFamily::Numeric => write!(f, "numeric"),
            SqlTypeFamily::Bool => write!(f, "bool"),
            SqlTypeFamily::Unknown => write!(f, "unknown"),
            SqlTypeFamily::Temporal(temporal) => write!(f, "{}", temporal),
        }
    }
}

impl PartialOrd for SqlTypeFamily {
    fn partial_cmp(&self, other: &SqlTypeFamily) -> Option<Ordering> {
        match (self, other) {
            (SqlTypeFamily::Unknown, SqlTypeFamily::Unknown) => Some(Ordering::Equal),
            (SqlTypeFamily::Unknown, _other) => Some(Ordering::Less),
            (SqlTypeFamily::Int(_), SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Int(_), SqlTypeFamily::Bool) => None,
            (SqlTypeFamily::Int(this), SqlTypeFamily::Int(that)) => this.partial_cmp(that),
            (SqlTypeFamily::Int(_), SqlTypeFamily::Float(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Int(_), SqlTypeFamily::Numeric) => Some(Ordering::Less),
            (SqlTypeFamily::Int(_), SqlTypeFamily::String(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Int(_), SqlTypeFamily::Temporal(_)) => None,
            (SqlTypeFamily::Float(_), SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Float(_), SqlTypeFamily::Bool) => None,
            (SqlTypeFamily::Float(_), SqlTypeFamily::Int(_)) => Some(Ordering::Greater),
            (SqlTypeFamily::Float(this), SqlTypeFamily::Float(that)) => this.partial_cmp(that),
            (SqlTypeFamily::Float(_), SqlTypeFamily::Numeric) => Some(Ordering::Greater),
            (SqlTypeFamily::Float(_), SqlTypeFamily::String(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Float(_), SqlTypeFamily::Temporal(_)) => None,
            (SqlTypeFamily::String(this), SqlTypeFamily::String(that)) => this.partial_cmp(that),
            (SqlTypeFamily::String(_), _other) => Some(Ordering::Greater),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Bool) => None,
            (SqlTypeFamily::Numeric, SqlTypeFamily::Int(_)) => Some(Ordering::Greater),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Float(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Numeric) => Some(Ordering::Equal),
            (SqlTypeFamily::Numeric, SqlTypeFamily::String(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(_)) => None,
            (SqlTypeFamily::Bool, SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Bool, SqlTypeFamily::Bool) => Some(Ordering::Equal),
            (SqlTypeFamily::Bool, SqlTypeFamily::String(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Bool, _other) => None,
            (SqlTypeFamily::Temporal(_), SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Temporal(this), SqlTypeFamily::Temporal(that)) => this.partial_cmp(that),
            (SqlTypeFamily::Temporal(_), SqlTypeFamily::String(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Temporal(_), _other) => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord)]
pub enum IntNumFamily {
    SmallInt,
    Integer,
    BigInt,
}

impl Display for IntNumFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            IntNumFamily::SmallInt => write!(f, "smallint"),
            IntNumFamily::Integer => write!(f, "integer"),
            IntNumFamily::BigInt => write!(f, "bigint"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord)]
pub enum FloatNumFamily {
    Real,
    Double,
}

impl Display for FloatNumFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FloatNumFamily::Real => write!(f, "real"),
            FloatNumFamily::Double => write!(f, "double precision"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord)]
pub enum StringFamily {
    Char,
    VarChar,
    Text,
}

impl Display for StringFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            StringFamily::Char => write!(f, "char"),
            StringFamily::VarChar => write!(f, "varchar"),
            StringFamily::Text => write!(f, "text"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum TemporalFamily {
    Time,
    Date,
    Timestamp,
    TimestampTZ,
    Interval,
}

impl PartialOrd for TemporalFamily {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (TemporalFamily::Time, TemporalFamily::Time) => Some(Ordering::Equal),
            (TemporalFamily::Time, _other) => None,
            (TemporalFamily::Date, TemporalFamily::Date) => Some(Ordering::Equal),
            (TemporalFamily::Date, TemporalFamily::Time) => None,
            (TemporalFamily::Date, TemporalFamily::Interval) => None,
            (TemporalFamily::Date, TemporalFamily::Timestamp) => Some(Ordering::Less),
            (TemporalFamily::Date, TemporalFamily::TimestampTZ) => Some(Ordering::Less),
            (TemporalFamily::Timestamp, TemporalFamily::Timestamp) => Some(Ordering::Equal),
            (TemporalFamily::Timestamp, TemporalFamily::Date) => Some(Ordering::Greater),
            (TemporalFamily::Timestamp, TemporalFamily::Time) => None,
            (TemporalFamily::Timestamp, TemporalFamily::Interval) => None,
            (TemporalFamily::Timestamp, TemporalFamily::TimestampTZ) => Some(Ordering::Less),
            (TemporalFamily::TimestampTZ, TemporalFamily::TimestampTZ) => Some(Ordering::Equal),
            (TemporalFamily::TimestampTZ, TemporalFamily::Time) => None,
            (TemporalFamily::TimestampTZ, TemporalFamily::Interval) => None,
            (TemporalFamily::TimestampTZ, TemporalFamily::Date) => Some(Ordering::Greater),
            (TemporalFamily::TimestampTZ, TemporalFamily::Timestamp) => Some(Ordering::Greater),
            (TemporalFamily::Interval, TemporalFamily::Interval) => Some(Ordering::Equal),
            (TemporalFamily::Interval, _other) => None,
        }
    }
}

impl Display for TemporalFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TemporalFamily::Time => write!(f, "time"),
            TemporalFamily::Date => write!(f, "date"),
            TemporalFamily::Timestamp => write!(f, "timestamp"),
            TemporalFamily::TimestampTZ => write!(f, "timestamp with time zone"),
            TemporalFamily::Interval => write!(f, "interval"),
        }
    }
}

#[cfg(test)]
mod ordering {
    use super::*;

    #[rstest::rstest(
        this,
        that,
        expected,
        case::unknown_unknown(SqlTypeFamily::Unknown, SqlTypeFamily::Unknown, Some(Ordering::Equal)),
        case::unknown_bool(SqlTypeFamily::Unknown, SqlTypeFamily::Bool, Some(Ordering::Less)),
        case::bool_unknown(SqlTypeFamily::Bool, SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_numeric(SqlTypeFamily::Unknown, SqlTypeFamily::Numeric, Some(Ordering::Less)),
        case::numeric_unkown(SqlTypeFamily::Numeric, SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_char(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::Char), Some(Ordering::Less)),
        case::char_unknown(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_varchar(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::VarChar), Some(Ordering::Less)),
        case::varchar_unknown(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_text(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::Text), Some(Ordering::Less)),
        case::text_unknown(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_real(SqlTypeFamily::Unknown, SqlTypeFamily::Float(FloatNumFamily::Real), Some(Ordering::Less)),
        case::real_unknown(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_double(SqlTypeFamily::Unknown, SqlTypeFamily::Float(FloatNumFamily::Double), Some(Ordering::Less)),
        case::double_unknown(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_smallint(SqlTypeFamily::Unknown, SqlTypeFamily::Int(IntNumFamily::SmallInt), Some(Ordering::Less)),
        case::smallint_unknown(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_int(SqlTypeFamily::Unknown, SqlTypeFamily::Int(IntNumFamily::Integer), Some(Ordering::Less)),
        case::int_unknown(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_bigint(SqlTypeFamily::Unknown, SqlTypeFamily::Int(IntNumFamily::BigInt), Some(Ordering::Less)),
        case::bigint_unknown(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_time(SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Time), Some(Ordering::Less)),
        case::time_unknown(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_date(SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Date), Some(Ordering::Less)),
        case::date_unknown(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_timestamp(SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Timestamp), Some(Ordering::Less)),
        case::timestamp_unknown(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_timestamp_with_time_zone(SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), Some(Ordering::Less)),
        case::timestamp_with_time_zone(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_interval(SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Interval), Some(Ordering::Less)),
        case::interval_unknown(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Unknown, Some(Ordering::Greater))
    )]
    fn unknown(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
        assert_eq!(this.partial_cmp(&that), expected);
    }

    #[rstest::rstest(
        this,
        that,
        expected,
        case::bool_bool(SqlTypeFamily::Bool, SqlTypeFamily::Bool, Some(Ordering::Equal)),
        case::bool_unknown(SqlTypeFamily::Bool, SqlTypeFamily::Unknown, Some(Ordering::Greater)),
        case::unknown_bool(SqlTypeFamily::Unknown, SqlTypeFamily::Bool, Some(Ordering::Less)),
        case::bool_text(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::Text), Some(Ordering::Less)),
        case::text_bool(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Bool, Some(Ordering::Greater)),
        case::bool_char(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::Char), Some(Ordering::Less)),
        case::char_bool(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Bool, Some(Ordering::Greater)),
        case::bool_varchar(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::VarChar), Some(Ordering::Less)),
        case::varchar_bool(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Bool, Some(Ordering::Greater)),
        case::bool_samllint(SqlTypeFamily::Bool, SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
        case::smallint_bool(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Bool, None),
        case::bool_int(SqlTypeFamily::Bool, SqlTypeFamily::Int(IntNumFamily::Integer), None),
        case::int_bool(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Bool, None),
        case::bool_bigint(SqlTypeFamily::Bool, SqlTypeFamily::Int(IntNumFamily::BigInt), None),
        case::bigint_bool(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Bool, None),
        case::bool_real(SqlTypeFamily::Bool, SqlTypeFamily::Float(FloatNumFamily::Real), None),
        case::real_bool(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Bool, None),
        case::bool_double(SqlTypeFamily::Bool, SqlTypeFamily::Float(FloatNumFamily::Double), None),
        case::double_bool(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Bool, None),
        case::bool_numeric(SqlTypeFamily::Bool, SqlTypeFamily::Numeric, None),
        case::numeric_bool(SqlTypeFamily::Numeric, SqlTypeFamily::Bool, None),
        case::bool_time(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Time), None),
        case::time_bool(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Bool, None),
        case::bool_date(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Date), None),
        case::date_bool(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Bool, None),
        case::bool_timestamp(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
        case::timestamp_bool(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Bool, None),
        case::bool_timestamptz(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
        case::timestamptz_bool(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Bool, None),
        case::bool_interval(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
        case::interval_bool(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Bool, None)
    )]
    fn boolean(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
        assert_eq!(this.partial_cmp(&that), expected);
    }

    #[cfg(test)]
    mod integers {
        use super::*;

        #[rstest::rstest(
            this,
            that,
            expected,
            case::smallint_smallint(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Equal)
            ),
            case::smallint_int(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Less)
            ),
            case::int_smallint(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_bigint(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Less)
            ),
            case::bigint_smallint(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_real(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Less)
            ),
            case::real_smallint(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_double(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::double_smallint(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_numeric(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Numeric, Some(Ordering::Less)),
            case::numeric_smallint(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::SmallInt), Some(Ordering::Greater)),
            case::smallint_char(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_smallint(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_varchar(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_smallint(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_text(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_smallint(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_time(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_smallint(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_date(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_smallint(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_timestamp(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_smallint(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_timestamptz(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_smallint(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_interval(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_smallint(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::SmallInt), None)
        )]
        fn small(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::int_int(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Equal)
            ),
            case::int_smallint(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_int(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Less)
            ),
            case::int_bigint(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Int(IntNumFamily::BigInt), Some(Ordering::Less)),
            case::bigint_int(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_real(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Less)
            ),
            case::real_int(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_double(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::double_int(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_numeric(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Numeric, Some(Ordering::Less)),
            case::numeric_int(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::Integer), Some(Ordering::Greater)),
            case::int_char(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_int(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_varchar(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_int(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_text(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_int(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_time(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_int(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_date(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_int(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_timestamp(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_int(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_timestamptz(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_int(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_interval(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_int(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::Integer), None)
        )]
        fn int(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::bigint_bigint(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Int(IntNumFamily::BigInt), Some(Ordering::Equal)),
            case::bigint_int(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_bigint(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Int(IntNumFamily::BigInt), Some(Ordering::Less)),
            case::bigint_smallint(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_bigint(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Less)
            ),
            case::bigint_real(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Less)
            ),
            case::real_bigint(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_double(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::double_bigint(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_numeric(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Numeric, Some(Ordering::Less)),
            case::numeric_bigint(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::BigInt), Some(Ordering::Greater)),
            case::bigint_char(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::Char), Some(Ordering::Less)),
            case::char_bigint(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_varchar(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_bigint(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_text(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::Text), Some(Ordering::Less)),
            case::text_bigint(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_time(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_bigint(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_date(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_bigint(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_timestamp(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_bigint(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_timestamptz(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_bigint(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_interval(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_bigint(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::BigInt), None)
        )]
        fn big_int(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }
    }

    #[cfg(test)]
    mod floats {
        use super::*;

        #[rstest::rstest(
            this,
            that,
            expected,
            case::real_real(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Equal)
            ),
            case::real_double(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::double_real(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::real_smallint(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_real(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Less)
            ),
            case::real_int(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_real(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Less)
            ),
            case::real_bigint(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_real(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Less)
            ),
            case::real_numeric(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
            case::numeric_real(SqlTypeFamily::Numeric, SqlTypeFamily::Float(FloatNumFamily::Real), Some(Ordering::Less)),
            case::real_bool(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Bool, None),
            case::bool_real(SqlTypeFamily::Bool, SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_char(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_real(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::real_varchar(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_real(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::real_text(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_real(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::real_time(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_real(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_date(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_real(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_timestamp(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_real(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_timestamptz(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_real(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_interval(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_real(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Float(FloatNumFamily::Real), None)
        )]
        fn real(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::double_double(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Equal)
            ),
            case::double_real(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::real_double(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::double_smallint(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_double(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::double_int(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_double(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::double_bigint(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_double(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Less)
            ),
            case::double_numeric(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
            case::numeric_double(SqlTypeFamily::Numeric, SqlTypeFamily::Float(FloatNumFamily::Double), Some(Ordering::Less)),
            case::double_bool(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Bool, None),
            case::bool_double(SqlTypeFamily::Bool, SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_char(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_double(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Greater)
            ),
            case::double_varchar(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_double(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Greater)
            ),
            case::double_text(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_double(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Greater)
            ),
            case::double_time(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_double(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_date(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_double(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_timestamp(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_double(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_timestamptz(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_double(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_interval(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_double(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Float(FloatNumFamily::Double), None)
        )]
        fn double(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }
    }

    #[rstest::rstest(
        this,
        that,
        expected,
        case::numeric_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Numeric, Some(Ordering::Equal)),
        case::numeric_real(SqlTypeFamily::Numeric, SqlTypeFamily::Float(FloatNumFamily::Real), Some(Ordering::Less)),
        case::real_numeric(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
        case::numeric_double(SqlTypeFamily::Numeric, SqlTypeFamily::Float(FloatNumFamily::Double), Some(Ordering::Less)),
        case::double_numeric(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
        case::numeric_smallint(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::SmallInt), Some(Ordering::Greater)),
        case::smallint_numeric(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Numeric, Some(Ordering::Less)),
        case::numeric_int(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::Integer), Some(Ordering::Greater)),
        case::int_numeric(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Numeric, Some(Ordering::Less)),
        case::numeric_bigint(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::BigInt), Some(Ordering::Greater)),
        case::bigint_numeric(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Numeric, Some(Ordering::Less)),
        case::numeric_bool(SqlTypeFamily::Numeric, SqlTypeFamily::Bool, None),
        case::bool_numeric(SqlTypeFamily::Bool, SqlTypeFamily::Numeric, None),
        case::numeric_char(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::Char), Some(Ordering::Less)),
        case::char_numeric(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
        case::numeric_varchar(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::VarChar), Some(Ordering::Less)),
        case::varchar_numeric(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
        case::numeric_text(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::Text), Some(Ordering::Less)),
        case::text_numeric(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
        case::numeric_time(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Time), None),
        case::time_numeric(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Numeric, None),
        case::numeric_date(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Date), None),
        case::date_numeric(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Numeric, None),
        case::numeric_timestamp(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
        case::timestamp_numeric(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Numeric, None),
        case::numeric_timestamptz(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
        case::timestamptz_numeric(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Numeric, None),
        case::numeric_interval(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
        case::interval_numeric(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Numeric, None)
    )]
    fn numeric(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
        assert_eq!(this.partial_cmp(&that), expected);
    }

    #[cfg(test)]
    mod strings {
        use super::*;

        #[rstest::rstest(
            this,
            that,
            expected,
            case::char_char(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Equal)
            ),
            case::char_varchar(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_char(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Greater)
            ),
            case::char_text(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_char(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Greater)
            ),
            case::char_real(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::real_char(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_double(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Greater)
            ),
            case::double_char(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_int(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::int_char(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_int(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_char(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_bigint(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_char(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::Char), Some(Ordering::Less)),
            case::char_bool(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Bool, Some(Ordering::Greater)),
            case::bool_char(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::Char), Some(Ordering::Less)),
            case::char_numeric(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
            case::numeric_char(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::Char), Some(Ordering::Less)),
            case::char_time(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                Some(Ordering::Greater)
            ),
            case::time_char(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_date(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_char(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_timestamp(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_char(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_timestamptz(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Greater)
            ),
            case::timestamptz_char(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_interval(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                Some(Ordering::Greater)
            ),
            case::interval_char(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            )
        )]
        fn char(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::varchar_varchar(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Equal)
            ),
            case::varchar_char(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Greater)
            ),
            case::char_varchar(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_text(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_varchar(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Greater)
            ),
            case::varchar_real(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::real_varchar(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_double(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Greater)
            ),
            case::double_varchar(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_smallint(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_varchar(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_int(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_varchar(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_bigint(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_varchar(
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_bool(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Bool, Some(Ordering::Greater)),
            case::bool_varchar(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::VarChar), Some(Ordering::Less)),
            case::varchar_numeric(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
            case::numeric_varchar(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::VarChar), Some(Ordering::Less)),
            case::varchar_time(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                Some(Ordering::Greater)
            ),
            case::time_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_date(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_timestamp(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_timestamptz(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Greater)
            ),
            case::timestamptz_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_interval(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                Some(Ordering::Greater)
            ),
            case::interval_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            )
        )]
        fn varchar(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::text_text(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Equal)
            ),
            case::text_char(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Greater)
            ),
            case::char_text(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_varchar(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Greater)
            ),
            case::varchar_text(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_real(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Float(FloatNumFamily::Real),
                Some(Ordering::Greater)
            ),
            case::real_text(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_double(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Float(FloatNumFamily::Double),
                Some(Ordering::Greater)
            ),
            case::double_text(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_smallint(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                Some(Ordering::Greater)
            ),
            case::smallint_text(
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_int(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Int(IntNumFamily::Integer),
                Some(Ordering::Greater)
            ),
            case::int_text(
                SqlTypeFamily::Int(IntNumFamily::Integer),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_bigint(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
                Some(Ordering::Greater)
            ),
            case::bigint_text(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::Text), Some(Ordering::Less)),
            case::text_bool(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Bool, Some(Ordering::Greater)),
            case::bool_text(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::Text), Some(Ordering::Less)),
            case::text_numeric(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Numeric, Some(Ordering::Greater)),
            case::numeric_text(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::Text), Some(Ordering::Less)),
            case::text_time(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                Some(Ordering::Greater)
            ),
            case::time_text(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_date(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_text(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_timestamp(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_text(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_timestamptz(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Greater)
            ),
            case::timestamptz_text(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_interval(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                Some(Ordering::Greater)
            ),
            case::interval_text(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            )
        )]
        fn text(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }
    }

    #[cfg(test)]
    mod temporal {
        use super::*;

        #[rstest::rstest(
            this,
            that,
            expected,
            case::date_date(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Equal)
            ),
            case::date_time(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_date(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_timestamp(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Less)
            ),
            case::timestamp_date(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Less)
            ),
            case::timestamptz_date(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_interval(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_date(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_text(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_date(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_char(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_date(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_date(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_real(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_date(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_double(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_date(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_smallint(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_date(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_int(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_date(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_bigint(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_date(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_bool(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Bool, None),
            case::bool_date(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_numeric(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Numeric, None),
            case::numeric_date(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Date), None)
        )]
        fn date(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::time_time(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                Some(Ordering::Equal)
            ),
            case::time_date(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_time(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_timestamp(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_time(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_timestamptz(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_time(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_interval(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_time(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_text(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_time(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                Some(Ordering::Greater)
            ),
            case::time_char(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_time(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                Some(Ordering::Greater)
            ),
            case::time_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_time(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                Some(Ordering::Greater)
            ),
            case::time_real(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_time(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_double(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_time(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_smallint(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_time(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_int(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_time(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_bigint(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_time(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_bool(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Bool, None),
            case::bool_time(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_numeric(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Numeric, None),
            case::numeric_time(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Time), None)
        )]
        fn time(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::timestamp_timestamp(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Equal)
            ),
            case::timestamp_time(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_timestamp(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_date(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_timestamp(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Less)
            ),
            case::timestamp_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Less)
            ),
            case::timestamptz_timestamp(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_interval(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                None
            ),
            case::interval_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                None
            ),
            case::timestamp_text(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_timestamp(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_char(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_timestamp(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_timestmap(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_real(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_timestamp(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_real(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_timestamp(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestmap_smallint(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_timestamp(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_int(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_timestamp(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_bigint(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_timestamp(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_bool(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Bool, None),
            case::bool_timestamp(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None),
            case::timestamp_numeric(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Numeric, None),
            case::numeric_timestamp(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Timestamp), None)
        )]
        fn timestamp(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::timestamptz_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Equal)
            ),
            case::timestamptz_date(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                Some(Ordering::Greater)
            ),
            case::date_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::Date),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Less)
            ),
            case::timestamptz_time(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_timestamptz(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_timestamp(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                Some(Ordering::Greater)
            ),
            case::timestamp_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Less)
            ),
            case::timestamptz_interval(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                None
            ),
            case::interval_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                None
            ),
            case::timestamptz_text(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_timestamptz(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Greater)
            ),
            case::timestamptz_char(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_timestamptz(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Greater)
            ),
            case::timestamptz_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_timestamptz(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                Some(Ordering::Greater)
            ),
            case::timestamptz_real(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_timestamptz(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_double(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_timestamptz(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_smallint(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_timestamptz(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_int(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_timestamptz(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_bigint(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_timestamptz(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_bool(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Bool, None),
            case::bool_timestamptz(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None),
            case::timestamptz_numeric(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Numeric, None),
            case::numeric_timestamptz(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), None)
        )]
        fn timestamptz(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }

        #[rstest::rstest(
            this,
            that,
            expected,
            case::interval_interval(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                Some(Ordering::Equal)
            ),
            case::interval_date(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Date), None),
            case::date_interval(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_time(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Time), None),
            case::time_interval(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_timestamp(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                None
            ),
            case::timestamp_interval(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                None
            ),
            case::interval_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                None
            ),
            case::timestamptz_interval(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                None
            ),
            case::interval_text(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::String(StringFamily::Text),
                Some(Ordering::Less)
            ),
            case::text_interval(
                SqlTypeFamily::String(StringFamily::Text),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                Some(Ordering::Greater)
            ),
            case::interval_char(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::String(StringFamily::Char),
                Some(Ordering::Less)
            ),
            case::char_interval(
                SqlTypeFamily::String(StringFamily::Char),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                Some(Ordering::Greater)
            ),
            case::interval_varchar(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::String(StringFamily::VarChar),
                Some(Ordering::Less)
            ),
            case::varchar_interval(
                SqlTypeFamily::String(StringFamily::VarChar),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                Some(Ordering::Greater)
            ),
            case::interval_real(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Float(FloatNumFamily::Real), None),
            case::real_interval(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_double(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Float(FloatNumFamily::Double), None),
            case::double_interval(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_smallint(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::SmallInt), None),
            case::smallint_interval(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_int(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::Integer), None),
            case::int_interval(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_bigint(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::BigInt), None),
            case::bigint_interval(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::interval_bool(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Bool, None),
            case::bool_interval(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Interval), None),
            case::inteval_numeric(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Numeric, None),
            case::numeric_interval(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Interval), None)
        )]
        fn interval(this: SqlTypeFamily, that: SqlTypeFamily, expected: Option<Ordering>) {
            assert_eq!(this.partial_cmp(&that), expected);
        }
    }
}
