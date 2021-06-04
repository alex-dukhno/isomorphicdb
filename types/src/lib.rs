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
            (SqlTypeFamily::Int(_), SqlTypeFamily::String(_)) => None,
            (SqlTypeFamily::Float(_), SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Float(_), SqlTypeFamily::Bool) => None,
            (SqlTypeFamily::Float(_), SqlTypeFamily::Int(_)) => Some(Ordering::Greater),
            (SqlTypeFamily::Float(this), SqlTypeFamily::Float(that)) => this.partial_cmp(that),
            (SqlTypeFamily::Float(_), SqlTypeFamily::Numeric) => Some(Ordering::Greater),
            (SqlTypeFamily::Float(_), SqlTypeFamily::String(_)) => None,
            (SqlTypeFamily::String(_), SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::String(_), SqlTypeFamily::Bool) => None,
            (SqlTypeFamily::String(_), SqlTypeFamily::Int(_)) => None,
            (SqlTypeFamily::String(_), SqlTypeFamily::Float(_)) => None,
            (SqlTypeFamily::String(_), SqlTypeFamily::Numeric) => None,
            (SqlTypeFamily::String(this), SqlTypeFamily::String(that)) => this.partial_cmp(that),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Bool) => None,
            (SqlTypeFamily::Numeric, SqlTypeFamily::Int(_)) => Some(Ordering::Greater),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Float(_)) => Some(Ordering::Less),
            (SqlTypeFamily::Numeric, SqlTypeFamily::Numeric) => Some(Ordering::Equal),
            (SqlTypeFamily::Numeric, SqlTypeFamily::String(_)) => None,
            (SqlTypeFamily::Bool, SqlTypeFamily::Unknown) => Some(Ordering::Greater),
            (SqlTypeFamily::Bool, SqlTypeFamily::Bool) => Some(Ordering::Equal),
            (SqlTypeFamily::Bool, _other) => None,
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

#[cfg(test)]
mod ordering {
    use super::*;

    #[test]
    fn unknown() {
        assert_eq!(SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Unknown), Some(Ordering::Equal));

        assert_eq!(SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Bool), Some(Ordering::Less));
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Unknown), Some(Ordering::Greater));

        assert_eq!(SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Numeric), Some(Ordering::Less));
        assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Unknown), Some(Ordering::Greater));

        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );

        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );

        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Unknown),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn boolean() {
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Bool), Some(Ordering::Equal));

        assert_eq!(SqlTypeFamily::Unknown.partial_cmp(&SqlTypeFamily::Bool), Some(Ordering::Less));
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Unknown), Some(Ordering::Greater));

        assert_eq!(SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::String(StringFamily::Text)), None);
        assert_eq!(SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::String(StringFamily::Char)), None);
        assert_eq!(SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)), None);

        assert_eq!(SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)), None);
        assert_eq!(SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)), None);
        assert_eq!(SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)), None);

        assert_eq!(SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)), None);
        assert_eq!(SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)), None);

        assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Numeric), None);
    }

    #[cfg(test)]
    mod integers {
        use super::*;

        #[test]
        fn small() {
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Numeric),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                None
            );
        }

        #[test]
        fn int() {
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Numeric),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                None
            );
        }

        #[test]
        fn big_int() {
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Less)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Numeric),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                None
            );
        }
    }

    #[cfg(test)]
    mod floats {
        use super::*;

        #[test]
        fn real() {
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Less)
            );

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Numeric),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Less)
            );

            assert_eq!(SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Bool), None);
            assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)), None);

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                None
            );
        }

        #[test]
        fn double() {
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Numeric),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                Some(Ordering::Less)
            );

            assert_eq!(SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Bool), None);
            assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)), None);

            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                None
            );
        }
    }

    #[test]
    fn numeric() {
        assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Numeric), Some(Ordering::Equal));

        assert_eq!(
            SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::Numeric),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::Numeric),
            Some(Ordering::Greater)
        );

        assert_eq!(
            SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::Numeric),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::Numeric),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::Numeric),
            Some(Ordering::Less)
        );

        assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::Bool), None);
        assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::Numeric), None);

        assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::String(StringFamily::Char)), None);
        assert_eq!(SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Numeric), None);
        assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)), None);
        assert_eq!(SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Numeric), None);
        assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::String(StringFamily::Text)), None);
        assert_eq!(SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Numeric), None);
    }

    #[cfg(test)]
    mod strings {
        use super::*;

        #[test]
        fn char() {
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                None
            );

            assert_eq!(SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Bool), None);
            assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::String(StringFamily::Char)), None);

            assert_eq!(SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::Numeric), None);
            assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::String(StringFamily::Char)), None);
        }

        #[test]
        fn varchar() {
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                Some(Ordering::Greater)
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                None
            );

            assert_eq!(SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Bool), None);
            assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)), None);

            assert_eq!(SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::Numeric), None);
            assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)), None);
        }

        #[test]
        fn text() {
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                Some(Ordering::Equal)
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::String(StringFamily::Char)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Char).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                Some(Ordering::Less)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::String(StringFamily::VarChar)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::VarChar).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                Some(Ordering::Less)
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Real)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Real).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Float(FloatNumFamily::Double)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Float(FloatNumFamily::Double).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );

            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::SmallInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::Integer)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::Integer).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );
            assert_eq!(
                SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Int(IntNumFamily::BigInt)),
                None
            );
            assert_eq!(
                SqlTypeFamily::Int(IntNumFamily::BigInt).partial_cmp(&SqlTypeFamily::String(StringFamily::Text)),
                None
            );

            assert_eq!(SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Bool), None);
            assert_eq!(SqlTypeFamily::Bool.partial_cmp(&SqlTypeFamily::String(StringFamily::Text)), None);

            assert_eq!(SqlTypeFamily::String(StringFamily::Text).partial_cmp(&SqlTypeFamily::Numeric), None);
            assert_eq!(SqlTypeFamily::Numeric.partial_cmp(&SqlTypeFamily::String(StringFamily::Text)), None);
        }
    }
}
