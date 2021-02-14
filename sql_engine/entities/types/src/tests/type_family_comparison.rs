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

use super::*;

#[test]
fn same_types() {
    assert_eq!(
        SqlTypeFamily::SmallInt.compare(&SqlTypeFamily::SmallInt),
        Ok(SqlTypeFamily::SmallInt)
    );
    assert_eq!(
        SqlTypeFamily::Integer.compare(&SqlTypeFamily::Integer),
        Ok(SqlTypeFamily::Integer)
    );
    assert_eq!(
        SqlTypeFamily::BigInt.compare(&SqlTypeFamily::BigInt),
        Ok(SqlTypeFamily::BigInt)
    );
    assert_eq!(
        SqlTypeFamily::Real.compare(&SqlTypeFamily::Real),
        Ok(SqlTypeFamily::Real)
    );
    assert_eq!(
        SqlTypeFamily::Double.compare(&SqlTypeFamily::Double),
        Ok(SqlTypeFamily::Double)
    );
    assert_eq!(
        SqlTypeFamily::String.compare(&SqlTypeFamily::String),
        Ok(SqlTypeFamily::String)
    );
    assert_eq!(
        SqlTypeFamily::Bool.compare(&SqlTypeFamily::Bool),
        Ok(SqlTypeFamily::Bool)
    );
}

#[cfg(test)]
mod with_higher_in_same_type_group {
    use super::*;

    #[test]
    fn integers() {
        assert_eq!(
            SqlTypeFamily::SmallInt.compare(&SqlTypeFamily::Integer),
            Ok(SqlTypeFamily::Integer)
        );
        assert_eq!(
            SqlTypeFamily::Integer.compare(&SqlTypeFamily::SmallInt),
            Ok(SqlTypeFamily::Integer)
        );

        assert_eq!(
            SqlTypeFamily::SmallInt.compare(&SqlTypeFamily::BigInt),
            Ok(SqlTypeFamily::BigInt)
        );
        assert_eq!(
            SqlTypeFamily::BigInt.compare(&SqlTypeFamily::SmallInt),
            Ok(SqlTypeFamily::BigInt)
        );

        assert_eq!(
            SqlTypeFamily::Integer.compare(&SqlTypeFamily::BigInt),
            Ok(SqlTypeFamily::BigInt)
        );
        assert_eq!(
            SqlTypeFamily::BigInt.compare(&SqlTypeFamily::Integer),
            Ok(SqlTypeFamily::BigInt)
        );
    }

    #[test]
    fn floats() {
        assert_eq!(
            SqlTypeFamily::Real.compare(&SqlTypeFamily::Double),
            Ok(SqlTypeFamily::Double)
        );
        assert_eq!(
            SqlTypeFamily::Double.compare(&SqlTypeFamily::Real),
            Ok(SqlTypeFamily::Double)
        );
    }

    #[test]
    fn float_and_integer() {
        assert_eq!(
            SqlTypeFamily::SmallInt.compare(&SqlTypeFamily::Real),
            Ok(SqlTypeFamily::Real)
        );
        assert_eq!(
            SqlTypeFamily::Real.compare(&SqlTypeFamily::SmallInt),
            Ok(SqlTypeFamily::Real)
        );

        assert_eq!(
            SqlTypeFamily::SmallInt.compare(&SqlTypeFamily::Double),
            Ok(SqlTypeFamily::Double)
        );
        assert_eq!(
            SqlTypeFamily::Double.compare(&SqlTypeFamily::SmallInt),
            Ok(SqlTypeFamily::Double)
        );

        assert_eq!(
            SqlTypeFamily::Integer.compare(&SqlTypeFamily::Real),
            Ok(SqlTypeFamily::Real)
        );
        assert_eq!(
            SqlTypeFamily::Real.compare(&SqlTypeFamily::Integer),
            Ok(SqlTypeFamily::Real)
        );

        assert_eq!(
            SqlTypeFamily::Integer.compare(&SqlTypeFamily::Double),
            Ok(SqlTypeFamily::Double)
        );
        assert_eq!(
            SqlTypeFamily::Double.compare(&SqlTypeFamily::Integer),
            Ok(SqlTypeFamily::Double)
        );

        assert_eq!(
            SqlTypeFamily::BigInt.compare(&SqlTypeFamily::Real),
            Ok(SqlTypeFamily::Real)
        );
        assert_eq!(
            SqlTypeFamily::Real.compare(&SqlTypeFamily::BigInt),
            Ok(SqlTypeFamily::Real)
        );

        assert_eq!(
            SqlTypeFamily::BigInt.compare(&SqlTypeFamily::Double),
            Ok(SqlTypeFamily::Double)
        );
        assert_eq!(
            SqlTypeFamily::Double.compare(&SqlTypeFamily::BigInt),
            Ok(SqlTypeFamily::Double)
        );
    }
}
