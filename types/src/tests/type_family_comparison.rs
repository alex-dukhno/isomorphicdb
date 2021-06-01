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

use super::*;

#[test]
fn same_types() {
    assert_eq!(
        SqlTypeFamilyOld::SmallInt.compare(&SqlTypeFamilyOld::SmallInt),
        Ok(SqlTypeFamilyOld::SmallInt)
    );
    assert_eq!(
        SqlTypeFamilyOld::Integer.compare(&SqlTypeFamilyOld::Integer),
        Ok(SqlTypeFamilyOld::Integer)
    );
    assert_eq!(SqlTypeFamilyOld::BigInt.compare(&SqlTypeFamilyOld::BigInt), Ok(SqlTypeFamilyOld::BigInt));
    assert_eq!(SqlTypeFamilyOld::Real.compare(&SqlTypeFamilyOld::Real), Ok(SqlTypeFamilyOld::Real));
    assert_eq!(SqlTypeFamilyOld::Double.compare(&SqlTypeFamilyOld::Double), Ok(SqlTypeFamilyOld::Double));
    assert_eq!(SqlTypeFamilyOld::String.compare(&SqlTypeFamilyOld::String), Ok(SqlTypeFamilyOld::String));
    assert_eq!(SqlTypeFamilyOld::Bool.compare(&SqlTypeFamilyOld::Bool), Ok(SqlTypeFamilyOld::Bool));
}

#[cfg(test)]
mod with_higher_in_same_type_group {
    use super::*;

    #[test]
    fn integers() {
        assert_eq!(
            SqlTypeFamilyOld::SmallInt.compare(&SqlTypeFamilyOld::Integer),
            Ok(SqlTypeFamilyOld::Integer)
        );
        assert_eq!(
            SqlTypeFamilyOld::Integer.compare(&SqlTypeFamilyOld::SmallInt),
            Ok(SqlTypeFamilyOld::Integer)
        );

        assert_eq!(
            SqlTypeFamilyOld::SmallInt.compare(&SqlTypeFamilyOld::BigInt),
            Ok(SqlTypeFamilyOld::BigInt)
        );
        assert_eq!(
            SqlTypeFamilyOld::BigInt.compare(&SqlTypeFamilyOld::SmallInt),
            Ok(SqlTypeFamilyOld::BigInt)
        );

        assert_eq!(SqlTypeFamilyOld::Integer.compare(&SqlTypeFamilyOld::BigInt), Ok(SqlTypeFamilyOld::BigInt));
        assert_eq!(SqlTypeFamilyOld::BigInt.compare(&SqlTypeFamilyOld::Integer), Ok(SqlTypeFamilyOld::BigInt));
    }

    #[test]
    fn floats() {
        assert_eq!(SqlTypeFamilyOld::Real.compare(&SqlTypeFamilyOld::Double), Ok(SqlTypeFamilyOld::Double));
        assert_eq!(SqlTypeFamilyOld::Double.compare(&SqlTypeFamilyOld::Real), Ok(SqlTypeFamilyOld::Double));
    }

    #[test]
    fn float_and_integer() {
        assert_eq!(SqlTypeFamilyOld::SmallInt.compare(&SqlTypeFamilyOld::Real), Ok(SqlTypeFamilyOld::Real));
        assert_eq!(SqlTypeFamilyOld::Real.compare(&SqlTypeFamilyOld::SmallInt), Ok(SqlTypeFamilyOld::Real));

        assert_eq!(
            SqlTypeFamilyOld::SmallInt.compare(&SqlTypeFamilyOld::Double),
            Ok(SqlTypeFamilyOld::Double)
        );
        assert_eq!(
            SqlTypeFamilyOld::Double.compare(&SqlTypeFamilyOld::SmallInt),
            Ok(SqlTypeFamilyOld::Double)
        );

        assert_eq!(SqlTypeFamilyOld::Integer.compare(&SqlTypeFamilyOld::Real), Ok(SqlTypeFamilyOld::Real));
        assert_eq!(SqlTypeFamilyOld::Real.compare(&SqlTypeFamilyOld::Integer), Ok(SqlTypeFamilyOld::Real));

        assert_eq!(SqlTypeFamilyOld::Integer.compare(&SqlTypeFamilyOld::Double), Ok(SqlTypeFamilyOld::Double));
        assert_eq!(SqlTypeFamilyOld::Double.compare(&SqlTypeFamilyOld::Integer), Ok(SqlTypeFamilyOld::Double));

        assert_eq!(SqlTypeFamilyOld::BigInt.compare(&SqlTypeFamilyOld::Real), Ok(SqlTypeFamilyOld::Real));
        assert_eq!(SqlTypeFamilyOld::Real.compare(&SqlTypeFamilyOld::BigInt), Ok(SqlTypeFamilyOld::Real));

        assert_eq!(SqlTypeFamilyOld::BigInt.compare(&SqlTypeFamilyOld::Double), Ok(SqlTypeFamilyOld::Double));
        assert_eq!(SqlTypeFamilyOld::Double.compare(&SqlTypeFamilyOld::BigInt), Ok(SqlTypeFamilyOld::Double));
    }
}
