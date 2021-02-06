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

#[cfg(test)]
mod arithmetic {
    use super::*;

    #[test]
    fn number_and_number() {
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::Integer), Some(SqlTypeFamily::Integer)),
            true
        );
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::Real), Some(SqlTypeFamily::Integer)),
            true
        );
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::Integer), Some(SqlTypeFamily::Real)),
            true
        );
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::Real), Some(SqlTypeFamily::Real)),
            true
        );
    }

    #[test]
    fn number_and_string() {
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::Integer), Some(SqlTypeFamily::String)),
            false
        );
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::String), Some(SqlTypeFamily::Integer)),
            false
        );
    }

    #[test]
    fn number_and_bool() {
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::Integer), Some(SqlTypeFamily::Bool)),
            false
        );
        assert_eq!(
            BiOperation::Arithmetic(BiArithmetic::Add)
                .supported_type_family(Some(SqlTypeFamily::Bool), Some(SqlTypeFamily::Integer)),
            false
        );
    }
}
