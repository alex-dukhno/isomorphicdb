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
use types::{FloatNumFamily, IntNumFamily, StringFamily, TemporalFamily};

#[cfg(test)]
mod addition {
    use super::*;

    #[cfg(test)]
    mod successfully_inferred_return_type {
        use super::*;

        #[cfg(test)]
        mod integers {
            use super::*;

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::smallint_smallint_smallint(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt)
                ),
                case::smallint_unknown_smallint(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Int(IntNumFamily::SmallInt)
                ),
                case::unknown_smallint_smallint(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt)
                ),
                case::smallint_int_int(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::Integer)
                ),
                case::int_smallint_int(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::Integer)
                ),
                case::smallint_bigint_bigint(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::bigint_smallint_bigint(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::smallint_numerci_numeric(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Numeric, SqlTypeFamily::Numeric),
                case::numeric_smallint_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Numeric),
                case::smallint_real_double(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_smallint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::smallint_double_double(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_smallint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::smallint_date_date(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                ),
                case::date_smallint_date(
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                )
            )]
            fn smallint(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::int_int_int(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::Integer)
                ),
                case::int_unknown_int(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Int(IntNumFamily::Integer)
                ),
                case::unknown_int_int(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::Integer)
                ),
                case::smallint_int_int(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::Integer)
                ),
                case::int_smallint_int(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::Integer)
                ),
                case::int_bigint_bigint(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::bigint_int_bigint(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::int_numerci_numeric(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Numeric, SqlTypeFamily::Numeric),
                case::numeric_int_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Numeric),
                case::int_real_double(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_int_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::int_double_double(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_int_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::int_date_date(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                ),
                case::date_int_date(
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                )
            )]
            fn int(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::bigint_bigint_bigint(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::bigint_unknown_bigint(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::unknown_bigint_bigint(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::int_bigint_bigint(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::bigint_int_bigint(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::smallint_bigint_bigint(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::bigint_smallint_bigint(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Int(IntNumFamily::BigInt)
                ),
                case::bigint_numerci_numeric(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Numeric, SqlTypeFamily::Numeric),
                case::numeric_bigint_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Numeric),
                case::bigint_real_double(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_bigint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::bigint_double_double(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_bigint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                )
            )]
            fn bigint(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }
        }

        #[cfg(test)]
        mod floats {
            use super::*;

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::real_real_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_unknown_real(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Float(FloatNumFamily::Real)
                ),
                case::unknown_real_real(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Real)
                ),
                case::real_numerci_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Numeric,
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::numeric_real_double(
                    SqlTypeFamily::Numeric,
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_double_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_real_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::int_real_double(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_int_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::smallint_real_double(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_smallint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_bigint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::bigint_real_double(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                )
            )]
            fn real(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::double_double_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_unknown_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::unknown_double_double(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_numerci_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Numeric,
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::numeric_double_double(
                    SqlTypeFamily::Numeric,
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::real_double_double(
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_real_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Real),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::int_double_double(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_int_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::smallint_double_double(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_smallint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::double_bigint_double(
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                ),
                case::bigint_double_double(
                    SqlTypeFamily::Int(IntNumFamily::BigInt),
                    SqlTypeFamily::Float(FloatNumFamily::Double),
                    SqlTypeFamily::Float(FloatNumFamily::Double)
                )
            )]
            fn double(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }
        }

        #[rstest::rstest(
            left_type,
            right_type,
            return_type,
            case::numeric_numeric_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Numeric, SqlTypeFamily::Numeric),
            case::double_numerci_double(
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Numeric,
                SqlTypeFamily::Float(FloatNumFamily::Double)
            ),
            case::numeric_unknown_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Unknown, SqlTypeFamily::Numeric),
            case::unknown_numeric_numeric(SqlTypeFamily::Unknown, SqlTypeFamily::Numeric, SqlTypeFamily::Numeric),
            case::numeric_double_double(
                SqlTypeFamily::Numeric,
                SqlTypeFamily::Float(FloatNumFamily::Double),
                SqlTypeFamily::Float(FloatNumFamily::Double)
            ),
            case::real_numeric_double(
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Numeric,
                SqlTypeFamily::Float(FloatNumFamily::Double)
            ),
            case::numeric_real_double(
                SqlTypeFamily::Numeric,
                SqlTypeFamily::Float(FloatNumFamily::Real),
                SqlTypeFamily::Float(FloatNumFamily::Double)
            ),
            case::int_numeric_numeric(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Numeric, SqlTypeFamily::Numeric),
            case::numeric_int_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Numeric),
            case::smallint_numeric_numeric(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Numeric, SqlTypeFamily::Numeric),
            case::numeric_smallint_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Numeric),
            case::numeric_bigint_numeric(SqlTypeFamily::Numeric, SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Numeric),
            case::bigint_numeric_numeric(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Numeric, SqlTypeFamily::Numeric)
        )]
        fn numeric(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
            assert_eq!(
                BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                Ok(return_type)
            )
        }

        #[cfg(test)]
        mod temporal {
            use super::*;

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::date_smallint_date(
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                ),
                case::smallint_date_date(
                    SqlTypeFamily::Int(IntNumFamily::SmallInt),
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                ),
                case::date_int_date(
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                ),
                case::int_date_date(
                    SqlTypeFamily::Int(IntNumFamily::Integer),
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Date)
                ),
                case::date_interval(
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::interval_date(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                )
            )]
            fn date(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::time_interval(
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Time)
                ),
                case::interval_time(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::Time)
                ),
                case::time_timestamp(
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::timestamp_time(
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::time_timestamptz(
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::timestamptz_time(
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::time_date(
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::date_time(
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::time_unknown(
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::Time)
                ),
                case::unknown_time(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::Time),
                    SqlTypeFamily::Temporal(TemporalFamily::Time)
                )
            )]
            fn time(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::timestamp_interval_timestamp(
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::interval_timestamp_timestamp(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::timestamp_unknown_timestamp(
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::unknown_timestamp_timestamp(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                )
            )]
            fn timestamp(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::timestamptz_interval_timestamptz(
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::interval_timestamptz_timestamptz(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::timestamptz_unknown_timestamptz(
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::unknown_timestamptz_timestamptz(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                )
            )]
            fn timestamptz(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                return_type,
                case::interval_interval(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval)
                ),
                case::interval_timestamp(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::timestamp_interval(
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::interval_timestamptz(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::timestamptz_interval(
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::interval_date(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::date_interval(
                    SqlTypeFamily::Temporal(TemporalFamily::Date),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::interval_unknown(
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::Interval)
                ),
                case::unknown_interval(
                    SqlTypeFamily::Unknown,
                    SqlTypeFamily::Temporal(TemporalFamily::Interval),
                    SqlTypeFamily::Temporal(TemporalFamily::Interval)
                )
            )]
            fn interval(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Ok(return_type)
                )
            }
        }

        #[rstest::rstest(
            left_type,
            right_type,
            return_type,
            case::interval_unknown(
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::Interval)
            ),
            case::unknown_interval(
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
                SqlTypeFamily::Temporal(TemporalFamily::Interval)
            ),
            case::unknown_timestamp_timestamp(
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
            ),
            case::timestamp_unknown_timestamp(
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
            ),
            case::unknown_timestamptz_timestamptz(
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
            ),
            case::timestamptz_unknown_timestamptz(
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
            ),
            case::unknown_time_time(
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::Temporal(TemporalFamily::Time)
            ),
            case::time_unknown_time(
                SqlTypeFamily::Temporal(TemporalFamily::Time),
                SqlTypeFamily::Unknown,
                SqlTypeFamily::Temporal(TemporalFamily::Time)
            )
        )]
        fn unknown(left_type: SqlTypeFamily, right_type: SqlTypeFamily, return_type: SqlTypeFamily) {
            assert_eq!(
                BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                Ok(return_type)
            )
        }
    }

    #[cfg(test)]
    mod can_not_infer_return_type {
        use super::*;

        #[rstest::rstest(
            left_type,
            right_type,
            case::unknown_unknown(SqlTypeFamily::Unknown, SqlTypeFamily::Unknown),
            case::unknown_date(SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Date)),
            case::date_unknown(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Unknown),
            case::unknown_char(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::Char)),
            case::char_unknown(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Unknown),
            case::unknown_varchar(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::VarChar)),
            case::varchar_unknown(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Unknown),
            case::unknown_text(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::Text)),
            case::text_unknown(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Unknown),
            case::unknown_bool(SqlTypeFamily::Unknown, SqlTypeFamily::Bool),
            case::bool_unknown(SqlTypeFamily::Bool, SqlTypeFamily::Unknown)
        )]
        fn unknown(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
            assert_eq!(
                BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                Err(UndefinedBiOperatorError {
                    op: BiOperator::Arithmetic(BiArithmetic::Add),
                    left: left_type,
                    right: right_type
                })
            )
        }

        #[cfg(test)]
        mod strings {
            use super::*;

            #[rstest::rstest(
                left_type,
                right_type,
                case::char_char(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::String(StringFamily::Char)),
                case::char_unknown(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Unknown),
                case::unknown_char(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::Char)),
                case::char_date(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_char(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::String(StringFamily::Char)),
                case::char_time(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_char(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::String(StringFamily::Char)),
                case::char_timestamp(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_char(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::String(StringFamily::Char)),
                case::char_timestamptz(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_char(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::String(StringFamily::Char)),
                case::char_interval(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_char(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::String(StringFamily::Char)),
                case::char_varchar(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_char(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::String(StringFamily::Char)),
                case::char_text(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::String(StringFamily::Text)),
                case::text_char(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::Char)),
                case::char_smallint(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                case::smallint_char(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::String(StringFamily::Char)),
                case::char_int(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Int(IntNumFamily::Integer)),
                case::int_char(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::String(StringFamily::Char)),
                case::char_bigint(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_char(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::Char)),
                case::char_real(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_char(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::String(StringFamily::Char)),
                case::char_double(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_char(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::String(StringFamily::Char)),
                case::char_numeric(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Numeric),
                case::numeric_char(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::Char)),
                case::char_bool(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Bool),
                case::bool_char(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::Char))
            )]
            fn char(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                case::varchar_varchar(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_unknown(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Unknown),
                case::unknown_varchar(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_date(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_varchar(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_time(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_varchar(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_timestamp(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_varchar(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_timestamptz(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_varchar(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_interval(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_varchar(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_char(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::String(StringFamily::Char)),
                case::char_varchar(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_text(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::String(StringFamily::Text)),
                case::text_varchar(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_smallint(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                case::smallint_varchar(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_int(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Int(IntNumFamily::Integer)),
                case::int_varchar(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_bigint(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_varchar(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_real(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_varchar(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_double(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_varchar(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_numeric(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Numeric),
                case::numeric_varchar(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_bool(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Bool),
                case::bool_varchar(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::VarChar))
            )]
            fn varchar(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                case::text_text(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::Text)),
                case::text_unknown(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Unknown),
                case::unknown_text(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::Text)),
                case::text_date(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_text(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::String(StringFamily::Text)),
                case::text_time(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_text(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::String(StringFamily::Text)),
                case::text_timestamp(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_text(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::String(StringFamily::Text)),
                case::text_timestamptz(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_text(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::String(StringFamily::Text)),
                case::text_interval(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_text(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::String(StringFamily::Text)),
                case::text_varchar(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_text(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::String(StringFamily::Text)),
                case::text_char(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::Char)),
                case::char_text(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::String(StringFamily::Text)),
                case::text_smallint(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                case::smallint_text(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::String(StringFamily::Text)),
                case::text_int(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::Integer)),
                case::int_text(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::String(StringFamily::Text)),
                case::text_bigint(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_text(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::Text)),
                case::text_real(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_text(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::String(StringFamily::Text)),
                case::text_double(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_text(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::String(StringFamily::Text)),
                case::text_numeric(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Numeric),
                case::numeric_text(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::Text)),
                case::text_bool(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Bool),
                case::bool_text(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::Text))
            )]
            fn text(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }
        }

        #[cfg(test)]
        mod temporal {
            use super::*;

            #[rstest::rstest(
                left_type,
                right_type,
                case::date_date(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_unknown(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Unknown),
                case::unknown_date(SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_timestamp(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_date(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_timestamptz(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_date(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_text(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::String(StringFamily::Text)),
                case::text_date(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_varchar(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_date(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_char(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::String(StringFamily::Char)),
                case::char_date(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_bigint(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_date(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_real(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_date(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_double(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_date(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_numeric(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Numeric),
                case::numeric_date(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_bool(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Bool),
                case::bool_date(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Date))
            )]
            fn date(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                case::time_time(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_text(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::String(StringFamily::Text)),
                case::text_time(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_varchar(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_time(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_char(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::String(StringFamily::Char)),
                case::char_time(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_smallint(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                case::smallint_time(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_int(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::Integer)),
                case::int_time(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_bigint(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_time(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_real(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_time(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_double(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_time(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_numeric(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Numeric),
                case::numeric_time(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Time)),
                case::time_bool(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Bool),
                case::bool_time(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Time))
            )]
            fn time(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                case::timestamp_timestamp(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_date(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_timestamp(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_timestamptz(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_timestamp(
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::timestamp_text(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::String(StringFamily::Text)),
                case::text_timestamp(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_varchar(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_timestamp(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_char(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::Char)),
                case::char_timestamp(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_smallint(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                case::smallint_timestamp(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_int(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::Integer)),
                case::int_timestamp(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_bigint(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_timestamp(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_real(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_timestamp(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_double(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_timestamp(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_numeric(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Numeric),
                case::numeric_timestamp(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                case::timestamp_bool(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Bool),
                case::bool_timestamp(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Timestamp))
            )]
            fn timestamp(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                case::timestamptz_timestamptz(
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::timestamptz_date(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Temporal(TemporalFamily::Date)),
                case::date_timestamptz(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_timestamp(
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp)
                ),
                case::timestamp_timestamptz(
                    SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                    SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)
                ),
                case::timestamptz_text(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::String(StringFamily::Text)),
                case::text_timestamptz(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_varchar(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_timestamptz(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_char(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::String(StringFamily::Char)),
                case::char_timestamptz(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_smallint(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                case::smallint_timestamptz(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_int(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::Integer)),
                case::int_timestamptz(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_bigint(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_timestamptz(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_real(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_timestamptz(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_double(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_timestamptz(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_numeric(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Numeric),
                case::numeric_timestamptz(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                case::timestamptz_bool(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Bool),
                case::bool_timestamptz(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ))
            )]
            fn timestamptz(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }

            #[rstest::rstest(
                left_type,
                right_type,
                case::interval_text(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::String(StringFamily::Text)),
                case::text_interval(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_varchar(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::String(StringFamily::VarChar)),
                case::varchar_interval(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_char(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::String(StringFamily::Char)),
                case::char_interval(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_smallint(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                case::smallint_interval(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_int(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::Integer)),
                case::int_interval(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_bigint(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Int(IntNumFamily::BigInt)),
                case::bigint_interval(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_real(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Float(FloatNumFamily::Real)),
                case::real_interval(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_double(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Float(FloatNumFamily::Double)),
                case::double_interval(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_numeric(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Numeric),
                case::numeric_interval(SqlTypeFamily::Numeric, SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                case::interval_bool(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Bool),
                case::bool_interval(SqlTypeFamily::Bool, SqlTypeFamily::Temporal(TemporalFamily::Interval))
            )]
            fn interval(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
                assert_eq!(
                    BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                    Err(UndefinedBiOperatorError {
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        left: left_type,
                        right: right_type
                    })
                )
            }
        }

        #[rstest::rstest(
            left_type,
            right_type,
            case::text_text(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::Text)),
            case::text_unknown(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Unknown),
            case::unknown_text(SqlTypeFamily::Unknown, SqlTypeFamily::String(StringFamily::Text)),
            case::text_date(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Date)),
            case::date_text(SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::String(StringFamily::Text)),
            case::text_time(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Time)),
            case::time_text(SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::String(StringFamily::Text)),
            case::text_timestamp(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
            case::timestamp_text(SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::String(StringFamily::Text)),
            case::text_timestamptz(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
            case::timestamptz_text(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::String(StringFamily::Text)),
            case::text_interval(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Temporal(TemporalFamily::Interval)),
            case::interval_text(SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::String(StringFamily::Text)),
            case::text_varchar(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::VarChar)),
            case::varchar_text(SqlTypeFamily::String(StringFamily::VarChar), SqlTypeFamily::String(StringFamily::Text)),
            case::text_char(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::String(StringFamily::Char)),
            case::char_text(SqlTypeFamily::String(StringFamily::Char), SqlTypeFamily::String(StringFamily::Text)),
            case::text_smallint(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::SmallInt)),
            case::smallint_text(SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::String(StringFamily::Text)),
            case::text_int(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::Integer)),
            case::int_text(SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::String(StringFamily::Text)),
            case::text_bigint(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Int(IntNumFamily::BigInt)),
            case::bigint_text(SqlTypeFamily::Int(IntNumFamily::BigInt), SqlTypeFamily::String(StringFamily::Text)),
            case::text_real(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Float(FloatNumFamily::Real)),
            case::real_text(SqlTypeFamily::Float(FloatNumFamily::Real), SqlTypeFamily::String(StringFamily::Text)),
            case::text_double(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Float(FloatNumFamily::Double)),
            case::double_text(SqlTypeFamily::Float(FloatNumFamily::Double), SqlTypeFamily::String(StringFamily::Text)),
            case::text_numeric(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Numeric),
            case::numeric_text(SqlTypeFamily::Numeric, SqlTypeFamily::String(StringFamily::Text)),
            case::text_bool(SqlTypeFamily::String(StringFamily::Text), SqlTypeFamily::Bool),
            case::bool_text(SqlTypeFamily::Bool, SqlTypeFamily::String(StringFamily::Text))
        )]
        fn bool(left_type: SqlTypeFamily, right_type: SqlTypeFamily) {
            assert_eq!(
                BiOperator::Arithmetic(BiArithmetic::Add).infer_return_type(left_type, right_type),
                Err(UndefinedBiOperatorError {
                    op: BiOperator::Arithmetic(BiArithmetic::Add),
                    left: left_type,
                    right: right_type
                })
            )
        }
    }
}
