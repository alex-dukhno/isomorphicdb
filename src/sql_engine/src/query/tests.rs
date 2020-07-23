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

use crate::query::*;

mod scalar {
    use super::*;

    // #[test]
    // fn scalar_op() {
    //     let scalar = ScalarOp::Column(0);
    //     assert_eq!(scalar, ScalarOp::Column(0));
    // }

    #[test]
    #[ignore]
    fn row_packing_single() {
        let datums = vec![Datum::from_bool(true)];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x1]));
    }

    #[test]
    #[ignore]
    fn row_packing_multiple() {
        let datums = vec![Datum::from_bool(true), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x1, 0x4, 0xa0, 0x86, 0x1, 0x0]));
    }

    #[test]
    #[ignore]
    fn row_packing_with_floats() {
        let datums = vec![
            Datum::from_bool(false),
            Datum::from_i32(100000),
            Datum::from_f64(100.134_212_309_847),
        ];
        let row = Row::pack(&datums);
        assert_eq!(
            row,
            Row::with_data(vec![0x2, 0x4, 0xa0, 0x86, 0x1, 0x0, 0x6, 0xb7, 0x44, 0xc8, 0x42])
        );
    }

    #[test]
    #[ignore]
    fn row_packing_with_null() {
        let datums = vec![Datum::from_bool(true), Datum::from_null(), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x1, 0x0, 0x4, 0xa0, 0x86, 0x1, 0x0]));
    }

    #[test]
    #[ignore]
    fn row_packing_string() {
        let datums = vec![Datum::from_bool(true), Datum::from_str("hello")];
        let row = Row::pack(&datums);
        assert_eq!(
            row,
            Row::with_data(vec![
                0x1, 0x8, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x65, 0x6c, 0x6c, 0x6f
            ])
        );
    }

    #[test]
    #[ignore]
    fn row_unpacking_single() {
        let datums = vec![Datum::from_bool(true)];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

    #[test]
    #[ignore]
    fn row_unpacking_multiple() {
        let datums = vec![Datum::from_bool(true), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

    #[test]
    #[ignore]
    fn row_unpacking_with_floats() {
        let datums = vec![
            Datum::from_bool(false),
            Datum::from_i32(100000),
            Datum::from_f64(100.134_212_309_847),
        ];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

    #[test]
    #[ignore]
    fn row_unpacking_with_null() {
        let datums = vec![Datum::from_bool(true), Datum::from_null(), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

    #[test]
    #[ignore]
    fn row_unpacking_string() {
        let datums = vec![Datum::from_bool(true), Datum::from_str("hello")];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }
}
