use crate::query::*;

mod scalar {
    use super::*;

    // #[test]
    // fn scalar_op() {
    //     let scalar = ScalarOp::Column(0);
    //     assert_eq!(scalar, ScalarOp::Column(0));
    // }

    #[test]
    fn row_packing_single() {
        let datums = vec![Datum::from_bool(true)];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x1]));
    }

    #[test]
    fn row_packing_multiple() {
        let datums = vec![Datum::from_bool(true), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x1, 0x4, 0xa0, 0x86, 0x1, 0x0]));
    }

    #[test]
    fn row_packing_with_floats() {
        let datums = vec![Datum::from_bool(false), Datum::from_i32(100000), Datum::from_f32(100.134212309847)];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x2, 0x4, 0xa0, 0x86, 0x1, 0x0, 0x6, 0xb7, 0x44, 0xc8, 0x42]));
    }

    #[test]
    fn row_packing_with_null() {
        let datums = vec![Datum::from_bool(true), Datum::from_null(), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x1, 0x0, 0x4, 0xa0, 0x86, 0x1, 0x0]));
    }

    #[test]
    fn row_packing_string() {
        let datums = vec![Datum::from_bool(true), Datum::from_str("hello")];
        let row = Row::pack(&datums);
        assert_eq!(row, Row::with_data(vec![0x1, 0x8, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x65, 0x6c, 0x6c, 0x6f]));
    }

    #[test]
    fn row_unpacking_single() {
        let datums = vec![Datum::from_bool(true)];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

        #[test]
    fn row_unpacking_multiple() {
        let datums = vec![Datum::from_bool(true), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

    #[test]
    fn row_unpacking_with_floats() {
        let datums = vec![Datum::from_bool(false), Datum::from_i32(100000), Datum::from_f32(100.134212309847)];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

    #[test]
    fn row_unpacking_with_null() {
        let datums = vec![Datum::from_bool(true), Datum::from_null(), Datum::from_i32(100000)];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }

    #[test]
    fn row_unpacking_string() {
        let datums = vec![Datum::from_bool(true), Datum::from_str("hello")];
        let row = Row::pack(&datums);
        assert_eq!(row.unpack(), datums);
    }
}