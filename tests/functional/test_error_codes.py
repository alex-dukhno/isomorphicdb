import pytest

from psycopg2._psycopg import cursor

from psycopg2.errors import (NumericValueOutOfRange, NullValueNotAllowed, MostSpecificTypeMismatch, DivisionByZero)
# all imports from errors are OK if you can find such exception class in docs
# >>> https://www.psycopg.org/docs/errors.html

from psycopg2.errorcodes import *
from tests.functional.fixtures import create_drop_test_schema_fixture, create_cursor


def test_numeric_constraint_violations(create_drop_test_schema_fixture: cursor):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_col smallint, i_col integer, bi_col bigint);')
    args = [(-32768, -2147483648, -9223372036854775808),
            (32767, 2147483647, 9223372036854775807)]
    cur.executemany('insert into schema_name.table_name values (%s, %s, %s)', args)

    # await for NumericValueOutOfRange, will throw an error on different exception
    with pytest.raises(NumericValueOutOfRange) as e:
        cur.execute('insert into schema_name.table_name values (%d, %d, %d);' %
                    (32767, 2147483647, 9223372036854775809))
    assert e.value.pgcode == NUMERIC_VALUE_OUT_OF_RANGE  # check for specific exception code

    with pytest.raises(NumericValueOutOfRange) as e:
        cur.execute('insert into schema_name.table_name values (%d, %d, %d);' %
                    (32767, 2147483647, 9223372036854775809))
    assert e.value.pgcode == NUMERIC_VALUE_OUT_OF_RANGE

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchall()
    assert r == [(-32768, -2147483648, -9223372036854775808,), (32767, 2147483647, 9223372036854775807,)]


@pytest.mark.xfail
def test_not_null_constraint_violation(create_drop_test_schema_fixture: cursor):
    """ check that DB can control that NULL value is not allowed for specific column"""

    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(id integer, test integer not null)')

    with pytest.raises(NullValueNotAllowed) as e:
        cur.execute("insert into schema_name.table_name (id) values (%s)", (1,))
    assert e.value.pgcode == NULL_VALUE_NOT_ALLOWED


def test_most_specific_type_mismatch(create_drop_test_schema_fixture: cursor):
    """ check that DB can control that value has specific type"""

    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(id integer, test integer)')

    with pytest.raises(MostSpecificTypeMismatch) as e:
        cur.execute("insert into schema_name.table_name (id, test) values (%s, %s)", (1, 'test_string_not_int'))
    assert e.value.pgcode == MOST_SPECIFIC_TYPE_MISMATCH


@pytest.mark.skip
def test_division_by_zero(create_drop_test_schema_fixture: cursor):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(id integer, test integer)')

    with pytest.raises(DivisionByZero) as e:
        cur.execute("insert into schema_name.table_name (id, test) values (%s, 1/0)", (1,))
    assert e.value.pgcode == DIVISION_BY_ZERO
