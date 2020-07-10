# Copyright 2020 Alex Dukhno
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import psycopg2 as pg
import pytest

from psycopg2._psycopg import connection, cursor


@pytest.fixture(scope="session", autouse=True)
def create_cursor(request):

    # ToDo - connection process test is not ideal yet.
    conn = pg.connect(host="localhost", password="check_this_out", database="postgres")
    assert isinstance(conn, connection), "Failed to connect to DB"

    # ToDo - connection process test is not ideal yet.
    cur = conn.cursor()
    assert isinstance(cur, cursor)

    def close_all():
        cur.close()
        conn.close()

    request.addfinalizer(close_all)

    return cur


@pytest.fixture(scope='function')
def create_drop_test_schema_fixture(request, create_cursor):
    cur = create_cursor
    cur.execute('create schema schema_name;')

    def close_all():
        cur.execute("drop schema schema_name cascade;")

    request.addfinalizer(close_all)
    return cur


def test_integer_types(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_col smallint, i_col integer, bi_col bigint);')

    args = [(-32768, -2147483648, -9223372036854775808),
            (32767, 2147483647, 9223372036854775807)]
    cur.executemany('insert into schema_name.table_name values (%s, %s, %s)', args)

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(2)
    assert r == [(-32768, -2147483648, -9223372036854775808,), (32767, 2147483647, 9223372036854775807,)]


def test_character_types(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute(
        'create table schema_name.table_name(\
            col_no_len_chars char,\
            col_with_len_chars char(10),\
            col_var_char_smallest varchar(1),\
            col_var_char_large    varchar(20)\
            );')
    args = [('c', '1234567890', 'c', '12345678901234567890'),
            ('1', '1234567   ', 'c', '1234567890')]
    query = 'insert into schema_name.table_name values (%s, %s, %s, %s);'
    cur.executemany(query, args)

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(2)
    assert r == [('c', '1234567890', 'c', '12345678901234567890',), ('1', '1234567', 'c', '1234567890',)]


def test_boolean_types(create_drop_test_schema_fixture):
    import random
    cur = create_drop_test_schema_fixture

    cur.execute(
        'CREATE TABLE schema_name.table_name('
        '   col boolean'
        ');'
    )

    word_to_value = {
        "TRUE": True,
        "FALSE": False,
        "'true'": True,
        "'false'": False,
        "'t'": True,
        "'f'": False,
        "'yes'": True,
        "'no'": False,
        "'y'": True,
        "'n'": False,
        "'on'": True,
        "'off'": False,
        "'1'": True,
        "'0'": False,
        "TRUE::boolean": True,
        "FALSE::boolean": False,
        "'yes'::boolean": True,
        "'no'::boolean": False,
    }

    for (w, outcome) in word_to_value.items():
        # it should work regardless of case so we randomly generate
        # a string, which have both upper and lower case letters
        random_case_w = "".join(random.choice([k.upper(), k]) for k in w)
        cur.execute('INSERT INTO schema_name.table_name VALUES(%s);' % random_case_w)
        cur.execute('SELECT * FROM schema_name.table_name;')
        r = cur.fetchmany(1)
        assert r == [(outcome, )], "Failed for value: %s" % random_case_w
        cur.execute('DELETE FROM schema_name.table_name;')


def test_numeric_constraint_violations(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_col smallint, i_col integer, bi_col bigint);')
    args = [(-32768, -2147483648, -9223372036854775808),
            (32767, 2147483647, 9223372036854775807)]
    cur.executemany('insert into schema_name.table_name values (%s, %s, %s)', args)

    try:
        cur.execute('insert into schema_name.table_name values (%d, %d, %d);' % (32767, 2147483647, 9223372036854775809))
    except pg.errors.NumericValueOutOfRange as e:
        assert e.pgcode == '22003'
        # assert e.pgerror == "ERROR: bigint out of range"
    except pg.Error:
        assert False

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchall()
    assert r == [(-32768, -2147483648, -9223372036854775808,), (32767, 2147483647, 9223372036854775807,)]


def test_many_numeric_constraint_violations(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_col smallint, i_col integer, bi_col bigint);')
    args = [(-32768, -2147483648, -9223372036854775808),
            (32767, 2147483647, 9223372036854775807)]
    cur.executemany('insert into schema_name.table_name values (%s, %s, %s)', args)

    try:
        cur.execute('insert into schema_name.table_name values (%d, %d, %d);' % (33767, 2147483647, 9223372036854775809))
    except pg.errors.NumericValueOutOfRange as e:
        #  This is generating two error messages but it seems it this script only sees the first one.
        assert e.pgcode == '22003'
    except pg.Error:
        assert False

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchall()
    assert r == [(-32768, -2147483648, -9223372036854775808,), (32767, 2147483647, 9223372036854775807,)]


def test_math_operations_in_insert(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_col smallint);')
    query = 'insert into schema_name.table_name values (%d %s %d)'
    args = [(3, '+', 5), (3, '-', 5), (3, '*', 5), (15, '/', 5)]
    for arg in args:
        cur.execute(query % arg)

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchall()

    assert r == [(8,), (-2,), (15, ), (3,)]
