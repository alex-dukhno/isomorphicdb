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

from psycopg2.errors import DuplicateSchema, DuplicateTable
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


def test_create_duplicate_schema(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    with pytest.raises(DuplicateSchema):  # Expects for DuplicateSchema exception
        cur.execute('create schema schema_name;')


def test_create_drop_schema(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create schema schema_name_new;')
    cur.execute('drop schema schema_name_new;')


def test_create_drop_empty_table(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.empty_table();')
    cur.execute('drop table schema_name.empty_table;')


def test_create_duplicated_table(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    with pytest.raises(DuplicateTable):  # Expects for DuplicateTable exception
        cur.execute('create table schema_name.empty_table();')
        cur.execute('create table schema_name.empty_table();')


def test_insert_select(create_cursor, create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column smallint);')

    cur.execute('insert into schema_name.table_name values (%d);' % 1)

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchone()
    assert r == (1,), "fetched unexpected value"


def test_insert_select_many(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column smallint);')

    cur.execute('insert into schema_name.table_name values (%d), (%d), (%d);' % (1, 2, 3))

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(1,), (2,), (3,)]


def test_insert_select_update_all_select(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column smallint);')

    cur.execute('insert into schema_name.table_name values (%d), (%d), (%d);' % (1, 2, 3))

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(1,), (2,), (3,)]

    cur.execute('update schema_name.table_name set si_column = %d;' % 4)
    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(4,), (4,), (4,)]


def test_insert_select_delete_all_select(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column smallint);')

    cur.execute('insert into schema_name.table_name values (%d), (%d), (%d);' % (1, 2, 3))

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(1,), (2,), (3,)]

    cur.execute('delete from schema_name.table_name;')
    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == []


def test_insert_select_many_columns(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

    cur.executemany('insert into schema_name.table_name values (%s, %s, %s);',
                    [(1, 2, 3), (4, 5, 6), (7, 8, 9)])

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(1, 2, 3,), (4, 5, 6,), (7, 8, 9,)]


def test_insert_update_specified_column(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

    cur.executemany('insert into schema_name.table_name values (%s, %s, %s);',
                    [(1, 2, 3), (4, 5, 6), (7, 8, 9)])

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(1, 2, 3,), (4, 5, 6,), (7, 8, 9,)]

    cur.execute('update schema_name.table_name set si_column_2 = %d;' % 10)
    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(1, 10, 3,), (4, 10, 6,), (7, 10, 9,)]


def test_insert_select_reordered(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

    cur.executemany('insert into schema_name.table_name values (%s, %s, %s);',
                [(1, 2, 3), (4, 5, 6), (7, 8, 9)])

    cur.execute('select si_column_3, si_column_1, si_column_2 from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(3, 1, 2,), (6, 4, 5,), (9, 7, 8,)]


def test_insert_select_same_column_many_times(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

    cur.executemany('insert into schema_name.table_name values (%s, %s, %s);',
                [(1, 2, 3), (4, 5, 6), (7, 8, 9)])

    cur.execute('select si_column_3, si_column_1, si_column_2, si_column_1, si_column_3 from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(3, 1, 2, 1, 3), (6, 4, 5, 4, 6,), (9, 7, 8, 7, 9,)]


def test_insert_with_named_columns(create_drop_test_schema_fixture):
    cur = create_drop_test_schema_fixture
    cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

    cur.executemany('insert into schema_name.table_name (si_column_2, si_column_3, si_column_1) values (%s, %s, %s);',
                [(1, 2, 3), (4, 5, 6), (7, 8, 9)])

    cur.execute('select * from schema_name.table_name;')
    r = cur.fetchmany(3)
    assert r == [(3, 1, 2,), (6, 4, 5,), (9, 7, 8,)]
