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


@pytest.fixture(scope="session", autouse=True)
def create_cursor(request):
    try:
        conn = pg.connect(host="localhost", password="check_this_out", database="postgres")
        cur = conn.cursor()

        def close_all():
            conn.close()
            cur.close()

        request.addfinalizer(close_all)
    except Exception as e:
        assert False, str(e)
    return cur


def test_create_drop_schema(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
    finally:
        cur.execute('drop schema schema_name;')


def test_create_drop_empty_table(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.empty_table();')
    finally:
        cur.execute('drop table schema_name.empty_table;')
        cur.execute('drop schema schema_name;')


def test_insert_select(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column smallint);')

        cur.execute('insert into schema_name.table_name values (%d);' % 1)

        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchone()
        assert r == (1,)
    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')


def test_insert_select_many(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column smallint);')

        cur.execute('insert into schema_name.table_name values (%d), (%d), (%d);' % (1, 2, 3))

        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(1,), (2,), (3,)]
    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')


def test_insert_select_update_all_select(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column smallint);')

        cur.execute('insert into schema_name.table_name values (%d), (%d), (%d);' % (1, 2, 3))

        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(1,), (2,), (3,)]

        cur.execute('update schema_name.table_name set si_column = %d;' % 4)
        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(4,), (4,), (4,)]
    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')


def test_insert_select_delete_all_select(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column smallint);')

        cur.execute('insert into schema_name.table_name values (%d), (%d), (%d);' % (1, 2, 3))

        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(1,), (2,), (3,)]

        cur.execute('delete from schema_name.table_name;')
        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == []
    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')


def test_insert_select_many_columns(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

        for t in [(1, 2, 3), (4, 5, 6), (7, 8, 9)]:
            cur.execute('insert into schema_name.table_name values (%s, %s, %s);' % t)

        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(1, 2, 3,), (4, 5, 6,), (7, 8, 9,)]
    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')


def test_insert_update_specified_column(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

        for t in [(1, 2, 3), (4, 5, 6), (7, 8, 9)]:
            cur.execute('insert into schema_name.table_name values (%s, %s, %s);' % t)

        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(1, 2, 3,), (4, 5, 6,), (7, 8, 9,)]

        cur.execute('update schema_name.table_name set si_column_2 = %d;' % 10)
        cur.execute('select * from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(1, 10, 3,), (4, 10, 6,), (7, 10, 9,)]
    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')


def test_insert_select_reordered(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

        for t in [(1, 2, 3), (4, 5, 6), (7, 8, 9)]:
            cur.execute('insert into schema_name.table_name values (%s, %s, %s);' % t)

        cur.execute('select si_column_3, si_column_1, si_column_2 from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(3, 1, 2,), (6, 4, 5,), (9, 7, 8,)]

    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')


def test_insert_select_same_column_many_times(create_cursor):
    cur = create_cursor
    try:
        cur.execute('create schema schema_name;')
        cur.execute('create table schema_name.table_name(si_column_1 smallint, si_column_2 smallint, si_column_3 smallint);')

        for t in [(1, 2, 3), (4, 5, 6), (7, 8, 9)]:
            cur.execute('insert into schema_name.table_name values (%s, %s, %s);' % t)

        cur.execute('select si_column_3, si_column_1, si_column_2, si_column_1, si_column_3 from schema_name.table_name;')
        r = cur.fetchmany(3)
        assert r == [(3, 1, 2, 1, 3), (6, 4, 5, 4, 6,), (9, 7, 8, 7, 9,)]

    finally:
        cur.execute('drop table schema_name.table_name;')
        cur.execute('drop schema schema_name;')
