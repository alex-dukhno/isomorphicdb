import pytest

from psycopg2._psycopg import connection, cursor
from psycopg2 import connect


@pytest.fixture(scope="session", autouse=True)
def create_cursor(request) -> cursor:

    conn = connect(host="localhost", password="check_this_out", database="postgres")
    assert isinstance(conn, connection), "Failed to connect to DB"

    cur = conn.cursor()
    assert isinstance(cur, cursor)

    def close_all():
        cur.close()
        conn.close()

    request.addfinalizer(close_all)

    return cur


@pytest.fixture(scope='function')
def create_drop_test_schema_fixture(request, create_cursor) -> cursor:
    cur = create_cursor
    # ToDo
    # cur.execute("drop schema if exists schema_name cascade;")
    cur.execute('create schema schema_name;')

    def close_all():
        cur.execute("drop schema schema_name cascade;")

    request.addfinalizer(close_all)
    return cur