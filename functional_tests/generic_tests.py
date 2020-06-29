import psycopg2 as pg
import pytest

@pytest.fixture
def create_conn():
    # conn = pg.connect(host="localhost", password="check_this_out")  # connects to default database
    conn = pg.connect(host="localhost", password="check_this_out", database="postgres")  # connects to postgres DB
    return conn


def test_conn(create_conn):
    conn = create_conn
    assert conn is not None
