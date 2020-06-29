import psycopg2 as pg

# mute a row with # symbol to test these cases individually
conn = pg.connect(host="localhost", password="check_this_out")  # connects to default database
conn = pg.connect(host="localhost", password="check_this_out", database="postgres")  # connects to postgres DB