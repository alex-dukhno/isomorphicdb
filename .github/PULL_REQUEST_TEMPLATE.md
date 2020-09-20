Closes #(issue number)

#### Description
short description what was changed

#### Is it a feature that change user experience?
Please add compatibility tests or provide possible sql queries that we can add
to our compatibility test suite. Thank you!

#### Client output

e.g. for `psql`

```
psql (12.1, server 0.0.0)
Type "help" for help.

xxx=> create schema schema_name;
CREATE SCHEMA
xxx=> create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);
CREATE TABLE
xxx=> insert into schema_name.table_name (col3, col2, col1) values (1, 2, 3);
INSERT 0 1
xxx=> insert into schema_name.table_name (col3, col2, col1) values (4, 5, 6);
INSERT 0 1
xxx=> select * from schema_name.table_name;
 col1 | col2 | col3
------+------+------
    3 |    2 |    1
    6 |    5 |    4
(2 rows)
```

