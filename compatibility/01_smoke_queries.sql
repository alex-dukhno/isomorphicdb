create schema SMOKE_QUERIES;

create table SMOKE_QUERIES.VALIDATION_TABLE
(
    column_test smallint
);

insert into SMOKE_QUERIES.VALIDATION_TABLE
values (1);

select column_test
from SMOKE_QUERIES.VALIDATION_TABLE;

update SMOKE_QUERIES.VALIDATION_TABLE
set column_test = 2;

select column_test
from SMOKE_QUERIES.VALIDATION_TABLE;

delete
from SMOKE_QUERIES.VALIDATION_TABLE;

select column_test
from SMOKE_QUERIES.VALIDATION_TABLE;

drop table SMOKE_QUERIES.VALIDATION_TABLE;

drop schema SMOKE_QUERIES;
