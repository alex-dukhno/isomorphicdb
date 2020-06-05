create schema SMOKE_QUERIES;

create table SMOKE_QUERIES.VALIDATION_TABLE (column_1 smallint, column_2 smallint, column_3 smallint);

insert into SMOKE_QUERIES.VALIDATION_TABLE values (1, 2, 3);

select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;

insert into SMOKE_QUERIES.VALIDATION_TABLE values (4, 5, 6), (7, 8, 9);

select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;

select column_1, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;

select column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;

select column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;

select * from SMOKE_QUERIES.VALIDATION_TABLE;

select column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;

select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;

drop table SMOKE_QUERIES.VALIDATION_TABLE;

drop schema SMOKE_QUERIES;