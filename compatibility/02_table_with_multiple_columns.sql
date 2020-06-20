-- Copyright 2020 Alex Dukhno
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

create schema SMOKE_QUERIES;

create table SMOKE_QUERIES.VALIDATION_TABLE (column_1 int2, column_2 int2, column_3 int2);

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

select column_3, column_2, column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;

drop table SMOKE_QUERIES.VALIDATION_TABLE;

drop schema SMOKE_QUERIES;
