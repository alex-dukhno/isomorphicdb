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

create schema SMOKE_TYPES;

create table SMOKE_TYPES.NUMERIC_TYPES
(
    column_small_int        smallint,
    column_integer          integer,
    column_big_int          bigint,
    column_longest_decimal  decimal(131072, 16383),
    column_shortest_decimal decimal(2, 1),
    column_no_scale_decimal decimal(2),
    column_real             real,
    column_double_precision double precision,
    column_small_serial     smallserial,
    column_serial           serial,
    column_big_serial       bigserial
);

insert into SMOKE_TYPES.NUMERIC_TYPES
values (-32768, -2147483648, -9223372036854775808, -100500.100500, -10.1, -20, -123456.0, -123456789012345.0, 1, 1, 1);
insert into SMOKE_TYPES.NUMERIC_TYPES
values (32767, 2147483647, 9223372036854775807, 100500.100500, 10.9, 30, 123456.0, 123456789012345.0, 32767, 2147483647,
        9223372036854775807);

select *
from SMOKE_TYPES.NUMERIC_TYPES;

drop table SMOKE_TYPES.NUMERIC_TYPES;

create table SMOKE_TYPES.CHARACTER_TYPES
(
    column_no_len_chars      char,
    column_with_len_chars    char(10),
    column_var_char_smallest varchar(1),
    column_var_char_large    varchar(20)
);

insert into SMOKE_TYPES.CHARACTER_TYPES
values ('c', '1234567890', 'c', '12345678901234567890');

insert into SMOKE_TYPES.CHARACTER_TYPES
values ('1', '1234567   ', 'c', '1234567890');

select *
from SMOKE_TYPES.CHARACTER_TYPES;

drop table SMOKE_TYPES.CHARACTER_TYPES;

create table SMOKE_TYPES.BOOL_TYPE
(
    column_boolean boolean
);

insert into SMOKE_TYPES.BOOL_TYPE
values (1);
insert into SMOKE_TYPES.BOOL_TYPE
values (0);
insert into SMOKE_TYPES.BOOL_TYPE
values ('y');
insert into SMOKE_TYPES.BOOL_TYPE
values ('n');
insert into SMOKE_TYPES.BOOL_TYPE
values (true);
insert into SMOKE_TYPES.BOOL_TYPE
values (false);

select *
from SMOKE_TYPES.BOOL_TYPE;

drop table SMOKE_TYPES.BOOL_TYPE;

create table SMOKE_TYPES.DATE_TIME_TYPES
(
    column_date              date,
    column_time              time,
    column_time_with_tz      time with time zone,
    column_timestamp         timestamp,
    column_timestamp_with_tz timestamp with time zone /*,
    column_interval          interval */
);

insert into SMOKE_TYPES.DATE_TIME_TYPES
values ('2020-02-13', '12:01:23', '12:01:23 PST', '1999-01-08 04:05:06', '1999-01-08 04:05:06 PST'/*, '2 year'*/)
