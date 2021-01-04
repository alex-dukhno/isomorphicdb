-- Copyright 2020 - present Alex Dukhno
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
    column_big_int          bigint
);

insert into SMOKE_TYPES.NUMERIC_TYPES
values (-32768, -2147483648, -9223372036854775808);
insert into SMOKE_TYPES.NUMERIC_TYPES
values (32767, 2147483647, 9223372036854775807);

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

drop schema SMOKE_TYPES;
