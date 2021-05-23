 * Feature Name: Type System
 * Authors: Alex Dukhno (@alex-dukhno)
 * RFC PR: [#595](https://github.com/alex-dukhno/isomorphicdb/pull/595)
 * RFC Tracking Issue: [#598](https://github.com/alex-dukhno/isomorphicdb/issues/598)

# Summary

This RFC aims to:
 * improve inference, type check and type coercion of existing types with supported operators.
 * extend type system with new types like `decimal` (`numeric`), float types `float`, `double precision`, temporal types
`date`, `time`, `timestamp`, `timestamp with time zone` and serials `smallserial`, `serial`, `bigserial`
 * add supported for relational operators

Quick research on `prepared statement` showed that this is required functionality to fully support `prepared statement`.
Possibly it impacts `extended query protocol` and future research for workloads forecasting.

# Motivation and background

## PostgreSQL Arithmetic operations

### Numbers and literals

PostgreSQL converts implicitly string into number if one of the parameter is a number

```sql
select 1 + '1';
 ?column?
----------
        2
(1 row)

select '1' + 1;
?column?
----------
        2
(1 row)
```

However, fails when both of them are literals

```sql
select '1' + '1';
ERROR:  42725: operator is not unique: unknown + unknown
LINE 1: select '1' + '1';
                   ^
HINT:  Could not choose a best candidate operator. You might need to add explicit type casts.
```

#### String literals

Implicit and explicit cast of string literals give same results:

```sql
select 1 + 'abc';
ERROR:  22P02: invalid input syntax for type integer: "abc"
LINE 1: select 1 + 'abc';
                   ^

select 1 + cast('abc' as int);
ERROR:  22P02: invalid input syntax for type integer: "abc"
LINE 1: select 1 + cast('abc' as int);
```

#### Date literals

Date literals and numbers works with explicit cast. Implicit cast is not supported for date literals in any position.

```sql
select 1 + cast('2021-01-01' as date);
?column?
------------
 2021-01-02
(1 row)

select 1 + '2021-01-01';
ERROR:  22P02: invalid input syntax for type integer: "2021-01-01"
LINE 1: select 1 + '2021-01-01';
                   ^
```

Subtraction works only if number is the second argument

```sql
select 1 - cast('2021-01-01' as date);
ERROR:  42883: operator does not exist: integer - date
LINE 1: select 1 - cast('2021-01-01' as date);
                 ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.

select cast('2021-01-01' as date) - 1;
  ?column?
------------
 2020-12-31
(1 row)
```

Addition and subtraction operates with days. `+` next day, `-` previous day.  
Other arithmetic operations are not supported

#### Timestamp literals

Operations of timestamps and numbers are not supported with neither implicit nor explicit casts.

```sql
select cast('2021-05-16 12:24:07' as timestamp) + 1;
ERROR:  42883: operator does not exist: timestamp without time zone + integer
LINE 1: select cast('2021-05-16 12:24:07' as timestamp) + 1;
                                                        ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
```

#### Time literals

Operations of time and numbers are not supported with neither implicit nor explicit casts.

```sql
select cast('12:24:07' as time) + 1;
ERROR:  42883: operator does not exist: time without time zone + integer
LINE 1: select cast('12:24:07' as time) + 1;
                                        ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
```

### Number and boolean

PostgreSQL does not convert implicitly boolean to numbers, however, explicit cast helps

```sql
select 1 + true;
ERROR:  42883: operator does not exist: integer + boolean
LINE 1: select 1 + true;
                 ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.

select 1 + cast(true as int);
 ?column?
----------
        2
(1 row)
```

#### Overflow handling

Numeric literal implicitly has `integer` type unless first parameter is in a `bigint` range.

```sql
select 32767 + 2147483647;
ERROR:  22003: integer out of range

select 32767 + 32767;
 ?column?
----------
    65534
(1 row)

select pg_typeof(32767 + 32767);
pg_typeof
-----------
integer
(1 row)

select 9223372036854775 + 2147483647;
?column?
------------------
 9223374184338422
(1 row)

select pg_typeof(9223372036854775 + 2147483647);
pg_typeof
-----------
bigint
(1 row)
```

Number literals with floating-point explicitly converted into `numeric` type

```sql
select pg_typeof(123.123);
 pg_typeof
-----------
 numeric
(1 row)
```

### Bitwise operation

Supported only for integer types.

```sql
select 1 & true;
ERROR:  42883: operator does not exist: integer & boolean
LINE 1: select 1 & true;
                 ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
```

```sql
select 1 & 'true';
ERROR:  22P02: invalid input syntax for type integer: "true"
LINE 1: select 1 & 'true';
                   ^
```

```sql
select 1 & 1.2;
ERROR:  42883: operator does not exist: integer & numeric
LINE 1: select 1 & 1.2;
                 ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
```

```sql
select 1 & cast('2021-01-01' as date);
ERROR:  42883: operator does not exist: integer & date
LINE 1: select 1 & cast('2021-01-01' as date);
                 ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
```

```sql
select 1 & '12:00:00'::time;
ERROR:  42883: operator does not exist: integer & time without time zone
LINE 1: select 1 & '12:00:00'::time;
                 ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
```

```sql
select 1 & '2021-01-01 12:00:00'::timestamp;
ERROR:  42883: operator does not exist: integer & timestamp without time zone
LINE 1: select 1 & '2021-01-01 12:00:00'::timestamp;
                 ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
```

### Date, Time and Intervals

```sql
select '2020-01-01'::date + '12:00:00'::time;
      ?column?
---------------------
 2020-01-01 12:00:00
(1 row)
```

```sql
select '2020-01-01'::date + interval '1 day';
      ?column?
---------------------
 2020-01-02 00:00:00
(1 row)

select '2020-01-01'::date + interval '1 year';
      ?column?
---------------------
 2021-01-01 00:00:00
(1 row)
```

## Limit

Limit supports integer and floating-point numbers

```sql
select * from test limit 1;
 i | i2 | i3
---+----+----
 1 |  2 |  1
(1 row)

select * from test limit 1.5;
 i | i2 | i3
---+----+----
 1 |  2 |  1
 1 |  1 |  2
(2 rows)

select * from test limit 1.49;
 i | i2 | i3
---+----+----
 1 |  2 |  1
(1 row)

select * from numbers limit '3.9';
ERROR:  22P02: invalid input syntax for type bigint: "3.9"
LINE 1: select * from numbers limit '3.9';
                                    ^
postgres=# select * from numbers limit 2.1;
col_si | col_i | col_bi
--------+-------+--------
    100 |   100 |    100
    100 |   100 |    100
(2 rows)

select * from numbers limit '3' + 0.9;
col_si | col_i | col_bi
--------+-------+--------
    100 |   100 |    100
    100 |   100 |    100
      1 |     2 |      3
      1 |     2 |      3
(4 rows)
```

## Types conversion

`I` - possible implicit cast
`E` - possible explicit cast

| From \ To               |   Char  | Varchar |   Text  | SmallInt | Integer |  BigInt |   Real  |  Double Precision  | Numeric |   Date  |   Time  | Timestamp | Timestamp With TZ | Interval |
|:-----------------------:|:-------:|:-------:|:-------:|:--------:|:-------:|:-------:|:-------:|:------------------:|:-------:|:-------:|:-------:|:---------:|:-----------------:|:--------:|
| String literal          | `I`/`E` | `I`/`E` | `I`/`E` | `E`      | `I`/`E` | `I`/`E` | `E`     | `E`                | `I`/`E` | `E`     | `E`     | `E`       | `E`               | `E`      |
| Char                    | `I`/`E` | `I`/`E` | `I`/`E` | `E`      | `E`     | `E`     | `E`     | `E`                | `E`     | `E`     | `E`     | `E`       | `E`               | `E`      |
| Varchar                 | `I`/`E` | `I`/`E` | `I`/`E` | `E`      | `E`     | `E`     | `E`     | `E`                | `E`     | `E`     | `E`     | `E`       | `E`               | `E`      |
| Integer Num Literal     | `E`     | `E`     | `E`     | `E`      | `I`/`E` | `I`/`E` | `E`     | `E`                | `E`     |         |         |           |                   |          |
| Float Point Num Literal | `E`     | `E`     | `E`     | `E`      | `E`     | `I`/`E` | `E`     | `E`                | `I`/`E` |         |         |           |                   |          |
| SmallInt                | `E`     | `E`     | `E`     | `I`/`E`  | `I`     | `I`     | `E`     | `E`                | `E`     |         |         |           |                   |          |
| Integer                 | `E`     | `E`     | `E`     | `E`      | `I`/`E` | `I`     | `E`     | `E`                | `E`     |         |         |           |                   |          |
| BigInt                  | `E`     | `E`     | `E`     | `E`      | `I`     | `I`/`E` | `E`     | `E`                | `E`     |         |         |           |                   |          | 
| Real                    | `E`     | `E`     | `E`     | `E`      | `E`     | `E`     | `I`/`E` | `E`                | `E`     |         |         |           |                   |          | 
| Double Precision        | `E`     | `E`     | `E`     | `E`      | `E`     | `E`     | `E`     | `I`/`E`            | `E`     |         |         |           |                   |          |
| Numeric                 | `E`     | `E`     | `E`     | `E`      | `E`     | `E`     | `E`     | `E`                | `I`/`E` |         |         |           |                   |          |
| Date                    | `E`     | `I`/`E` | `E`     |          |         |         |         |                    |         | `I`/`E` |         | `E`       | `E`               |          |
| Time                    | `E`     | `E`     | `E`     |          |         |         |         |                    |         |         | `I`/`E` |           |                   | `E`      |
| Timestamp               | `E`     | `E`     | `E`     |          |         |         |         |                    |         | `E`     | `E`     | `I`/`E`   | `E`               |          |
| Timestamp With TZ       | `E`     | `E`     | `E`     |          |         |         |         |                    |         | `E`     | `E`     | `E`       | `I`/`E`           |          |
| Interval                | `E`     | `E`     | `E`     |          |         |         |         |                    |         |         | `E`     |           |                   | `I`/`E`  |

## Types and operations

### Arithmetic

#### Binary Operators

Operations with numbers are presented in the following table

| Left Type               | Operators                    | Right Type              | Success          | Error                                                              |
|:-----------------------:|:----------------------------:|:-----------------------:|:----------------:|:------------------------------------------------------------------:|
| String literal          | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | Integer          | 22P02: invalid input syntax for type integer: "\<value>"           |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `%`, `^` | String literal          | Integer          | 22P02: invalid input syntax for type integer: "\<value>"           |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | Integer          |                                                                    |
| String literal          | `+`, `-`, `/`, `*`, `%`, `^` | String literal          | ---------------- | 42725: operator is not unique: unknown \<operator> unknown         |
| Float Point Num literal | `+`, `-`, `/`, `*`, `%`, `^` | String literal          | Numeric          | 22P02: invalid input syntax for type numeric: "\<value>"           |
| String literal          | `+`, `-`, `/`, `*`, `%`, `^` | Float Point Num literal | Numeric          | 22P02: invalid input syntax for type numeric: "\<value>"           |
| Float Point Num literal | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | Numeric          |                                                                    |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `%`, `^` | Float Point Num literal | Numeric          |                                                                    |
| Float Point Num literal | `+`, `-`, `/`, `*`, `%`, `^` | Float Point Num literal | Numeric          |                                                                    |
|-------------------------|------------------------------|-------------------------|------------------|--------------------------------------------------------------------|
| SmallInt                | `+`, `-`, `/`, `*`, `%`, `^` | String literal          | SmallInt         | 22P02: invalid input syntax for type smallint: "\<value>"          |
| String literal          | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | SmallInt         | 22P02: invalid input syntax for type smallint: "\<value>"          |
| SmallInt                | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | Integer          |                                                                    |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | Integer          |                                                                    |
| Float Point Num literal | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | Numeric          |                                                                    |
| SmallInt                | `+`, `-`, `/`, `*`, `%`, `^` | Float Point Num literal | Numeric          |                                                                    |
| SmallInt                | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | SmallInt         |                                                                    |
|-------------------------|------------------------------|-------------------------|------------------|--------------------------------------------------------------------|
| Integer                 | `+`, `-`, `/`, `*`, `%`, `^` | String literal          | Integer          | 22P02: invalid input syntax for type integer: "\<value>"           |
| String literal          | `+`, `-`, `/`, `*`, `%`, `^` | Integer                 | Integer          | 22P02: invalid input syntax for type integer: "\<value>"           |
| Integer                 | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | Integer          |                                                                    |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | Integer          |                                                                    |
| Integer                 | `+`, `-`, `/`, `*`, `%`, `^` | Float Point Num literal | Numeric          |                                                                    |
| Float Point Num literal | `+`, `-`, `/`, `*`, `%`, `^` | Integer                 | Numeric          |                                                                    |
| SmallInt                | `+`, `-`, `/`, `*`, `%`, `^` | Integer                 | Integer          |                                                                    |
| Integer                 | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | Integer          |                                                                    |
| Integer                 | `+`, `-`, `/`, `*`, `%`, `^` | Integer                 | Integer          |                                                                    |
|-------------------------|------------------------------|-------------------------|------------------|--------------------------------------------------------------------|
| BigInt                  | `+`, `-`, `/`, `*`, `%`, `^` | String literal          | BigInt           | 22P02: invalid input syntax for type bigint: "\<value>"            |
| String literal          | `+`, `-`, `/`, `*`, `%`, `^` | BigInt                  | BigInt           | 22P02: invalid input syntax for type bigint: "\<value>"            |
| BigInt                  | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | BigInt           |                                                                    |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `%`, `^` | BigInt                  | BigInt           |                                                                    |
| BigInt                  | `+`, `-`, `/`, `*`, `%`, `^` | Float Point Num literal | Numeric          |                                                                    |
| Float Point Num literal | `+`, `-`, `/`, `*`, `%`, `^` | BigInt                  | Numeric          |                                                                    |
| SmallInt                | `+`, `-`, `/`, `*`, `%`, `^` | BigInt                  | BigInt           |                                                                    |
| BigInt                  | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | BigInt           |                                                                    |
| BigInt                  | `+`, `-`, `/`, `*`, `%`, `^` | Integer                 | BigInt           |                                                                    |
| Integer                 | `+`, `-`, `/`, `*`, `%`, `^` | BigInt                  | BigInt           |                                                                    |
| BigInt                  | `+`, `-`, `/`, `*`, `%`, `^` | BigInt                  | BigInt           |                                                                    |
|-------------------------|------------------------------|-------------------------|------------------|--------------------------------------------------------------------|
| Real                    | `%`                          | String literal          | Real             | 42883: operator does not exist: real % \<another type>             |
| Real                    | `+`, `-`, `/`, `*`, `^`      | String literal          | Real             | 22P02: invalid input syntax for type real: "\<value>"              |
| String literal          | `+`, `-`, `/`, `*`, `^`      | Real                    | Real             | 22P02: invalid input syntax for type real: "\<value>"              |
| Real                    | `+`, `-`, `/`, `*`, `^`      | Integer Num Literal     | Double Precision |                                                                    |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `^`      | Real                    | Double Precision |                                                                    |
| Real                    | `+`, `-`, `/`, `*`, `^`      | Float Point Num literal | Double Precision |                                                                    |
| Float Point Num literal | `+`, `-`, `/`, `*`, `^`      | Real                    | Double Precision |                                                                    |
| SmallInt                | `+`, `-`, `/`, `*`, `^`      | Real                    | Double Precision |                                                                    |
| Real                    | `+`, `-`, `/`, `*`, `^`      | SmallInt                | Double Precision |                                                                    |
| Real                    | `+`, `-`, `/`, `*`, `^`      | Integer                 | Double Precision |                                                                    |
| Integer                 | `+`, `-`, `/`, `*`, `^`      | Real                    | Double Precision |                                                                    |
| Real                    | `+`, `-`, `/`, `*`, `^`      | BigInt                  | Double Precision |                                                                    |
| BigInt                  | `+`, `-`, `/`, `*`, `^`      | Real                    | Double Precision |                                                                    |
| Real                    | `+`, `-`, `/`, `*`, `^`      | Real                    | Real             |                                                                    |
|-------------------------|------------------------------|-------------------------|------------------|--------------------------------------------------------------------|
| Double Precision        | `%`                          | String literal          | ---------------- | 42883: operator does not exist: double precision % \<another type> |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | String literal          | Double Precision | 22P02: invalid input syntax for type double precision: "\<value>"  |
| String literal          | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision | 22P02: invalid input syntax for type double precision: "\<value>"  |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | Integer Num Literal     | Double Precision |                                                                    |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | Float Point Num literal | Double Precision |                                                                    |
| Float Point Num literal | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | SmallInt                | Double Precision |                                                                    |
| SmallInt                | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | Integer                 | Double Precision |                                                                    |
| Integer                 | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | BigInt                  | Double Precision |                                                                    |
| BigInt                  | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | Real                    | Double Precision |                                                                    |
| Real                    | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
|-------------------------|------------------------------|-------------------------|------------------|--------------------------------------------------------------------|
| Numeric                 | `+`, `-`, `/`, `*`, `%`, `^` | String literal          | Numeric          | 22P02: invalid input syntax for type numeric: "\<value>"           |
| String literal          | `+`, `-`, `/`, `*`, `%`, `^` | Numeric                 | Numeric          | 22P02: invalid input syntax for type numeric: "\<value>"           |
| Numeric                 | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | Numeric          |                                                                    |
| Integer Num Literal     | `+`, `-`, `/`, `*`, `%`, `^` | Numeric                 | Numeric          |                                                                    |
| Numeric                 | `+`, `-`, `/`, `*`, `%`, `^` | Float Point Num literal | Numeric          |                                                                    |
| Float Point Num literal | `+`, `-`, `/`, `*`, `%`, `^` | Numeric                 | Numeric          |                                                                    |
| Numeric                 | `+`, `-`, `/`, `*`, `%`, `^` | SmallInt                | Numeric          |                                                                    |
| SmallInt                | `+`, `-`, `/`, `*`, `%`, `^` | Numeric                 | Numeric          |                                                                    |
| Numeric                 | `+`, `-`, `/`, `*`, `%`, `^` | Integer                 | Numeric          |                                                                    |
| Integer                 | `+`, `-`, `/`, `*`, `%`, `^` | Numeric                 | Numeric          |                                                                    |
| Numeric                 | `+`, `-`, `/`, `*`, `%`, `^` | BigInt                  | Numeric          |                                                                    |
| BigInt                  | `+`, `-`, `/`, `*`, `%`, `^` | Numeric                 | Numeric          |                                                                    |
| Numeric                 | `%`                          | Real                    | ---------------- | 42883: operator does not exist: numeric % real                     |
| Real                    | `%`                          | Numeric                 | ---------------- | 42883: operator does not exist: real % numeric                     |
| Numeric                 | `+`, `-`, `/`, `*`, `^`      | Real                    | Double Precision |                                                                    |
| Real                    | `+`, `-`, `/`, `*`, `^`      | Numeric                 | Double Precision |                                                                    |
| Numeric                 | `%`                          | Double Precision        | ---------------- | 42883: operator does not exist: numeric % double precision         |
| Double Precision        | `%`                          | Numeric                 | ---------------- | 42883: operator does not exist: double precision % numeric         |
| Numeric                 | `+`, `-`, `/`, `*`, `^`      | Double Precision        | Double Precision |                                                                    |
| Double Precision        | `+`, `-`, `/`, `*`, `^`      | Numeric                 | Double Precision |                                                                    |
| Numeric                 | `+`, `-`, `/`, `*`, `%`, `^` | Numeric                 | Numeric          |                                                                    |
|-------------------------|------------------------------|-------------------------|------------------|--------------------------------------------------------------------|

Operations with non numbers are presented in the following table

| Left Type               | Operators                    | Right Type              | Success          | Error                                                                           |
|:-----------------------:|:----------------------------:|:-----------------------:|:----------------:|:-------------------------------------------------------------------------------:|
| Char                    | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | ---------------- | 42883: operator does not exist: character \<operator> integer                   |
| VarChar                 | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | ---------------- | 42883: operator does not exist: character varying \<operator> integer           |
| Date                    | `+`, `-`,                    | Integer Num Literal     | Date             |                                                                                 |
| Date                    | `/`, `*`, `%`, `^`           | Integer Num Literal     | ---------------- | 42883: operator does not exist: date \<operator> integer                        |
| Date                    | `+`, `-`, `/`, `*`, `%`, `^` | Float Num Literal       | ---------------- | 42883: operator does not exist: date \<operator> integer                        |
| Time                    | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | ---------------- | 42883: operator does not exist: time without time zone \<operator> integer      |
| Timestamp               | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | ---------------- | 42883: operator does not exist: timestamp without time zone \<operator> integer |
| Timestamp With TZ       | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | ---------------- | 42883: operator does not exist: timestamp with time zone \<operator> integer    |
| Interval                | `+`, `-`, `/`, `*`, `%`, `^` | Integer Num Literal     | ---------------- | 42883: operator does not exist: interval \<operator> integer                    |
|-------------------------|------------------------------|-------------------------|------------------|---------------------------------------------------------------------------------|

#### Unary Operators

##### String Literals

Supported operators: `\/`, `\\/`, `+`, `!`, `@`

Unsupported operators: `-`, `!!` - throws error `ERROR:  42725: operator is not unique: <operator> unknown`

##### Non-Numeric Types

Unsupported operators: `\/`, `\\/`, `+`, `!`, `@`, `-`, `!!` 
throws error `42883: operator does not exist: <operator> character`

##### Integer Types and Integer Num Literals

Supported operators: `\/`, `\\/`, `+`, `!`, `@`, `-`, `!!`

##### Float Point Types and Float Point Num Literals

Supported operators: `\/`, `\\/`, `+`, `!`, `@`, `-`, `!!`

Unsupported operators: `!`, `!!` - throws error `42883: operator does not exist: double precision <operator>`

### Bitwise Operators

Bitwise binary operators `&`, `|`, `<<`, `#`, `>>` supported only by `Integer` types or `Integer Num Literals` or 
`String Literals` that could be implicitly converted into Integers

Bitwise unary operator `~` supported only by `Integer` types or `Integer Num Literals` or `String Literals` 
that could be implicitly converted into Integers

### String concatenation

Supported if one of arguments is `Char`, `VarChar`, `Text` or `String literals` the other one is implicitly converted 
into `String`.

Otherwise `42883: operator does not exist: <left type> || <right type>` error is thrown

### Match operators (LIKE and NOT LIKE)

Supported if left argument is `Char`, `VarChar`, `Text` or `String literals` and the right one is the string pattern;

Otherwise `42883: operator does not exist: <left type> ~~ <right type>` error is thrown

### Logical operators AND, OR, NOT

Supported by booleans. If one of operators is a `String literal` that could be converted into booleans: 
`'t'`, `'true'`, `'on'`, `'yes'`, `'y'`, `'1'`, `'f'`, `'false'`, `'off'`, `'no'`, `'n'`, `'0'`

If `String literal` can not be converted into boolean `22P02: invalid input syntax for type boolean: "<value>"` error 
is thrown

### Comparison Operators

Supported when types could be implicitly converted. The following list of type groups could be compared:
* `String literal`s, `Char`s, `VarChar`s and `Text`
* `String literal`s, Integers, Float Point numbers and `Numeric literals`
* `Date`s, `Timestamp`s and `Timestamp With TZ`
* `Time`
* `Interval`

Otherwise `42883: operator does not exist: <left type> <operator> <right type>` error is thrown

<!--
What problem are you trying to solve and why.
Audience: end-users, contributors, wondering developers :)
-->

# Technical design

## Expression trees

Evaluation expression tree could be very simple for example, in SQL query:

```sql
insert into table_1 values (3);
```

It contains only single node and could be depicted as:

```text
+---+
| 3 |
+---+
```

Also, it could involve several levels like in the query:

```sql
insert into table_1 values (1 + 2 * 3);
```

It will have three levels:

```text
       +---+              
  +----| + |----+         
  v    +---+    v         
+---+         +---+       
| 1 |    +----| * |----+  
+---+    v    +---+    v  
       +---+         +---+
       | 2 |         | 3 |
       +---+         +---+
```

Expression trees can be located in few places of a query.
Select projection:

```sql
select col_1 + 1, col_2 + 2 from my_table; 
```

Update assignments:

```sql
update my_table set col_1 = 1 + col_1;
```

Insert values list:

```sql
insert into my_table values (1 + 1, 10 + 2);
```

Predicate inside of `WHERE` clause or `JOIN`s

```sql
select * from my_table where col_1 + 10 = 25

select m.* from my_table m join another_table a on m.col_1 = a.col_2 + 1;
```

Values inside of `IN` operator

```sql
select * from my_tables where (col_1, col_2) in (select col_2, col_3 from another_table);
```

If we have a look at expression `'1' + 2 * 3` and its tree

```text
       +---+              
  +----| + |----+         
  v    +---+    v         
+---+         +---+       
|'1'|    +----| * |----+  
+---+    v    +---+    v  
       +---+         +---+
       | 2 |         | 3 |
       +---+         +---+
```

we can derive result type from the context: 
* column type - in case of `INSERT` or `UPDATE`
* left operand - in case of `IN` operator
* it has to be boolean type - in case of `WHERE` and `JOIN` predicates
* from column types that are retrieved from table  - in case of `SELECT` projections

In cases when type of whole expression could be determined `ImplicitCase` operator should be applied. For example, if
`'1' + 2 * 3` expression in position of insert values into `smallint` column then it should be rewritten into 
`cast('1' + 2 * 3 as smallint)` its tree has to be modified to

```text
+------------------------+    
| ImplicitCast(SmallInt) |    
+------------------------+    
             |                
             v                
           +---+              
      +----| + |----+         
      v    +---+    v         
    +---+         +---+       
    |'1'|    +----| * |----+  
    +---+    v    +---+    v  
           +---+         +---+
           | 2 |         | 3 |
           +---+         +---+
```

When validating operands of `+` and deciding throwing `Error` because `'1'` is string literal or trying to convert it
into integer types IsomorphicDB should validate subexpression of `*`.
Validation happens recursively. `2` and `3` are explicitly converted into values of `Integer` type thus return type of
`*` is `Integer` and `'1'` has to be casted into `Integer` type. The result of `'1' + 2 * 3` tree traversion it should
look like on the following scheme:

```text
           +--------------+                             
           | ImplicitCast |                             
           |  (SmallInt)  |                             
           +--------------+                             
                   |                                    
                   v                                    
                 +---+                                  
        +--------| + |---------------+                  
        v        +---+               v                  
+--------------+                   +---+                
| ImplicitCast |           +-------| * |--------+       
|  (Integer)   |           |       +---+        |       
+--------------+           v                    v       
        |          +--------------+     +--------------+
        v          | ImplicitCast |     | ImplicitCast |
      +---+        |  (Integer)   |     |  (Integer)   |
      |'1'|        +--------------+     +--------------+
      +---+                |                    |       
                           v                    v       
                         +---+                +---+     
                         | 2 |                | 3 |     
                         +---+                +---+     
```

Also, during the save validation traversal or as the second round of traversing expression tree types could be assigned
to tree nodes to ease the computation of value during query execution. In that regard expression tree would look like:

```text
                     +-----------------+                                          
                     |  ImplicitCast   |                                          
                     | type (SmallInt) |                                          
                     +-----------------+                                          
                              |                                                   
                              v                                                   
                     +----------------+                                           
                     |       +        |                                           
           +---------| type (Integer) |----------------+                          
           |         +----------------+                |                          
           v                                           v                          
  +----------------+                          +----------------+                  
  |  ImplicitCast  |                          |       *        |                  
  | type (Integer) |                    +-----| type (Integer) |-----+            
  +----------------+                    |     +----------------+     |            
           |                            |                            |            
           |                            v                            v            
           v                   +----------------+           +----------------+    
+--------------------+         |  ImplicitCast  |           |  ImplicitCast  |    
|        '1'         |         | type (Integer) |           | type (Integer) |    
| type (Str Literal) |         +----------------+           +----------------+    
+--------------------+                  |                            |            
                                        |                            |            
                                        v                            v            
                           +------------------------+   +------------------------+
                           |           2            |   |           3            |
                           | type (Int Num Literal) |   | type (Int Num Literal) |
                           +------------------------+   +------------------------+
```

## Expression Tree Transformation

The following is the list of what AST nodes current implementation of parser produces when parsing different `String` 
and `Numeric Literals`

```sql
insert into schema_name.table_name values ('1'::int);
```

value is parsed into `Cast { expr: Value(String("1")), data_type: Int }`

```sql
insert into schema_name.table_name values ('2021-01-01');
```

value is parsed into `Value(String("2021-01-01"))`

```sql
insert into schema_name.table_name values (123.123);
```

value is parsed into `Value(Number("123.123"))`

```sql
select * from table1 limit 3.9
```

limit value is parsed into `Value(Number("3.9"))`

```sql
delete from schema_name.table_name where id = 9223372036854775807;
```

predicate value is parsed into `Value(Number("9223372036854775807"))`

```sql
delete from schema_name.table_name where id = 12;
```

predicate value is parsed into `Value(Int(12))`

From the list we see that all `Integer Num Literal`s are parsed into `Value(Int(<value>))`.
If they larger than Rust `int32::MAX` or lesser than `int32::MIN` they are parsed into `Value(Number("<value>"))`
All string literals are parsed into `Value(String("<value>"))`

Taking expression `'1' + 2 * 21474836470` its AST would look like:

```text
                    +----------+                                           
                    | BinaryOp |                                           
           +--------| op: Add  |------------+                              
           |        +----------+            |                              
           v                                v                              
+--------------------+                +----------+                         
| Value(String("1")) |                | BinaryOp |                         
+--------------------+       +--------| op: Mul  |---------+               
                             |        +----------+         |               
                             |                             |               
                             v                             v               
                     +---------------+     +------------------------------+
                     | Value(Int(2)) |     | Value(Number("21474836470")) |
                     +---------------+     +------------------------------+
```

Then Query Analyzer transforms this AST into `UntypedTree`

```text
                    +----------+                                         
                    | BinaryOp |                                         
           +--------| op: Add  |------------+                            
           |        +----------+            |                            
           v                                v                            
+---------------------+               +----------+                       
| Value(Literal("1")) |               | BinaryOp |                       
+---------------------+      +--------| op: Mul  |--------+              
                             |        +----------+        |              
                             |                            |              
                             v                            v              
                     +---------------+     +----------------------------+
                     | Value(Int(2)) |     | Value(BigInt(21474836470)) |
                     +---------------+     +----------------------------+
```

Then TypeInference transform this AST into `TypedTree`

```text
                    +-------------+                                      
                    |  BinaryOp   |                                      
           +--------|   op: Add   |----------+                           
           |        | type BigInt |          |                           
           v        +-------------+          v                           
+---------------------+               +-------------+                    
| Value(Literal("1")) |               |  BinaryOp   |                    
| type StringLiteral  |      +--------|   op: Mul   |-----+              
+---------------------+      |        | type BigInt |     |              
                             |        +-------------+     |              
                             v                            v              
                     +---------------+     +----------------------------+
                     | Value(Int(2)) |     | Value(BigInt(21474836470)) |
                     | type Integer  |     |        type BigInt         |
                     +---------------+     +----------------------------+
```

Then TypeChecker checks for validity of operands. The validation phase should pass because it is Ok to execute 
`StringLiteral + BigInt`.

Then TypeCoercion adds required implicit casts so that expression tree could be executed.

```text
                             +-------------+                                      
                             |  BinaryOp   |                                      
           +-----------------|   op: Add   |----------+                           
           |                 | type BigInt |          |                           
           v                 +-------------+          v                           
   +---------------+                           +-------------+                    
   | ImplicitCast  |                           |  BinaryOp   |                    
   | type (BigInt) |                  +--------|   op: Mul   |-----+              
   +---------------+                  |        | type BigInt |     |              
           |                          |        +-------------+     |              
           |                          v                            v              
           v                  +---------------+     +----------------------------+
+---------------------+       | ImplicitCast  |     | Value(BigInt(21474836470)) |
| Value(Literal("1")) |       | type (BigInt) |     |        type BigInt         |
| type StringLiteral  |       +---------------+     +----------------------------+
+---------------------+               |                                           
                                      |                                           
                                      v                                           
                              +---------------+                                   
                              | Value(Int(2)) |                                   
                              | type Integer  |                                   
                              +---------------+                                   
```

Depending on the position of the expression TypeCoercion will add or skip implicit cast on the top of the tree. So if, 
for an example, expression `'1' + 2 * 21474836470` has to be inserted into `SmallInt` column. TypeCoercion will add
implicit cast to `SmallInt`. On the other hand if expression in a position of select projection it would be skipped.

<!--
Audience: end-users, contributors, wondering developers :)
-->

# Drawbacks

Not found

<!--
There is no silver bullet. Describe here possible disadvantages of described design and what possible tradeoffs.
-->

# Alternatives

None

<!--
* Is there another way to have things around? :)
* Can we have another designs what their pros and cons?
-->

# Unresolved questions

None

<!--
Do you have any questions before considering merging this RFC?
-->

# Future possibilities

Type resolution for parameters in `prepared statement`s and in queries with `extended query protocol`. Type system 
allows implementing more advance SQL operators which in turn opens up door for query processing for a workload
forecasting.
