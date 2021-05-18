 * Feature Name: Type System
 * Authors: Alex Dukhno (@alex-dukhno)
 * RFC PR: (PR # after acceptance of initial draft)
 * RFC Tracking Issue: Issue # after merging of RFC

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
```

<!--
What problem are you trying to solve and why.
Audience: end-users, contributors, wondering developers :)
-->

# Technical design



<!--
Audience: end-users, contributors, wondering developers :)
-->

# Drawbacks

There is no silver bullet. Describe here possible disadvantages of described design and what possible tradeoffs.

# Alternatives

* Is there another way to have things around? :)
* Can we have another designs what their pros and cons?

# State of the Art

Here you can link papers, other databases feature descriptions or RFCs to help others to get broader understanding of
problem space, and the design described in the RFC.

# Unresolved questions

Do you have any questions before considering merging this RFC?

# Future possibilities

This is a place where you can write your ideas that are related to the RFC but out of it scope. 
If you don't have any don't bother too much about that and left it blank. Anyway RFCs reviewers would probably give you 
a hint :)
