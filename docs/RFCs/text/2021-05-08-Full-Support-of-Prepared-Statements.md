 * Feature Name: Full Support of Prepared Statements
 * Authors: Alex Dukhno (@alex-dukhno)
 * RFC PR: (PR # after acceptance of initial draft)
 * RFC Tracking Issue: Issue # after merging of RFC

# Summary

PostgreSQL supports prepared statements with `PREPARE`, `EXECUTE` and `DEALLOCATE` keywords. This proposal describes how
it could be implemented in IsomorphicDB

# Motivation and background

 * SQL syntax compatibility with PostgreSQL
 * Possibility for users to manage cached query plans on session level
 * Support caching query plans for drivers that does not support Extended Query Protocol functionality

## Prepared Statement description

Prepared Statement has the following format:

```sql
PREPARE <unique_name> (<param_type>[, <param_type>]) AS <query with parameter indexes>;
```

User can specify `<param_type>` that is not a supported type. Parameter index is also specified by user, so it could
appear that query has one parameter but user specified `$100` as index.

### Parameter Types

In case user specifies not supported type parser can recognize it as a custom type. It requires only changes in
`DataType` enum, however, its variants often converted into `SqlType` or `SqlTypeFamily` enum variants for internal
usage.

Also, user can omit parameter types if engine can infer them from schema information. For an example consider this
snippet:

```sql
CREATE TABLE t1 (
    c1 SMALLINT,
    c2 INTEGER,
    c3 BIGINT
);

PREPARE query_plan AS INSERT INTO t1 VALUES ($1, $2, $3);
```

In this example query engine can infer that `$1` has `SMALLINT`, `$2` has `INTEGER` and `$3` has `BIGINT` types
respectively. This implies that user can omit some parameter types. A derived snippet from previous example:

```sql
PREPARE query_plan (INTEGER) AS INSERT INTO t1 VALUES ($1, $2, $3);
```

In this case `$1` has `INTEGER` type.

### Parameter Index

User can specify index number as they want. For example:

```sql
create table one_col (col1 integer);

prepare another_query_plan (smallint) as insert into one_col values ($2);
```

`PREPARE` will work fine with PostgreSQL. However, the following query will fail:

```sql
execute another_query_plan (1);
```

with error:

```psql
ERROR:  wrong number of parameters for prepared statement "another_query_plan"
DETAIL:  Expected 2 parameters but got 1.
```

Resubmitting `PREPARE` query with `$1` instead of `$2` will fix the problem or user can execute the following query:

```sql
execute another_query_plan (1, 2);
```

# Technical design



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
