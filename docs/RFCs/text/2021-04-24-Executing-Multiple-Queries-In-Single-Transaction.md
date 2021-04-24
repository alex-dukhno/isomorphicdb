 * Feature Name: Executing Multiple Queries In Single Transaction
 * Authors: Alex Dukhno (@alex-dukhno)
 * RFC PR: (PR # after acceptance of initial draft)

**Remember, you can submit a PR and ask for initial feedback with your RFC before the text is complete.**

# Summary

Currently, IsomorhicDB can either execute single query in a transaction if used [simple query protocol][1] or single stage of prepared query if used [extended query protocol][2]. This RFC can affect current implementation of:
 - how IsomorphicDB handles networking request
 - how Query Engine is implemented

This RFC does not contain information of how to handle query processing, e.g. how to handle dynamic parameters used with [extended query protocol][2] or what process of type inference or type coercion. These topics deserve their own RFCs.
Also it is not a goal of this RFC how much of same behavior IsomorphicDB should have of handling prepared statements. This information can be taken from official PostgreSQL documentation.
Transactions validation and conflict resolution is out of scope of the RFC. However, it might influence the process of execute multiple queries in a single transaction.

# Motivation and background

## Query execution stages overview

### Executing simple query

If client sends query using [simple query protocol][1] then the database performs the following steps:
 * receives client `Query { sql }`
 * `sql` string is parsed into `query_ast`
 * `query_ast` is passed to `Analyzer` which creates `untype_query`
 * `untyped_query` goes through `query_processing`: `type_inference`, `type_check` and `type_coercion`. After these steps `untyped_query` is trasfromed into `type_query`
 * `query_planner` creates `query_plan` out of `typed_query`
 * `query_plan` is executed

The life-cycle of a query in this case could be described with the following image:

```text
+------------------------------------------------------------------------------------------------------------+
|                                                                                                            |
|  Execute                                                                                                   |
|                                                                                                            |
|   +----------+       +--------+       +----------+      +-----------+      +---------+      +----------+   |
|   |          |       |        |       |          |      |           |      |         |      |          |   |
|   | Received |------>| Parsed |------>| Analyzed |----->| Processed |----->| Planned |----->| Executed |   |
|   |          |       |        |       |          |      |           |      |         |      |          |   |
|   +----------+       +--------+       +----------+      +-----------+      +---------+      +----------+   |
|                                                                                                            |
+------------------------------------------------------------------------------------------------------------+
```

### Executing simple query using Prepare and Execute keywords

PostgreSQL allows split execution [simple query into multiple stages][3] using `prepare` and `execute` keywords. Client will send `prepare` query with dynamic parameters and their types and then send `execute` query with parameters' arguments.
In this case the process of executing a query would be the following:
 * database receives from client `Query { sql }` where `sql` starts with `prepare` keyword
 * `sql` string is parsed into `query_ast`
 * `query_ast` is passed to `Analyzer` which creates `untype_query`
 * `untyped_query` goes through `query_processing`: `type_inference`, `type_check` and `type_coercion`. After these steps `untyped_query` is transformed into `type_query`
 * `query_planner` creates `query_plan` out of `typed_query`
 * database receives from client `Query { sql }` where `sql` starts with `execute` keyword
 * `query_plan` is executed using parameters sent by a client with the `execute` query
 * database receives from client `Query { sql }` where `sql` starts with `deallocate` keyword to clean up resources

The life-cycle of a query in this case could be described with the following image:

```text
+--------------------------------------------------------------------------------------------+
|                                                                                            |
| Prepare                                                                                    |
|                                                                                            |
|   +----------+       +--------+       +----------+      +-----------+      +---------+     |
|   |          |       |        |       |          |      |           |      |         |     |
|   | Received |------>| Parsed |------>| Analyzed |----->| Processed |----->| Planned |--+  |
|   |          |       |        |       |          |      |           |      |         |  |  |
|   +----------+       +--------+       +----------+      +-----------+      +---------+  |  |
|                                                                                         |  |
+-----------------------------------------------------------------------------------------+--+
                                                                                          |   
                                        +---------------------+        +------------------+--+
                                        |                     |        |                  |  |
                                        | Deallocate          |        | Execute          |  |
                                        |                     |        |                  |  |
                                        |                     |        | +--------+       |  |
                                        |                     |        | |        |       |  |
                                        |                     |        | |        |       |  |
                                        |   +-------------+   |        | |  +----------+  |  |
                                        |   |             |   |        | |  |          |  |  |
                                        |   | Deallocated |<--+----+   | +->| Executed |<-+  |
                                        |   |             |   |    |   |    |          |     |
                                        |   +-------------+   |    |   |    +----------+     |
                                        |                     |    |   |          |          |
                                        +---------------------+    |   +----------+----------+
                                                                   +--------------+           
```

`Query { sql: "execute ..." }` could be sent multiple times by a client.

### Executing extended query

PostgreSQL extended query protocol consists of multiple stages and use different types of `Request` instead of single `Query` for simple query protocol.
In this case the process of executing a query that is tested on the time of writing with configuration of compatibility tests [JDBC driver][5] would be the following:
 * client sends the following: `Parse { statement_name, sql, param_types }` and `DescribeStatement(statement_name)` as a single network message.
    - database handles client's `Parse { statement_name, sql, param_types }` by parsing `sql` into `query_ast`
    - `param_types` contains type [oid][4]s. It can contain `0`s in situations when client does not know about types of dynamic parameters. However, it could potentially contain info if client caches types data received from server and reuse it in subsequencial executions of the same query. `param_types` should be checked if they are valid for current database schema info
    - database handles `DescribeStatement(statement_name)` by passing `query_ast` into `Analyzer` and processing `untyped_query` with `query_processing` mechanism.
    - when database gets `typed_query` it also should resolve types of dynamic parameters presented in the query.
    - next database sends response to the client with `StatementParameters` that contains [oid][4]s of dynamic parameters and `StatementDescription` that contains name and type of columns that will be returned by database after query execution. `StatementDescription` has to be empty if it is not a `select` query
    - database can either save `typed_query` or build a `query_plan` and save it
 * then client sends: `Parse { statement_name, sql, param_types }`, `Bind { portal_name, statement_name, query_param_formats, query_params, result_value_formats }`, `DescribePortal(portal_name)` and `Execute { portal_name, max_rows}`
    - database retrieves saved info mapped with `statement_name`
    - database creates `portal` for further execution that is mapped with `portal_name`
    - database initializes values for dynamic parameters from `Bind` request. `query_param_formats` encodes what format is used to transfer `query_params` could be either `text` or `binary`.
    - database saves `result_value_formats` with newly created portal to know in what format to send data back to client
    - `DescribePortal(portal_name)` is handled in similar way as `DescribeStatement(statement_name)`. Database sends only `StatementDescription` that contains name and type of columns that will be returned by database after query execution. `StatementDescription` has to be empty if it is not a `select` query
    - `Execute { portal_name, max_rows}` is handled by executing mapped `query_plan` and parameter values with `portal_name`
 * portal and/or statement can deallocated by `ClosePortal` and `CloseStatement` requests from client respectively

The life-cycle of a query in this case could be described with the following image:

```text
+-------------------------------------------------------------------------------------------------------------+
|                                                                                                             |
|       Parse                                                                                                 |
| DescribeStatement                                                                                           |
|                                                                                                             |
|                                                                                                             |
|   +----------+       +--------+       +----------+      +-----------+      +---------+      +-----------+   |
|   |          |       |        |       |          |      |           |      |         |      |           |   |
|   | Received |       | Parsed |       | Analyzed |      | Processed |      | Planned |      |   Cache   |   |
|   |          |------>|        |------>|          |----->|           |----->|         |----->| Statement |   |
|   |          |       |        |       |          |      |           |      |         |      |           |   |
|   +----------+       +--------+       +----------+      +-----------+      +---------+      +-----------+   |
|                                                                                                   |         |
|                                                                                                   |         |
+---------------------------------------------------------------------------------------------------+---------+
                                                                                                    |
                                                                                                    |
                         +--------------------------------------------------------------------------+
 +-----------------------+----------+      +---------------------+       +---------------------+
 |      Parse            |          |      |         +-------+   |       |CloseStatement       |
 |      Bind             |          |      | Execute |       |   |       | ClosePortal         |
 | DescribePortal        |          |      |         |       |   |       |                     |
 |                       v          |      |   +----------+  |   |       |   +-------------+   |
 |                 +----------+     |      |   |          |  |   |       |   |             |   |
 |                 |          |     | +----+-->| Executed |<-+   |  +----+-->| Deallocated |   |
 |                 |  Cached  |     | |    |   |          |      |  |    |   |             |   |
 |                 |  Portal  |-----+-+    |   +----------+      |  |    |   +-------------+   |
 |                 |          |     |      |         |           |  |    |                     |
 |                 +----------+     |      |         |           |  |    |                     |
 |                                  |      |         +-----------+--+    |                     |
 |                                  |      |                     |       |                     |
 +----------------------------------+      +---------------------+       +---------------------+
```

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

Semantics of `max_rows` in `Execute { portal_name, max_rows }` request is not defined in this RFC and what its relation to `LIMIT` clause in a sql `select` query.
<!--
Do you have any questions before considering merging this RFC?
-->

# Future possibilities

This is a place where you can write your ideas that are related to the RFC but out of it scope.
If you don't have any don't bother too much about that and left it blank. Anyway RFCs reviewers would probably give you
a hint :)

[1]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[2]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
[3]: https://www.postgresql.org/docs/current/sql-prepare.html
[4]: https://www.postgresql.org/docs/current/datatype-oid.html
[5]: https://jdbc.postgresql.org
