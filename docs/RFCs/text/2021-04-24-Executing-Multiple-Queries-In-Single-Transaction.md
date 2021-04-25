 * Feature Name: Executing Multiple Queries In Single Transaction
 * Authors: Alex Dukhno (@alex-dukhno)
 * RFC PR: (PR # after acceptance of initial draft)
 * RFC Tracking Issue: Issue # after merging of RFC

# Summary

Currently, IsomorphicDB can either execute single query in a transaction if used [simple query protocol][1] or 
single stage of prepared query if used [extended query protocol][2]. This RFC can affect current implementation of:
 - how IsomorphicDB handles networking request
 - how Query Engine is implemented

This RFC does not contain information of how to handle query processing, e.g. how to handle dynamic parameters used with
[extended query protocol][2] or what process of type inference or type coercion. These topics deserve their own RFCs.
Also it is not a goal of this RFC how much of same behavior IsomorphicDB should have of handling prepared statements.
This information can be taken from official PostgreSQL documentation. Transactions validation and conflict resolution is
out of scope of the RFC. However, it might influence the process of execute multiple queries in a single transaction.

# Motivation and background

## Query execution stages overview

### Executing simple query

If client sends query using [simple query protocol][1] then the database performs the following steps:
 * receives client `Query { sql }`
 * `sql` string is parsed into `query_ast`
 * `query_ast` is passed to `Analyzer` which creates `untyped_query`
 * `untyped_query` goes through `query_processing`: `type_inference`, `type_check` and `type_coercion`. After these 
   steps `untyped_query` is transformed into `type_query`
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

There is an implicit `Deallocated` phase in this case. The database does not do anything but other cases have explicit 
request from client to clean up resources on the database side.

### Executing simple query using Prepare and Execute keywords

PostgreSQL allows split execution [simple query into multiple stages][3] using `prepare` and `execute` keywords. Client 
will send `prepare` query with dynamic parameters and their types and then send `execute` query with parameters' 
arguments.
In this case the process of executing a query would be the following:
 * database receives from client `Query { sql }` where `sql` starts with `prepare` keyword
 * `sql` string is parsed into `query_ast`
 * `query_ast` is passed to `Analyzer` which creates `untyped_query`
 * `untyped_query` goes through `query_processing`: `type_inference`, `type_check` and `type_coercion`. After these 
   steps `untyped_query` is transformed into `type_query`
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

PostgreSQL extended query protocol consists of multiple stages and use different types of request instead of single 
`Query` for simple query protocol.
In this case the process of executing a query that is tested on the time of writing with configuration of compatibility
tests [JDBC driver][5] would be the following:
 * client sends the following: `Parse { statement_name, sql, param_types }` and `DescribeStatement(statement_name)` as a
   single network message.
    - database handles client's `Parse { statement_name, sql, param_types }` by parsing `sql` into `query_ast`
    - `param_types` contains type [oid][4]s. It can contain `0`s in situations when client does not know about types of 
      dynamic parameters. However, it could potentially contain info if client caches types data received from server 
      and reuse it in subsequent executions of the same query. `param_types` should be checked if they are valid for 
      current database schema info
    - database handles `DescribeStatement(statement_name)` by passing `query_ast` into `Analyzer` and processing 
      `untyped_query` with `query_processing` mechanism.
    - when database gets `typed_query` it also should resolve types of dynamic parameters presented in the query.
    - next database sends response to the client with `StatementParameters` that contains [oid][4]s of dynamic 
      parameters and `StatementDescription` that contains name and type of columns that will be returned by the database 
      after query execution. `StatementDescription` has to be empty if it is not a `select` query
    - database can either save `typed_query` or build a `query_plan` and save it
 * then client sends: `Parse { statement_name, sql, param_types }`, 
   `Bind { portal_name, statement_name, query_param_formats, query_params, result_value_formats }`, 
   `DescribePortal(portal_name)` and `Execute { portal_name, max_rows}`
    - database retrieves saved info mapped with `statement_name`
    - database creates `portal` for further execution that is mapped with `portal_name`
    - database initializes values for dynamic parameters from `Bind` request. `query_param_formats` encodes what format
      is used to transfer `query_params` could be either `text` or `binary`.
    - database saves `result_value_formats` with the newly created portal to know in what format to send data back to 
      client
    - `DescribePortal(portal_name)` is handled in similar way as `DescribeStatement(statement_name)`. Database sends 
      only `StatementDescription` that contains name and type of columns that will be returned by the database after 
      query execution. `StatementDescription` has to be empty if it is not a `select` query
    - `Execute { portal_name, max_rows}` is handled by executing mapped `query_plan` and parameter values with 
      `portal_name`
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

## Overview
On the network layer client sends messages as raw bytes that deserialized into `Query`, `Parse`, `Describe`, `Bind` or
`Close` requests that influence the state of a query. Then request has to be dispatched either through all phases (in
the case of `Query`) or some part of them. Phases like `Analyzed`, `Processed`, `Planned` and `Executed` requires access
to the database schema either for meta-information like: `table_name`, `column_name` or `column_type` - to validate 
a query or correctly infer input and output types of the query.

## More detailed description
 
Here `QueryEngine` and `Session` represent two concepts to separate two different states. One is data stored in the 
database - `QueryEngine`, the other is connection state of a client to handle its queries - `Session`. They are used to 
easing description of the design.

On the network layer we can have a `Session` that is separated from `QueryEngine`. (On the time of writing there is 
`QueryEngine` struct that contains `Session` struct as a field). Whenever client sends a request `Session` should start 
a new transaction or use transaction that is in the progress. Transaction should be assigned with id (`txn-id`) and 
managed by `QueryEngine`. `Session` using `txn-id` should ask `QueryEngine` to further process query on its way to be
executed.

Code example:
```rust
type StatementName = String;
type PortalName = String;
type TxnId = u32;

struct Session {
    statements: HashMap<StatementName, QueryPlan>,
    portals: HashMap<StatementName, HashMap<PortalName, Vec<Value>>>,
    current_txn: Option<TxnId>
}


struct QueryEngine {
    parser: QueryParser,
    analyzer: QueryAnalyzer,
    // ... other fields to process a query
}

impl QueryEngine {
    fn start_transaction(&self) -> TxnId {
        self.allocate_next_transaction_id()
    }

    fn parse(&self, txn_id: TxnId, sql: &str) -> Result<query_ast::Query, Error> {
       self.parser.parse(sql)
    }
   
    fn describe(&self, txn_id: TxnId, query: Query) -> Result<QueryInfo, Error> {
       match self.analyzer.analyze(query) {
          Ok(mut query) => {
             self.type_inference.infer(&mut query)?;
             self.type_checker.check(&mut query)?;
             self.type_coercion.coerce(&mut query)?;
             QueryInfo {
                return_types,
                plan: self.query_planner.plan(query)?,
                // ... other fields
             }
          }
          // handle errors
       }
    }
}
```

## Testing

To have a confidence in implementation of the RFC we could incorporate different drivers test suites for end-to-end 
testing besides unit and integration tests that could be written in rust.
Drawback of using PostgreSQL drivers implementation tests is that they are testing also other PostgreSQL features that
either is not implemented or out of scope of the RFC.

# Drawbacks

Not found yet. A working prototype could yield some information if there is other possible solutions.

# Alternatives

One of an alternative could be that `Session` and `QueryEngine` communicates not directly by `Session` invocation of 
`QueryEngine` functions but using a task queue. However, it requires more detailed design and also influence on changes
of current thread model - which is simple thread per connection model. More advanced Thread Model should be researched
and designed in a separate RFC.

# State of the Art

[PostgreSQL Wire Protocol][6]

# Unresolved questions

Semantics of `max_rows` in `Execute { portal_name, max_rows }` request is not defined in this RFC and what its relation 
to `LIMIT` clause in a sql `select` query.

# Future possibilities

Implementation of this RFC does not directly influence on Transactions and Thread Model design, however, it is the first
step in that direction.

[1]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[2]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
[3]: https://www.postgresql.org/docs/current/sql-prepare.html
[4]: https://www.postgresql.org/docs/current/datatype-oid.html
[5]: https://jdbc.postgresql.org
[6]: https://www.postgresql.org/docs/current/protocol.html
