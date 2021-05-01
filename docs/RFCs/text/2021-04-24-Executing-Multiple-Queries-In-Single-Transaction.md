 * Feature Name: Executing Multiple Queries In Single Transaction
 * Authors: Alex Dukhno (@alex-dukhno)
 * RFC PR: [isomorphicdb/#570](https://github.com/alex-dukhno/isomorphicdb/issues/570)
 * RFC Tracking Issue: [isomorphicdb/#574](https://github.com/alex-dukhno/isomorphicdb/issues/574)

# Summary

Currently, IsomorphicDB can either execute single query in a transaction if used [simple query protocol][1] or 
single stage of prepared query if used [extended query protocol][2]. This RFC can affect current implementation of:
 - how IsomorphicDB handles networking request
 - how Query Engine is implemented

This RFC does not contain information of how to handle query processing, e.g. how to handle dynamic parameters used with
[extended query protocol][2] or what process of type inference or type coercion. These topics deserve their own RFCs.
Transactions validation and conflict resolution is out of scope of the RFC. This is the topic of future RFCs that might
influence the process of execute multiple queries in a single transaction.

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
request from the client to clean up resources on the database side.

### Executing simple query using Prepare and Execute keywords

PostgreSQL allows split execution of [simple query into multiple stages][3] using `prepare` and `execute` keywords. Client 
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

Client always starts transaction either with `Query` or `Parse` request.
`QueryParser` has to parse the query string and depending on the query:

1. if it is `Query { sql: "BEGIN" }` `QueryEngine` should start a transaction.
1. if it is `Query { sql: "PREPARE ..." }` `Session` has to allocate memory for a query plan and analyze, process and 
   plan the query using `QueryEngine` and save it with `""` (empty string) as `statement_name`, so-called unnamed 
   statement
1. if it is `Query { sql: "EXECUTE ..." }` `Session` has to check if there is an unnamed query plan and execute it using
`QueryEngine`
1. if it is `Parse` request `Session` has to allocate memory for a query and save parsed result as 
   `StatementState::Parsed`
1. if it is `DescribeStatement` `QueryEngine` has to be used to analyze, process and plan the query and 
   stored as `StatementState::Described` in the `Session`
1. if it is `Bind` `Session` allocates memory for a `Portal` and stores it
1. if it is `DescribePortal` database sends client needed info described in the section above
1. if it is `Execute` then `Session` lookups a `Portal` and executes the associated query_plan.
1. if it is `Query { sql: "COMMIT" }` or `Parse { sql: "COMMIT", ... }` `QueryEngine` tries to commit the transaction.
1. if it is `Query { sql: "ROLLBACK" }` or `Parse { sql: "ROLLBACK", ... }` `QueryEngine` tries to roll back 
   the transaction.

Code example:
```rust
type StatementName = String;
type PortalName = String;
type TxnId = u32;
type Oid = u32;
type Format = i32;

enum StatementState {
   Parsed { ast: QueryAst },
   Described { 
      ast: QueryPlan,
      return_types: Vec<Oid>,
      // ... other needed fields
   }
}

struct Portal {
   query_plan: QueryPlan,
   return_types: Vec<Oid>,
   return_formats: Vec<Format>,
   params_values: Vec<Value>,
}

struct Session {
    statements: HashMap<StatementName, StatementState>,
    portals: HashMap<StatementName, HashMap<PortalName, Portal>>,
    current_txn: Option<TxnContext>
}

// struct holds what tables, columns and other
// schema info that transaction is working on
// it is collected during transaction execution
// and used to quickly resolve table or column
// if it was already used by the transaction
struct TxnContext {
    txn_id: u32,
    tables: HashMap<String, TableId>,
    columns: HashMap<String, ColumnId>,
    // ... other fields
}

struct QueryEngine {
    parser: QueryParser,
    analyzer: QueryAnalyzer,
    // ... other fields to process a query
}

impl QueryEngine {
    fn start_transaction(&self) -> TxnId {
        let txn_id = self.allocate_next_transaction_id();
        TxnContext {
            txn_id,
            tables: HashMap::default(),
            columns: HashMap::defautl()
        }
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

1. It is not possible to use current transaction `Database` API using closures and its API should be reworked.
1. It is not possible to test with multiple clients connected to database and execute queries unless proper transactions
are implemented

# Alternatives

One of an alternative could be that `Session` and `QueryEngine` communicates not directly by invocation of `Session` and
`QueryEngine` functions but using a task queues. However, it requires more detailed design and also influence on changes
of current thread model - which is simple thread per connection model. More advanced Thread Model should be researched
and designed in a separate RFC.

# State of the Art

[PostgreSQL Wire Protocol][6]

# Unresolved questions

~~Semantics of `max_rows` in `Execute { portal_name, max_rows }` request is not defined in this RFC and what its relation 
to `LIMIT` clause in a sql `select` query.~~
`LIMIT` and `max_rows` are not related. For example `max_rows` could be set by JDBC driver as the following:

```java
PreparedStatement jdbcSelect = conn.prepareStatement("select * from test where i2 > ? limit 3");
PgStatement pgSelect = jdbcSelect.unwrap(PgStatement.class);
pgSelect.setUseServerPrepare(true);
pgSelect.setPrepareThreshold(1);
pgSelect.setMaxRows(10);
```

# Future possibilities

Implementation of this RFC does not directly influence on Transactions and Thread Model design, however, it is the first
step in that direction.

[1]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[2]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
[3]: https://www.postgresql.org/docs/current/sql-prepare.html
[4]: https://www.postgresql.org/docs/current/datatype-oid.html
[5]: https://jdbc.postgresql.org
[6]: https://www.postgresql.org/docs/current/protocol.html
