# High Level Overview

This document is by no mean complete ... specially Postgres Wire Protocol Message flow.
It will be changed in accordance to what we figure out about database system while 
developing it.

## Postgres Wire Protocol State Machine

Postgres Wire Protocol can work in two modes: Simple Query and Extended Query.
In Simple Mode client sends queries as a string and waits for a server response.
In Extended Mode client and server exchange several messages before client gets final response of a query execution.
I will use state names as a 
[Postgres Wire Protocol Message Names](https://www.postgresql.org/docs/current/protocol-message-formats.html).

### Simple Query Protocol

Simple Query State Machine contains the following states: **Query**, **RowDescription**, **ErrorResponse**, 
**CommandComplete**, **DataRow** and **ReadyForQuery**.
In Simple Mode client sends *Query* message to start query execution.

* During query execution server may send **ErrorResponse** if any of stages encounter an error.
* If client sends an `insert`, an `update` or a `delete` query on successful execution server sends **CommandCompete** message.
* If client sends a `select` query - server sends **RowDescription** message.
It contains all needed info for a client how to represent a data that is coming next.
For each selected row server sends **DataRow** message.
As with other type of queries in the end server sends **CommandCompete** message.
* After that server sends **ReadyForQuery** message to signal client that it is ready to serve next queries.

```
                   +---------+                   +------------------+
                   |  Query  |------------------>|  RowDescription  |---+
                   +---------+                   +------------------+   |
                        |                                  |            |
                        |                                  |            |
                        |                                  |            |
         +--------------+-----------------+----------------+            |
         |                                |                             |
         |                                |                             |
         |                                v                             |
         v                      +-------------------+                   |
+-----------------+             |  CommandComplete  |<-----+            |
|  ErrorResponse  |             +-------------------+      |            |
+-----------------+                       |                |            |
         |                                |                |            |
         |                                |                |            |
         |                                |                |            |
         |                                |                |            |
         v                                |                |            v
+-----------------+                       |                |      +-----------+
|  ReadyForQuery  |<----------------------+                +------|  DataRow  |--+
+-----------------+                                               +-----------+  |
                                                                        ^        |
                                                                        |        |
                                                                        +--------+
```

### Extended Query Protocol

Extended Query separated into three phases: `prepare`, `execute` and `deallocate`.
In `prepare` phase a client sends query to a server that could be parsed and analyzed.
Query looks like as the following:
```SQL
select * from foo where bar > $1
```

`$1` denotes a parameter that could vary and should be bound before execution.
Server sends back a description of a statement:

* what data types of columns to insert
* what data types of returning data

From our example, server has to send **ParameterDescription** message for `$1`
and **RowDescription** message with all columns from `foo` table. Server sends
**NoData** message if query execution does not have result to return.

Prepare Phase has the following state machine:
```
                 +-------+
                 | Parse |
                 +-------+
                     |
                     v
             +---------------+
             | ParseComplete |
             +---------------+
                     |
                     v
               +----------+
               | Describe |
               +----------+
                     |
                     v
         +----------------------+
         | ParameterDescription |
         +----------------------+
                     |
     +---------------+-------------+
     |                             |
     v                             v
+--------+                +----------------+
| NoData |                | RowDescription |
+--------+                +----------------+
     |          +------+           |
     +--------->| Sync |<----------+
                +------+
                    |
                    v
            +---------------+
            | ReadyForQuery |
            +---------------+
```

**NOTE:** !!! This part of the section could be changed in the future
Execution Phase has the following state machine:
```
                 +-------+
                 | Parse |
                 +-------+
                     |
                     v
             +---------------+
             | ParseComplete |
             +---------------+
                     |
                     v
               +----------+
               | Describe |
               +----------+
                     |
     +---------------+-------------+
     |                             |
     v                             v
+--------+                +----------------+
| NoData |                | RowDescription |
+--------+                +----------------+
     |          +------+           |
     +--------->| Sync |<----------+
                +------+
                    |
                    v
            +---------------+
            | ReadyForQuery |
            +---------------+
```


## Query Execution


