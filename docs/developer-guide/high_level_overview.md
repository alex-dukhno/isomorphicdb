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
     +----------+-------+        
     |                  |        
     v                  v        
+--------+     +----------------+
| NoData |     | RowDescription |
+--------+     +----------------+
     |     +------+     |        
     +---->| Sync |<----+        
           +------+              
               |                 
               v                 
       +---------------+         
       | ReadyForQuery |         
       +---------------+         
```

* First, a client sends **Parse** message with a query
* Server responds with **ParseComplete** if it successful
* Then the client asks for data types info by sending **Describe** method (in this case it is has `S` flag that stands for Statement)
* First, server answers with **ParameterDescription** message to describe data types for `$#` parameters
* Then, if it is `select` statement server sends **RowDescription** message, otherwise **NoData**
* Then the client sends **Sync** message to synchronise with server
* And the server responds with **ReadyForQuery**

------------------
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
          +------+              
          | Bind |              
          +------+              
              |                 
              v                 
      +--------------+          
      | BindComplete |          
      +--------------+          
              |                 
              v                 
        +----------+            
        | Describe |            
        +----------+            
              |                 
     +--------+--------+        
     |                 |        
     v                 v        
+--------+    +----------------+
| NoData |    | RowDescription |
+--------+    +----------------+
     |                 |        
     |    +------+     |        
     +--->| Sync |<----+        
          +------+              
              |                 
              v                 
      +---------------+         
      | ReadyForQuery |         
      +---------------+         
```

* First, a client sends **Parse** message with a query
* Server responds with **ParseComplete** if it successful
* Then the client sends **Bind** message with `$#` parameter values
* Then the client asks for data types info by sending **Describe** method (in this case it is has `P` flag that stands for Portal)
* Then, if it is `select` statement server sends **RowDescription** message, otherwise **NoData**
* Then the client sends **Sync** message to synchronise with server
* And the server responds with **ReadyForQuery**

## Query Execution without optimizations

Having a look at queries:

* `select`s: `select col1, col2 + 2, 1 = 2 from foo where bar = baz + 2`
* `insert`s: `insert into foo values (1, 2 * 8)` or `insert into foo select col_1, col_2 foo2 where col_2 > 1`
* `update`s: `update foo set col1 = 2 * col1 where col2 > 3`
* `delete`s: `delete from foo where bar > 3`

Every position with expression: return columns value, insert values, set values, predicates in `where` clause - can be seen as operators
tree. These trees should be instantiated in runtime and executed to gain needed computation results.
