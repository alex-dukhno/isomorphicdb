# Database

The project doesn't have any name so let it be `database` for now.

### Simple play around example

Start server with the command
```shell script
RUST_LOG=trace cargo run --bin server
```

It will print the following messages
```shell script
 INFO  server > Starting server on port 7000
 INFO  server > SQL engine has been created
 INFO  server > Network buffer of 256 size created
```

Then start client
```shell script
RUST_LOG=trace cargo run --bin client
```

It starts with
```shell script
 INFO  client > Starting client
```

Then enter one by one SQL queries

```sql
create table t (i int);
insert into t values(1);
select i from t;
```

The client will dump all data that it receives from server like

```shell script
create table t (i int);
 DEBUG client > typed command "create table t (i int);\n"
 DEBUG client > command send to server
 TRACE client > Received from server [1]
 DEBUG client > 1 server result code
 TRACE client > Received from server [84, 97, 98, 108, 101, 32, 116, 32, 119, 97, 115, 32, 99, 114, 101, 97, 116, 101, 100]
Ok("Table t was created")
insert into t values(1);
 DEBUG client > typed command "insert into t values(1);\n"
 DEBUG client > command send to server
 TRACE client > Received from server [2]
 DEBUG client > 2 server result code
 TRACE client > Received from server [100, 111, 110, 101]
Ok("done")
select i from t;
 DEBUG client > typed command "select i from t;\n"
 DEBUG client > command send to server
 TRACE client > Received from server [3, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]
 DEBUG client > 3 server result code
Ok("\u{1}\u{1}\u{1}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{1}\u{0}\u{0}\u{0}")
```

Type `exit` to quit the client

```shell script
exit
 DEBUG client > typed command "exit\n"
```
