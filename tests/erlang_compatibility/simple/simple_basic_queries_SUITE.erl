-module(simple_basic_queries_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2]).
-export([test_select_all_columns/1]).

-define(CreateSchema, "create schema SCHEMA_NAME").
-define(DropSchema, "drop schema SCHEMA_NAME").

-define(CreateTable, "create table SCHEMA_NAME.TABLE_NAME
        (COL_1 smallint, COL_2 smallint, COL_3 smallint)").
-define(DropTable, "drop table SCHEMA_NAME.TABLE_NAME").

-define(DeleteAllColumns, "delete from SCHEMA_NAME.TABLE_NAME").
-define(InsertDefaultRows, "insert into SCHEMA_NAME.TABLE_NAME values
        (1, 2, 3), (4, 5, 6), (7, 8, 9)").

-define(SelectAllColumns, "select * from SCHEMA_NAME.TABLE_NAME").
-define(SelectSpecifiedColumns, "select COL_2, COL_3 from
        SCHEMA_NAME.TABLE_NAME").

-define(UpdateAll, "update SCHEMA_NAME.TABLE_NAME set
        COL_1 = 10, COL_2 = 11, COL_3 = 12").

-define(DeleteAll, "delete from SCHEMA_NAME.TABLE_NAME").

all() -> [test_select_all_columns].

init_per_suite(Config) ->
    {ok, DbConn} = create_db_connection(),

    clear_all(DbConn),

    epgsql:squery(DbConn, ?CreateSchema),
    epgsql:squery(DbConn, ?CreateTable),

    [{db_conn, DbConn} | Config].

end_per_suite(_Config) ->
    {ok, DbConn} = create_db_connection(),

    clear_all(DbConn),

    ok.

init_per_testcase(_AnyTestCase, Config) ->
    {ok, DbConn} = create_db_connection(),

    delete_all_columns(DbConn),
    insert_default_rows(DbConn),

    [{db_conn, DbConn} | Config].

end_per_testcase(_AnyTestCase, Config) ->
    DbConn = ?config(db_conn, Config),

    delete_all_columns(DbConn),

    ok.

test_select_all_columns(Config) ->
    DbConn = ?config(db_conn, Config),

    {ok, DbColumns, DbValues}= epgsql:squery(DbConn, ?SelectAllColumns).

clear_all(DbConn) ->
    epgsql:squery(DbConn, ?DropTable),
    epgsql:squery(DbConn, ?DropSchema).

create_db_connection() ->
    epgsql:connect("localhost", "", "", #{codecs => []}).

delete_all_columns(DbConn) ->
    epgsql:squery(DbConn, ?DeleteAllColumns).

insert_default_rows(DbConn) ->
    DbResult = epgsql:squery(DbConn, ?InsertDefaultRows).
