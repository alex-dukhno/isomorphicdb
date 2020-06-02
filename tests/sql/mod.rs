use postgres::error::Error;
use postgres::{Client, NoTls};

#[test]
#[ignore] // server can't be run in process
fn create_simple_database() -> Result<(), Error> {
    let mut client = Client::connect("host=localhost user=postgres password=pass", NoTls).unwrap();

    client.simple_query("create schema SMOKE_QUERIES;")?;
    client.simple_query("create table SMOKE_QUERIES.VALIDATION_TABLE (column_test smallint);")?;

    client.simple_query("insert into SMOKE_QUERIES.VALIDATION_TABLE values (1);")?;
    client.simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")?;

    client.simple_query("update SMOKE_QUERIES.VALIDATION_TABLE set column_test = 2;")?;
    client.simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")?;

    client.simple_query("delete from SMOKE_QUERIES.VALIDATION_TABLE;")?;
    client.simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")?;

    client.simple_query("drop table SMOKE_QUERIES.VALIDATION_TABLE;")?;
    client.simple_query("drop schema SMOKE_QUERIES;")?;

    drop(client);
    Ok(())
}
