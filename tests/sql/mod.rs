use database::node::{Node, CREATED};
use postgres::error::Error;
use postgres::{Client, NoTls};
use std::sync::Arc;
use std::thread;

fn start_server(node: Arc<Node>) -> thread::JoinHandle<()> {
    let cloned = node.clone();

    let handler = thread::spawn(move || {
        cloned.start();
    });

    while node.state() == CREATED {}

    handler
}

#[test]
fn create_simple_database() -> Result<(), Error> {
    let node = Arc::new(Node::default());

    let handler = start_server(node.clone());

    let mut client = Client::connect("host=localhost user=postgres password=pass", NoTls)?;

    client.simple_query("create schema SMOKE_QUERIES;")?;
    client.simple_query("create table SMOKE_QUERIES.VALIDATION_TABLE (column_test smallint);")?;

    client.simple_query("insert into SMOKE_QUERIES.VALIDATION_TABLE values (1);")?;
    let selected =
        client.simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")?;
    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_test"), Some("1"));
    } else {
        panic!(
            "no records were retrived by 'select column_test from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    client.simple_query("update SMOKE_QUERIES.VALIDATION_TABLE set column_test = 2;")?;
    let selected =
        client.simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")?;
    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_test"), Some("2"));
    } else {
        panic!(
            "no records were retrived by 'select column_test from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    client.simple_query("delete from SMOKE_QUERIES.VALIDATION_TABLE;")?;
    let selected =
        client.simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")?;
    assert_eq!(selected.len(), 0 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(_row)) = selected.iter().next() {
        panic!("no records has to be retrived by 'select column_test from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    client.simple_query("drop table SMOKE_QUERIES.VALIDATION_TABLE;")?;
    client.simple_query("drop schema SMOKE_QUERIES;")?;

    drop(client);

    node.stop();

    handler.join().unwrap();

    drop(node);

    Ok(())
}

#[test]
#[ignore]
fn create_table_with_three_columns() -> Result<(), Error> {
    let node = Arc::new(Node::default());

    let handler = start_server(node.clone());

    let mut client = Client::connect("host=localhost user=postgres password=pass", NoTls)?;

    client.simple_query("create schema SMOKE_QUERIES;")?;
    client.simple_query("create table SMOKE_QUERIES.VALIDATION_TABLE (column_1 smallint, column_2 smallint, column_3 smallint);")?;

    client.simple_query("insert into SMOKE_QUERIES.VALIDATION_TABLE values (1, 2, 3);")?;

    let selected = client
        .simple_query("select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;")?;

    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_2"), Some("2"));
        assert_eq!(row.get("column_3"), Some("3"));
    } else {
        panic!("no records were retrived by 'select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    let selected =
        client.simple_query("select column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;")?;

    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_2"), Some("2"));
    } else {
        panic!("no records were retrived by 'select column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    let selected =
        client.simple_query("select column_1, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;")?;

    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_3"), Some("3"));
    } else {
        panic!("no records were retrived by 'select column_1, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    drop(client);

    node.stop();

    handler.join().unwrap();

    drop(node);

    Ok(())
}
