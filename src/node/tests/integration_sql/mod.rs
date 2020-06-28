// Copyright 2020 Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use node::node::{Node, CREATED, RUNNING};
use postgres::{Client, NoTls};
use std::{sync::Arc, thread};

fn start_server(node: Arc<Node>) -> thread::JoinHandle<()> {
    let cloned = node.clone();

    let handler = thread::spawn(move || {
        cloned.start();
    });

    while node.state() == CREATED {}

    handler
}

fn stop_server_workaround(client: &mut Client) {
    let _result = client.simple_query("TERMINATE");
    match Client::connect("host=localhost user=postgres password=pass", NoTls) {
        Ok(_) => {}
        Err(_) => {}
    }
}

#[test]
fn create_simple_database() {
    let node = Arc::new(Node::default());

    let handler = start_server(node.clone());

    let mut client =
        Client::connect("host=localhost user=postgres password=pass", NoTls).expect("to connect to server");

    client
        .simple_query("create schema SMOKE_QUERIES;")
        .expect("to create schema");
    client
        .simple_query("create table SMOKE_QUERIES.VALIDATION_TABLE (column_test smallint);")
        .expect("to create table");

    client
        .simple_query("insert into SMOKE_QUERIES.VALIDATION_TABLE values (1);")
        .expect("to insert value");
    let selected = client
        .simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select value");
    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_test"), Some("1"));
    } else {
        panic!("no records were retrieved by 'select column_test from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    client
        .simple_query("update SMOKE_QUERIES.VALIDATION_TABLE set column_test = 2;")
        .expect("to update value");
    let selected = client
        .simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select value");
    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_test"), Some("2"));
    } else {
        panic!("no records were retrived by 'select column_test from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    client
        .simple_query("delete from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to delete value");
    let selected = client
        .simple_query("select column_test from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select value");
    assert_eq!(selected.len(), 0 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(_row)) = selected.iter().next() {
        panic!("no records has to be retrieved by 'select column_test from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    client
        .simple_query("drop table SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to drop table");
    client
        .simple_query("drop schema SMOKE_QUERIES;")
        .expect("to drop schema");

    node.stop();
    while node.state() == RUNNING {
        println!("STOPPING!!!!");
    }

    stop_server_workaround(&mut client);

    drop(client);

    drop(node);

    handler.join().unwrap();
}

#[test]
fn create_table_with_three_columns() {
    let node = Arc::new(Node::default());

    let handler = start_server(node.clone());

    let mut client =
        Client::connect("host=localhost user=postgres password=pass", NoTls).expect("to connect to server");

    client
        .simple_query("create schema SMOKE_QUERIES;")
        .expect("to create schema");
    client
        .simple_query(
            "create table SMOKE_QUERIES.VALIDATION_TABLE (column_1 smallint, column_2 smallint, column_3 smallint);",
        )
        .expect("to create table");

    client
        .simple_query("insert into SMOKE_QUERIES.VALIDATION_TABLE values (1, 2, 3);")
        .expect("to insert values");

    let selected = client
        .simple_query("select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 1 + 1);
    if let Some(postgres::SimpleQueryMessage::Row(row)) = selected.iter().next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_2"), Some("2"));
        assert_eq!(row.get("column_3"), Some("3"));
    } else {
        panic!(
            "no records were retrieved by 'select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    client
        .simple_query("insert into SMOKE_QUERIES.VALIDATION_TABLE values (4, 5, 6), (7, 8, 9);")
        .expect("to insert values");

    let selected = client
        .simple_query("select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_2"), Some("2"));
        assert_eq!(row.get("column_3"), Some("3"));
    } else {
        panic!("expected more records were retrieved by 'select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("4"));
        assert_eq!(row.get("column_2"), Some("5"));
        assert_eq!(row.get("column_3"), Some("6"));
    } else {
        panic!("expected more records were retrieved by 'select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("7"));
        assert_eq!(row.get("column_2"), Some("8"));
        assert_eq!(row.get("column_3"), Some("9"));
    } else {
        panic!("expected more records were retrieved by 'select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    let selected = client
        .simple_query("select column_1, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_3"), Some("3"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_1, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("4"));
        assert_eq!(row.get("column_3"), Some("6"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_1, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("7"));
        assert_eq!(row.get("column_3"), Some("9"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_1, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    let selected = client
        .simple_query("select column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_2"), Some("2"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("4"));
        assert_eq!(row.get("column_2"), Some("5"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("7"));
        assert_eq!(row.get("column_2"), Some("8"));
    } else {
        panic!("expected more records were retrieved by 'select column_1, column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    let selected = client
        .simple_query("select column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_2"), Some("2"));
        assert_eq!(row.get("column_3"), Some("3"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_2"), Some("5"));
        assert_eq!(row.get("column_3"), Some("6"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_2"), Some("8"));
        assert_eq!(row.get("column_3"), Some("9"));
    } else {
        panic!(
            "expected more records were retrieved by 'select column_2, column_3 from SMOKE_QUERIES.VALIDATION_TABLE;'"
        );
    }

    let selected = client
        .simple_query("select * from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("1"));
        assert_eq!(row.get("column_2"), Some("2"));
        assert_eq!(row.get("column_3"), Some("3"));
    } else {
        panic!("expected more records were retrieved by 'select * from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("4"));
        assert_eq!(row.get("column_2"), Some("5"));
        assert_eq!(row.get("column_3"), Some("6"));
    } else {
        panic!("expected more records were retrieved by 'select * from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get("column_1"), Some("7"));
        assert_eq!(row.get("column_2"), Some("8"));
        assert_eq!(row.get("column_3"), Some("9"));
    } else {
        panic!("expected more records were retrieved by 'select * from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    let selected = client
        .simple_query("select column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("3"));
        assert_eq!(row.get(1), Some("1"));
        assert_eq!(row.get(2), Some("2"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("6"));
        assert_eq!(row.get(1), Some("4"));
        assert_eq!(row.get(2), Some("5"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("9"));
        assert_eq!(row.get(1), Some("7"));
        assert_eq!(row.get(2), Some("8"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    let selected = client
        .simple_query("select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("3"));
        assert_eq!(row.get(1), Some("2"));
        assert_eq!(row.get(2), Some("1"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("6"));
        assert_eq!(row.get(1), Some("5"));
        assert_eq!(row.get(2), Some("4"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("9"));
        assert_eq!(row.get(1), Some("8"));
        assert_eq!(row.get(2), Some("7"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    let selected = client
        .simple_query("select column_3, column_2, column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select values");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("3"));
        assert_eq!(row.get(1), Some("2"));
        assert_eq!(row.get(2), Some("3"));
        assert_eq!(row.get(3), Some("1"));
        assert_eq!(row.get(4), Some("2"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("6"));
        assert_eq!(row.get(1), Some("5"));
        assert_eq!(row.get(2), Some("6"));
        assert_eq!(row.get(3), Some("4"));
        assert_eq!(row.get(4), Some("5"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("9"));
        assert_eq!(row.get(1), Some("8"));
        assert_eq!(row.get(2), Some("9"));
        assert_eq!(row.get(3), Some("7"));
        assert_eq!(row.get(4), Some("8"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_3, column_1, column_2 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    client
        .simple_query("update SMOKE_QUERIES.VALIDATION_TABLE set column_1 = 10, column_2 = -20, column_3 = 30;")
        .expect("to update value");
    let selected = client
        .simple_query("select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;")
        .expect("to select value");

    assert_eq!(selected.len(), 3 + 1);

    let mut iter = selected.iter();

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("30"));
        assert_eq!(row.get(1), Some("-20"));
        assert_eq!(row.get(2), Some("10"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("30"));
        assert_eq!(row.get(1), Some("-20"));
        assert_eq!(row.get(2), Some("10"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    if let Some(postgres::SimpleQueryMessage::Row(row)) = iter.next() {
        assert_eq!(row.get(0), Some("30"));
        assert_eq!(row.get(1), Some("-20"));
        assert_eq!(row.get(2), Some("10"));
    } else {
        panic!("expected more records were retrieved by 'select column_3, column_2, column_1 from SMOKE_QUERIES.VALIDATION_TABLE;'");
    }

    node.stop();
    while node.state() == RUNNING {
        println!("STOPPING!!!!");
    }

    stop_server_workaround(&mut client);

    drop(client);

    drop(node);

    handler.join().unwrap();
}
