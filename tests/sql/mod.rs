use postgres::{Client, Config, NoTls};
use std::io;
use std::str::FromStr;

#[test]
#[ignore] // use psql for integration testing
fn create_simple_database() {
    /// psql use sslmode `require`
    let mut client = Client::connect("host=localhost user=postgres password=pass", NoTls).unwrap();
}
