use postgres::{Client, NoTls};

#[test]
#[ignore] // use psql for integration testing
fn create_simple_database() {
    // psql use sslmode `require`
    let _client = Client::connect("host=localhost user=postgres password=pass", NoTls).unwrap();
}
