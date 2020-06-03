extern crate database;
extern crate pretty_env_logger;

use database::node::Node;

fn main() {
    pretty_env_logger::init();
    Node::default().start();
}
