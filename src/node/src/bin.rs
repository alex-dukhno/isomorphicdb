extern crate node;
extern crate pretty_env_logger;

fn main() {
    pretty_env_logger::init();
    node::node::Node::default().start();
}
