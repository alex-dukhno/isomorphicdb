extern crate core;
extern crate log;

pub mod backend;
pub mod frontend;

pub type Projection = (Vec<String>, Vec<Vec<String>>);

#[derive(Debug, PartialEq)]
pub struct SchemaAlreadyExists;
#[derive(Debug, PartialEq)]
pub struct SchemaDoesNotExist;

#[derive(Debug, PartialEq)]
pub enum CreateTableError {
    SchemaDoesNotExist,
    TableAlreadyExists,
}

#[derive(Debug, PartialEq)]
pub enum DropTableError {
    SchemaDoesNotExist,
    TableDoesNotExist,
}

#[derive(Debug, PartialEq)]
pub enum OperationOnTableError {
    SchemaDoesNotExist,
    TableDoesNotExist,
}
