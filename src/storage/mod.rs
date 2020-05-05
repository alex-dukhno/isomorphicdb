mod in_memory;

use std::collections::HashSet;

use crate::storage::in_memory::InMemoryStorage;
use crate::types::Type;

pub fn in_memory() -> impl Storage {
    InMemoryStorage::default()
}

pub trait Storage {
    fn create_table(
        &mut self,
        table_name: &String,
        columns: Vec<(String, StorageType, HashSet<Constraint>)>,
    ) -> Result<SqlResult, SqlError>;

    fn insert_into(
        &mut self,
        table_name: &String,
        values: Vec<(String, Type)>,
    ) -> Result<SqlResult, SqlError>;

    fn select(
        &mut self,
        table_name: &String,
        predicate: Option<Predicate>,
    ) -> Result<Vec<Vec<Type>>, ()>;
}

pub enum StorageType {
    // i16
    SmallInt,
    // i32
    Integer,
    // i64
    BigInt,
    // 131072, 16383
    Decimal,
    // 131072, 16383
    Numeric,
    // f32
    Real,
    // f64
    DoublePrecision,
    // 1 .. i16::MAX
    SmallSerial,
    // 1 .. i32::MAX
    Serial,
    // 1 .. i64::MAXs
    BigSerial,
}

#[derive(Hash, PartialEq, Eq)]
pub enum Constraint {
    PrimaryKey,
    ForeignKey(String, String),
    NotNull,
    Unique,
}

pub enum Predicate {
    Equal(Type),
    Between(Type, Type),
    In(Vec<Type>),
    Not(Box<Predicate>),
}

#[derive(Debug, PartialEq)]
pub enum SqlResult {
    TableCreated,
    RecordInserted,
}

#[derive(Debug, PartialEq)]
pub enum SqlError {
    TableAlreadyExists,
    DuplicateColumnsName,
    TableDoesNotExists,
}
