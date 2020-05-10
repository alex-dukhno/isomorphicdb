extern crate types;

mod in_memory;

use std::collections::HashSet;

use crate::types::Type;

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
    VarChar,
}

impl StorageType {
    pub fn match_with(&self, sql_type: &Type) -> bool {
        match (self, sql_type) {
            (StorageType::SmallInt, Type::Int(_)) => true,
            (StorageType::Integer, Type::Int(_)) => true,
            (StorageType::BigInt, Type::Int(_)) => true,
            (StorageType::Serial, Type::Int(_)) => true,
            (StorageType::BigSerial, Type::Int(_)) => true,
            (StorageType::Decimal, Type::Decimal(_)) => true,
            (StorageType::Numeric, Type::Decimal(_)) => true,
            (StorageType::Real, Type::Decimal(_)) => true,
            (StorageType::DoublePrecision, Type::Decimal(_)) => true,
            (StorageType::VarChar, Type::VarChar(_)) => true,
            _ => false,
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
pub enum Constraint {
    PrimaryKey,
    ForeignKey(String, String),
    NotNull,
    Unique,
    Check(String, Predicate),
}

#[derive(Hash, PartialEq, Eq)]
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
    NotExistentColumnInConstrain,
    MismatchedConstraintType,
}
