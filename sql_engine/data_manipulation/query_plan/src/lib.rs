// Copyright 2020 - present Alex Dukhno
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

use bigdecimal::{BigDecimal, FromPrimitive};
use catalog::{Cursor, SqlTable};
use data_binary::{
    repr::{Datum, ToDatum},
    Binary,
};
use data_manipulation_query_result::QueryExecutionError;
use data_manipulation_typed_tree::{DynamicTypedTree, StaticTypedTree};
use data_scalar::ScalarValue;
use definition::ColumnDef;
use query_response::QueryEvent;
use std::collections::HashMap;
use types::{SqlType, SqlTypeFamily};

pub enum QueryPlanResult {
    Inserted(usize),
    Deleted(usize),
    Updated(usize),
    Selected((Vec<ColumnDef>, Vec<Vec<ScalarValue>>)),
}

impl From<QueryPlanResult> for QueryEvent {
    fn from(plan_result: QueryPlanResult) -> QueryEvent {
        match plan_result {
            QueryPlanResult::Inserted(inserted) => QueryEvent::RecordsInserted(inserted),
            QueryPlanResult::Deleted(inserted) => QueryEvent::RecordsDeleted(inserted),
            QueryPlanResult::Updated(inserted) => QueryEvent::RecordsUpdated(inserted),
            _ => unreachable!(),
        }
    }
}

pub enum QueryPlan {
    Insert(InsertQueryPlan),
    Delete(DeleteQueryPlan),
    Update(UpdateQueryPlan),
    Select(SelectQueryPlan),
}

impl QueryPlan {
    pub fn execute(self, param_values: Vec<ScalarValue>) -> Result<QueryPlanResult, QueryExecutionError> {
        match self {
            QueryPlan::Insert(insert_query_plan) => {
                insert_query_plan.execute(param_values).map(QueryPlanResult::Inserted)
            }
            QueryPlan::Delete(delete_query_plan) => delete_query_plan.execute().map(QueryPlanResult::Deleted),
            QueryPlan::Update(update_query_plan) => update_query_plan.execute().map(QueryPlanResult::Updated),
            QueryPlan::Select(select_query_plan) => select_query_plan.execute().map(QueryPlanResult::Selected),
        }
    }
}

pub trait Flow {
    type Output;

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError>;
}

pub struct StaticValues(Box<dyn Iterator<Item = Vec<Option<StaticTypedTree>>>>);

impl StaticValues {
    pub fn new(values: Vec<Vec<Option<StaticTypedTree>>>) -> Box<StaticValues> {
        Box::new(StaticValues(Box::new(values.into_iter())))
    }
}

impl Flow for StaticValues {
    type Output = Vec<Option<StaticTypedTree>>;

    fn next_tuple(&mut self, _param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        Ok(self.0.next())
    }
}

pub struct StaticExpressionEval {
    source: Box<dyn Flow<Output = Vec<Option<StaticTypedTree>>>>,
}

impl StaticExpressionEval {
    pub fn new(source: Box<dyn Flow<Output = Vec<Option<StaticTypedTree>>>>) -> Box<StaticExpressionEval> {
        Box::new(StaticExpressionEval { source })
    }
}

impl Flow for StaticExpressionEval {
    type Output = Vec<Option<ScalarValue>>;

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Ok(Some(tuple)) = self.source.next_tuple(param_values) {
            let mut next_tuple = vec![];
            for value in tuple {
                let typed_value = match value {
                    None => None,
                    Some(value) => match value.eval(param_values) {
                        Err(error) => return Err(error),
                        Ok(value) => Some(value),
                    },
                };
                next_tuple.push(typed_value);
            }
            Ok(Some(next_tuple))
        } else {
            Ok(None)
        }
    }
}

pub struct ConstraintValidator {
    source: Box<dyn Flow<Output = Vec<Option<ScalarValue>>>>,
    column_types: Vec<(String, SqlTypeFamily)>,
}

impl ConstraintValidator {
    pub fn new(
        source: Box<dyn Flow<Output = Vec<Option<ScalarValue>>>>,
        column_types: Vec<(String, SqlTypeFamily)>,
    ) -> Box<ConstraintValidator> {
        Box::new(ConstraintValidator { source, column_types })
    }
}

impl Flow for ConstraintValidator {
    type Output = Vec<Option<Box<dyn ToDatum>>>;

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Some(tuple) = self.source.next_tuple(param_values)? {
            let mut data = vec![];
            for (index, value) in tuple.into_iter().enumerate() {
                let value = match (value, self.column_types[index].1) {
                    (None, _) => None,
                    (Some(value), type_family) => match value.type_family() {
                        None => unimplemented!(),
                        Some(value_type) => match value_type.compare(&type_family) {
                            Ok(wide_type_family) => {
                                log::debug!("{:?} {:?} {:?}", value, wide_type_family, type_family);
                                match (value.clone(), type_family) {
                                    (ScalarValue::Num { value, .. }, SqlTypeFamily::SmallInt) => {
                                        if !(BigDecimal::from(i16::MIN)..=BigDecimal::from(i16::MAX)).contains(&value) {
                                            return Err(QueryExecutionError::most_specific_type_mismatch(
                                                value,
                                                type_family,
                                                self.column_types[index].0.as_str(),
                                                index,
                                            ));
                                        }
                                    }
                                    (ScalarValue::Num { value, .. }, SqlTypeFamily::Integer) => {
                                        if !(BigDecimal::from(i32::MIN)..=BigDecimal::from(i32::MAX)).contains(&value) {
                                            return Err(QueryExecutionError::most_specific_type_mismatch(
                                                value,
                                                type_family,
                                                self.column_types[index].0.as_str(),
                                                index,
                                            ));
                                        }
                                    }
                                    (ScalarValue::Num { value, .. }, SqlTypeFamily::BigInt) => {
                                        if !(BigDecimal::from(i64::MIN)..=BigDecimal::from(i64::MAX)).contains(&value) {
                                            return Err(QueryExecutionError::most_specific_type_mismatch(
                                                value,
                                                type_family,
                                                self.column_types[index].0.as_str(),
                                                index,
                                            ));
                                        }
                                    }
                                    (ScalarValue::String(_), _) => {}
                                    (ScalarValue::Bool(_), _) => {}
                                    _ => unimplemented!(),
                                }
                                Some(value.as_to_datum())
                            }
                            Err(_) => {
                                return Err(QueryExecutionError::most_specific_type_mismatch(
                                    value,
                                    type_family,
                                    self.column_types[index].0.as_str(),
                                    index,
                                ))
                            }
                        },
                    },
                };
                data.push(value);
            }
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

pub struct InsertQueryPlan {
    source: Box<dyn Flow<Output = Vec<Option<Box<dyn ToDatum>>>>>,
    table: Box<dyn SqlTable>,
}

impl InsertQueryPlan {
    pub fn new(
        source: Box<dyn Flow<Output = Vec<Option<Box<dyn ToDatum>>>>>,
        table: Box<dyn SqlTable>,
    ) -> InsertQueryPlan {
        InsertQueryPlan { source, table }
    }

    pub fn execute(mut self, param_values: Vec<ScalarValue>) -> Result<usize, QueryExecutionError> {
        let mut len = 0;
        while let Some(data) = self.source.next_tuple(&param_values)? {
            self.table.write(Binary::pack(
                &data
                    .into_iter()
                    .map(|v| v.map(|v| v.convert()).unwrap_or_else(Datum::from_null))
                    .collect::<Vec<Datum>>()
                    .as_slice(),
            ));
            len += 1;
        }
        Ok(len)
    }
}

pub struct FullTableScan {
    source: Cursor,
}

impl FullTableScan {
    pub fn new(source: &dyn SqlTable) -> Box<FullTableScan> {
        Box::new(FullTableScan { source: source.scan() })
    }
}

impl Flow for FullTableScan {
    type Output = (Binary, Binary);

    fn next_tuple(&mut self, _param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        let record = self.source.next();
        log::debug!("TABLE RECORD {:?}", record);
        Ok(record)
    }
}

pub struct TableRecordKeys {
    source: Box<dyn Flow<Output = (Binary, Binary)>>,
}

impl TableRecordKeys {
    pub fn new(source: Box<dyn Flow<Output = (Binary, Binary)>>) -> Box<TableRecordKeys> {
        Box::new(TableRecordKeys { source })
    }
}

impl Flow for TableRecordKeys {
    type Output = Binary;

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Some((key, _value)) = self.source.next_tuple(param_values)? {
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }
}

pub struct DeleteQueryPlan {
    source: Box<dyn Flow<Output = Binary>>,
    table: Box<dyn SqlTable>,
}

impl DeleteQueryPlan {
    pub fn new(source: Box<dyn Flow<Output = Binary>>, table: Box<dyn SqlTable>) -> DeleteQueryPlan {
        DeleteQueryPlan { source, table }
    }

    pub fn execute(mut self) -> Result<usize, QueryExecutionError> {
        let mut len = 0;
        while let Some(key) = self.source.next_tuple(&[])? {
            self.table.write_key(key, None);
            len += 1;
        }
        Ok(len)
    }
}

pub struct Repeater {
    source: Vec<Option<DynamicTypedTree>>,
}

impl Repeater {
    pub fn new(source: Vec<Option<DynamicTypedTree>>) -> Box<Repeater> {
        Box::new(Repeater { source })
    }
}

impl Flow for Repeater {
    type Output = Vec<Option<DynamicTypedTree>>;

    fn next_tuple(&mut self, _param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        Ok(Some(self.source.clone()))
    }
}

pub struct DynamicValues {
    source: Box<dyn Flow<Output = Vec<Option<DynamicTypedTree>>>>,
    records: Box<dyn Flow<Output = (Binary, Binary)>>,
}

impl DynamicValues {
    pub fn new(
        source: Box<dyn Flow<Output = Vec<Option<DynamicTypedTree>>>>,
        records: Box<dyn Flow<Output = (Binary, Binary)>>,
    ) -> Box<DynamicValues> {
        Box::new(DynamicValues { source, records })
    }
}

impl Flow for DynamicValues {
    type Output = Vec<Option<ScalarValue>>;

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Some((_key, value)) = self.records.next_tuple(param_values)? {
            let table_row = value
                .unpack()
                .into_iter()
                .map(|datum| match datum {
                    Datum::Null => ScalarValue::Null,
                    Datum::True => ScalarValue::Bool(true),
                    Datum::False => ScalarValue::Bool(false),
                    Datum::Int16(value) => ScalarValue::Num {
                        value: BigDecimal::from(value),
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    Datum::Int32(value) => ScalarValue::Num {
                        value: BigDecimal::from(value),
                        type_family: SqlTypeFamily::Integer,
                    },
                    Datum::Int64(value) => ScalarValue::Num {
                        value: BigDecimal::from(value),
                        type_family: SqlTypeFamily::BigInt,
                    },
                    Datum::Float32(value) => ScalarValue::Num {
                        value: BigDecimal::from_f32(*value).unwrap(),
                        type_family: SqlTypeFamily::Real,
                    },
                    Datum::Float64(value) => ScalarValue::Num {
                        value: BigDecimal::from_f64(*value).unwrap(),
                        type_family: SqlTypeFamily::Double,
                    },
                    Datum::String(value) => ScalarValue::String(value),
                })
                .collect::<Vec<_>>();
            if let Some(tuple) = self.source.next_tuple(param_values)? {
                let mut next_tuple = vec![];
                for value in tuple {
                    let value = match value {
                        None => None,
                        Some(tree) => match tree.eval(&table_row) {
                            Err(error) => return Err(error),
                            Ok(value) => Some(value),
                        },
                    };
                    next_tuple.push(value);
                }
                Ok(Some(next_tuple))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

pub struct UpdateQueryPlan {
    values: Box<dyn Flow<Output = Vec<Option<Box<dyn ToDatum>>>>>,
    records: Box<dyn Flow<Output = (Binary, Binary)>>,
    table: Box<dyn SqlTable>,
}

impl UpdateQueryPlan {
    pub fn new(
        values: Box<dyn Flow<Output = Vec<Option<Box<dyn ToDatum>>>>>,
        records: Box<dyn Flow<Output = (Binary, Binary)>>,
        table: Box<dyn SqlTable>,
    ) -> UpdateQueryPlan {
        UpdateQueryPlan { values, records, table }
    }

    pub fn execute(mut self) -> Result<usize, QueryExecutionError> {
        let mut len = 0;
        while let Some((key, row)) = self.records.next_tuple(&[])? {
            let mut unpacked = row.unpack();
            if let Some(values) = self.values.next_tuple(&[])? {
                for (index, value) in values.into_iter().enumerate() {
                    let new_value = match value {
                        None => unpacked[index].clone(),
                        Some(value) => value.convert(),
                    };
                    unpacked[index] = new_value;
                }
            }
            let new_row = Binary::pack(&unpacked);
            self.table.write_key(key, Some(new_row));
            len += 1;
        }
        Ok(len)
    }
}

pub struct SelectQueryPlan {
    source: Box<dyn Flow<Output = (Binary, Binary)>>,
    columns: Vec<String>,
    column_types: Vec<(String, SqlType)>,
}

impl SelectQueryPlan {
    pub fn new(
        source: Box<dyn Flow<Output = (Binary, Binary)>>,
        columns: Vec<String>,
        column_types: Vec<(String, SqlType)>,
    ) -> SelectQueryPlan {
        SelectQueryPlan {
            source,
            columns,
            column_types,
        }
    }

    pub fn execute(mut self) -> Result<(Vec<ColumnDef>, Vec<Vec<ScalarValue>>), QueryExecutionError> {
        log::debug!("COLUMNS TO SELECT {:?}", self.columns);
        let mut column_defs = vec![];
        let columns = self
            .column_types
            .iter()
            .enumerate()
            .map(|(index, (name, sql_type))| (name.clone(), ColumnDef::new(name.clone(), *sql_type, index)))
            .collect::<HashMap<String, ColumnDef>>();
        for name in self.columns.iter() {
            column_defs.push(columns.get(name).unwrap().clone());
        }
        log::debug!("COLUMNS METADATA {:?}", column_defs);
        let mut set = vec![];
        while let Some(row) = self.source.next_tuple(&[])? {
            let unpacked = row.1.unpack();
            let mut data = vec![];
            for name in self.columns.iter() {
                let index = columns.get(name).unwrap().index();
                let value = match &unpacked[index] {
                    Datum::Null => ScalarValue::Null,
                    Datum::True => ScalarValue::Bool(true),
                    Datum::False => ScalarValue::Bool(false),
                    Datum::Int16(value) => ScalarValue::Num {
                        value: BigDecimal::from(*value),
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    Datum::Int32(value) => ScalarValue::Num {
                        value: BigDecimal::from(*value),
                        type_family: SqlTypeFamily::Integer,
                    },
                    Datum::Int64(value) => ScalarValue::Num {
                        value: BigDecimal::from(*value),
                        type_family: SqlTypeFamily::BigInt,
                    },
                    Datum::Float32(value) => ScalarValue::Num {
                        value: BigDecimal::from_f32(**value).unwrap(),
                        type_family: SqlTypeFamily::Real,
                    },
                    Datum::Float64(value) => ScalarValue::Num {
                        value: BigDecimal::from_f64(**value).unwrap(),
                        type_family: SqlTypeFamily::Double,
                    },
                    Datum::String(value) => ScalarValue::String(value.clone()),
                };
                data.push(value);
            }
            set.push(data);
        }
        Ok((column_defs, set))
    }
}
