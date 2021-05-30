// Copyright 2020 - 2021 Alex Dukhno
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

#![allow(clippy::type_complexity)]

use bigdecimal::{BigDecimal, FromPrimitive};
use binary::BinaryValue;
use definition::ColumnDef;
use query_response::QueryEvent;
use query_result::QueryExecutionError;
use scalar::ScalarValue;
use std::collections::HashMap;
use storage::{Cursor, TableRef};
use typed_tree::TypedTreeOld;
use types::{SqlType, SqlTypeFamily};

#[derive(Debug, PartialEq)]
pub enum QueryExecutionResult {
    Inserted(usize),
    Deleted(usize),
    Updated(usize),
    Selected((Vec<(String, u32)>, Vec<Vec<ScalarValue>>)),
}

impl From<QueryExecutionResult> for Vec<QueryEvent> {
    fn from(plan_result: QueryExecutionResult) -> Vec<QueryEvent> {
        match plan_result {
            QueryExecutionResult::Inserted(inserted) => vec![QueryEvent::RecordsInserted(inserted)],
            QueryExecutionResult::Deleted(inserted) => vec![QueryEvent::RecordsDeleted(inserted)],
            QueryExecutionResult::Updated(inserted) => vec![QueryEvent::RecordsUpdated(inserted)],
            QueryExecutionResult::Selected((desc, data)) => {
                let mut events = vec![QueryEvent::RowDescription(desc)];
                let len = data.len();
                for row in data {
                    events.push(QueryEvent::DataRow(row.into_iter().map(|scalar| scalar.as_text()).collect()));
                }
                events.push(QueryEvent::RecordsSelected(len));
                events
            }
        }
    }
}

// TODO: ReadOnly, ReadWrite plan
pub enum QueryPlan {
    Insert(InsertQueryPlan),
    Delete(DeleteQueryPlan),
    Update(UpdateQueryPlan),
    Select(SelectQueryPlan),
}

impl QueryPlan {
    pub fn execute(self, param_values: Vec<ScalarValue>) -> Result<QueryExecutionResult, QueryExecutionError> {
        match self {
            QueryPlan::Insert(insert_query_plan) => insert_query_plan.execute(param_values).map(QueryExecutionResult::Inserted),
            QueryPlan::Delete(delete_query_plan) => delete_query_plan.execute(param_values).map(QueryExecutionResult::Deleted),
            QueryPlan::Update(update_query_plan) => update_query_plan.execute(param_values).map(QueryExecutionResult::Updated),
            QueryPlan::Select(select_query_plan) => select_query_plan.execute(param_values).map(QueryExecutionResult::Selected),
        }
    }
}

pub trait Flow {
    type Output;

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError>;
}

pub struct StaticValues(Box<dyn Iterator<Item = Vec<Option<TypedTreeOld>>>>);

impl StaticValues {
    pub fn new(values: Vec<Vec<Option<TypedTreeOld>>>) -> Box<StaticValues> {
        Box::new(StaticValues(Box::new(values.into_iter())))
    }
}

impl Flow for StaticValues {
    type Output = Vec<Option<TypedTreeOld>>;

    fn next_tuple(&mut self, _param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        Ok(self.0.next())
    }
}

pub struct StaticExpressionEval {
    source: Box<dyn Flow<Output = Vec<Option<TypedTreeOld>>>>,
}

impl StaticExpressionEval {
    pub fn new(source: Box<dyn Flow<Output = Vec<Option<TypedTreeOld>>>>) -> Box<StaticExpressionEval> {
        Box::new(StaticExpressionEval { source })
    }
}

impl Flow for StaticExpressionEval {
    type Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>);

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Ok(Some(tuple)) = self.source.next_tuple(param_values) {
            let mut next_tuple = vec![];
            for value in tuple {
                let typed_value = match value {
                    None => None,
                    Some(value) => match value.eval(param_values, &[]) {
                        Err(error) => return Err(error),
                        Ok(value) => Some(value),
                    },
                };
                next_tuple.push(typed_value);
            }
            Ok(Some((vec![ScalarValue::Null], next_tuple)))
        } else {
            Ok(None)
        }
    }
}

pub struct ConstraintValidator {
    source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>)>>,
    column_types: Vec<(String, SqlTypeFamily)>,
}

impl ConstraintValidator {
    pub fn new(
        source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>)>>,
        column_types: Vec<(String, SqlTypeFamily)>,
    ) -> Box<ConstraintValidator> {
        Box::new(ConstraintValidator { source, column_types })
    }
}

impl Flow for ConstraintValidator {
    type Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>);

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Some((key, tuple)) = self.source.next_tuple(param_values)? {
            log::debug!("ConstraintValidator key - {:?}", key);
            let mut data = vec![];
            for (index, value) in tuple.into_iter().enumerate() {
                let value = match (value, self.column_types[index].1) {
                    (None, _) => None,
                    (Some(value), type_family) => match value.type_family() {
                        None => unimplemented!(),
                        Some(value_type) => match value_type.compare(&type_family) {
                            Ok(wide_type_family) => {
                                log::debug!("ConstraintValidator {:?} {:?} {:?}", value, wide_type_family, type_family);
                                match (value.clone(), type_family) {
                                    (ScalarValue::Num { value, .. }, SqlTypeFamily::SmallInt) => {
                                        if !(BigDecimal::from(i16::MIN)..=BigDecimal::from(i16::MAX)).contains(&value) {
                                            return Err(QueryExecutionError::out_of_range(
                                                type_family,
                                                self.column_types[index].0.as_str(),
                                                index + 1,
                                            ));
                                        }
                                    }
                                    (ScalarValue::Num { value, .. }, SqlTypeFamily::Integer) => {
                                        if !(BigDecimal::from(i32::MIN)..=BigDecimal::from(i32::MAX)).contains(&value) {
                                            return Err(QueryExecutionError::out_of_range(
                                                type_family,
                                                self.column_types[index].0.as_str(),
                                                index + 1,
                                            ));
                                        }
                                    }
                                    (ScalarValue::Num { value, .. }, SqlTypeFamily::BigInt) => {
                                        if !(BigDecimal::from(i64::MIN)..=BigDecimal::from(i64::MAX)).contains(&value) {
                                            return Err(QueryExecutionError::out_of_range(
                                                type_family,
                                                self.column_types[index].0.as_str(),
                                                index + 1,
                                            ));
                                        }
                                    }
                                    (ScalarValue::String(_), _) => {}
                                    (ScalarValue::Bool(_), _) => {}
                                    _ => unimplemented!(),
                                }
                                Some(value)
                            }
                            Err(_) => {
                                return Err(QueryExecutionError::invalid_text_representation(type_family, value));
                            }
                        },
                    },
                };
                data.push(value);
            }
            Ok(Some((key, data)))
        } else {
            Ok(None)
        }
    }
}

pub struct InsertQueryPlan {
    source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>)>>,
    table: TableRef,
}

impl InsertQueryPlan {
    pub fn new(source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>)>>, table: TableRef) -> InsertQueryPlan {
        InsertQueryPlan { source, table }
    }

    pub fn execute(mut self, param_values: Vec<ScalarValue>) -> Result<usize, QueryExecutionError> {
        let mut len = 0;
        while let Some((_, data)) = self.source.next_tuple(&param_values)? {
            self.table.write(
                data.into_iter()
                    .map(|v| v.map(|v| v.convert()).unwrap_or_else(BinaryValue::null))
                    .collect::<Vec<BinaryValue>>(),
            );
            len += 1;
        }
        Ok(len)
    }
}

pub struct Filter {
    source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>,
    predicate: Option<TypedTreeOld>,
}

impl Filter {
    pub fn new(source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>, predicate: Option<TypedTreeOld>) -> Box<Filter> {
        Box::new(Filter { source, predicate })
    }
}

impl Flow for Filter {
    type Output = (Vec<ScalarValue>, Vec<ScalarValue>);

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        while let Some((key, value)) = self.source.next_tuple(param_values)? {
            match &self.predicate {
                None => return Ok(Some((key, value))),
                Some(predicate) => {
                    log::debug!("Filter before: {:?}, {:?}", key, value);
                    let result = predicate.clone().eval(param_values, &value);
                    log::debug!("Filter after: {:?}", result);
                    if let Ok(ScalarValue::Bool(true)) = result {
                        log::debug!("Filter filtered key - {:?}", key);
                        return Ok(Some((key, value)));
                    }
                }
            }
        }
        Ok(None)
    }
}

pub struct Projection {
    source: Box<dyn Flow<Output = (Vec<BinaryValue>, Vec<BinaryValue>)>>,
}

impl Projection {
    pub fn new(source: Box<dyn Flow<Output = (Vec<BinaryValue>, Vec<BinaryValue>)>>) -> Box<Projection> {
        Box::new(Projection { source })
    }
}

impl Flow for Projection {
    type Output = (Vec<ScalarValue>, Vec<ScalarValue>);

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        fn mapper(datum: &BinaryValue) -> ScalarValue {
            match datum {
                BinaryValue::Null => ScalarValue::Null,
                BinaryValue::Bool(boolean) => ScalarValue::Bool(*boolean),
                BinaryValue::Int16(value) => ScalarValue::Num {
                    value: BigDecimal::from(*value),
                    type_family: SqlTypeFamily::SmallInt,
                },
                BinaryValue::Int32(value) => ScalarValue::Num {
                    value: BigDecimal::from(*value),
                    type_family: SqlTypeFamily::Integer,
                },
                BinaryValue::Int64(value) => ScalarValue::Num {
                    value: BigDecimal::from(*value),
                    type_family: SqlTypeFamily::BigInt,
                },
                BinaryValue::Float32(value) => ScalarValue::Num {
                    value: BigDecimal::from_f32(**value).unwrap(),
                    type_family: SqlTypeFamily::Real,
                },
                BinaryValue::Float64(value) => ScalarValue::Num {
                    value: BigDecimal::from_f64(**value).unwrap(),
                    type_family: SqlTypeFamily::Double,
                },
                BinaryValue::String(value) => ScalarValue::String(value.clone()),
            }
        }

        if let Some(row) = self.source.next_tuple(&param_values)? {
            let key = row.0;
            let value = row.1;
            Ok(Some((
                key.iter().map(mapper).collect::<Vec<ScalarValue>>(),
                value.iter().map(mapper).collect::<Vec<ScalarValue>>(),
            )))
        } else {
            Ok(None)
        }
    }
}

pub struct FullTableScan {
    source: Cursor,
}

impl FullTableScan {
    pub fn new(source: &TableRef) -> Box<FullTableScan> {
        Box::new(FullTableScan { source: source.scan() })
    }
}

impl Flow for FullTableScan {
    type Output = (Vec<BinaryValue>, Vec<BinaryValue>);

    fn next_tuple(&mut self, _param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        let record = self.source.next();
        log::debug!("TABLE RECORD {:?}", record);
        Ok(record)
    }
}

pub struct TableRecordKeys {
    source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>,
}

impl TableRecordKeys {
    pub fn new(source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>) -> Box<TableRecordKeys> {
        Box::new(TableRecordKeys { source })
    }
}

impl Flow for TableRecordKeys {
    type Output = Vec<BinaryValue>;

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Some((key, _value)) = self.source.next_tuple(param_values)? {
            Ok(Some(key.into_iter().map(|v| v.convert()).collect::<Vec<BinaryValue>>()))
        } else {
            Ok(None)
        }
    }
}

pub struct DeleteQueryPlan {
    source: Box<dyn Flow<Output = Vec<BinaryValue>>>,
    table: TableRef,
}

impl DeleteQueryPlan {
    pub fn new(source: Box<dyn Flow<Output = Vec<BinaryValue>>>, table: TableRef) -> DeleteQueryPlan {
        DeleteQueryPlan { source, table }
    }

    pub fn execute(mut self, param_values: Vec<ScalarValue>) -> Result<usize, QueryExecutionError> {
        let mut len = 0;
        while let Some(key) = self.source.next_tuple(&param_values)? {
            self.table.write_key(key, None);
            len += 1;
        }
        Ok(len)
    }
}

pub struct Repeater {
    source: Vec<Option<TypedTreeOld>>,
}

impl Repeater {
    pub fn new(source: Vec<Option<TypedTreeOld>>) -> Box<Repeater> {
        Box::new(Repeater { source })
    }
}

impl Flow for Repeater {
    type Output = Vec<Option<TypedTreeOld>>;

    fn next_tuple(&mut self, _param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        Ok(Some(self.source.clone()))
    }
}

pub struct DynamicValues {
    source: Box<dyn Flow<Output = Vec<Option<TypedTreeOld>>>>,
    records: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>,
}

impl DynamicValues {
    pub fn new(
        source: Box<dyn Flow<Output = Vec<Option<TypedTreeOld>>>>,
        records: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>,
    ) -> Box<DynamicValues> {
        Box::new(DynamicValues { source, records })
    }
}

impl Flow for DynamicValues {
    type Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>);

    fn next_tuple(&mut self, param_values: &[ScalarValue]) -> Result<Option<Self::Output>, QueryExecutionError> {
        if let Some((key, value)) = self.records.next_tuple(param_values)? {
            let table_row = value;
            log::trace!("[DynamicValues] VALUES - {:?}", table_row);
            if let Some(tuple) = self.source.next_tuple(param_values)? {
                let mut next_tuple = vec![];
                for value in tuple {
                    let value = match value {
                        None => None,
                        Some(tree) => match tree.eval(param_values, &table_row) {
                            Err(error) => return Err(error),
                            Ok(value) => Some(value),
                        },
                    };
                    next_tuple.push(value);
                }
                Ok(Some((key, next_tuple)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

pub struct UpdateQueryPlan {
    values: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>)>>,
    records: Box<dyn Flow<Output = (Vec<BinaryValue>, Vec<BinaryValue>)>>,
    table: TableRef,
}

impl UpdateQueryPlan {
    pub fn new(
        values: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<Option<ScalarValue>>)>>,
        records: Box<dyn Flow<Output = (Vec<BinaryValue>, Vec<BinaryValue>)>>,
        table: TableRef,
    ) -> UpdateQueryPlan {
        UpdateQueryPlan { values, records, table }
    }

    pub fn execute(mut self, param_values: Vec<ScalarValue>) -> Result<usize, QueryExecutionError> {
        fn mapper(datum: BinaryValue) -> ScalarValue {
            match datum {
                BinaryValue::Null => ScalarValue::Null,
                BinaryValue::Bool(boolean) => ScalarValue::Bool(boolean),
                BinaryValue::Int16(value) => ScalarValue::Num {
                    value: BigDecimal::from(value),
                    type_family: SqlTypeFamily::SmallInt,
                },
                BinaryValue::Int32(value) => ScalarValue::Num {
                    value: BigDecimal::from(value),
                    type_family: SqlTypeFamily::Integer,
                },
                BinaryValue::Int64(value) => ScalarValue::Num {
                    value: BigDecimal::from(value),
                    type_family: SqlTypeFamily::BigInt,
                },
                BinaryValue::Float32(value) => ScalarValue::Num {
                    value: BigDecimal::from_f32(*value).unwrap(),
                    type_family: SqlTypeFamily::Real,
                },
                BinaryValue::Float64(value) => ScalarValue::Num {
                    value: BigDecimal::from_f64(*value).unwrap(),
                    type_family: SqlTypeFamily::Double,
                },
                BinaryValue::String(value) => ScalarValue::String(value),
            }
        }

        let mut len = 0;
        let mut values = HashMap::new();
        while let Some((updated_key, value)) = self.values.next_tuple(&param_values)? {
            values.insert(updated_key, value);
        }
        while let Some((key, row)) = self.records.next_tuple(&param_values)? {
            let mut unpacked = row;
            let unpacked_key = key.clone().into_iter().map(mapper).collect::<Vec<ScalarValue>>();
            if let Some(value) = values.remove(&unpacked_key) {
                for (index, value) in value.into_iter().enumerate() {
                    let new_value = match value {
                        None => unpacked[index].clone(),
                        Some(value) => value.convert(),
                    };
                    unpacked[index] = new_value;
                }
                let new_row = unpacked;
                self.table.write_key(key, Some(new_row));
                len += 1;
            }
        }
        Ok(len)
    }
}

pub struct SelectQueryPlan {
    source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>,
    columns: Vec<String>,
    column_types: Vec<(String, SqlType)>,
}

impl SelectQueryPlan {
    pub fn new(
        source: Box<dyn Flow<Output = (Vec<ScalarValue>, Vec<ScalarValue>)>>,
        columns: Vec<String>,
        column_types: Vec<(String, SqlType)>,
    ) -> SelectQueryPlan {
        SelectQueryPlan {
            source,
            columns,
            column_types,
        }
    }

    pub fn execute(mut self, param_values: Vec<ScalarValue>) -> Result<(Vec<(String, u32)>, Vec<Vec<ScalarValue>>), QueryExecutionError> {
        log::debug!("COLUMNS TO SELECT {:?}", self.columns);
        let mut column_defs = vec![];
        let columns = self
            .column_types
            .iter()
            .enumerate()
            .map(|(index, (name, sql_type))| (name.clone(), ColumnDef::new(name.clone(), *sql_type, index)))
            .collect::<HashMap<String, ColumnDef>>();
        for name in self.columns.iter() {
            let column = columns.get(name).unwrap();
            column_defs.push((column.name().to_owned(), (&column.sql_type()).into()));
        }
        log::debug!("COLUMNS METADATA {:?}", column_defs);
        let mut set = vec![];
        while let Some((_key, value)) = self.source.next_tuple(&param_values)? {
            let mut data = vec![];
            for name in self.columns.iter() {
                let index = columns.get(name).unwrap().index();
                let value = value[index].clone();
                data.push(value);
            }
            set.push(data);
        }
        Ok((column_defs, set))
    }
}
