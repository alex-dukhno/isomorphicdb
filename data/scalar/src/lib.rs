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

use ordered_float::OrderedFloat;

#[derive(Debug, PartialEq)]
pub enum ScalarValue {
    Null,
    True,
    False,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    String(String),
}

impl ScalarValue {
    #[allow(clippy::wrong_self_convention)]
    pub fn as_text(self) -> String {
        match self {
            Self::Null => "NULL".to_owned(),
            Self::True => "t".to_owned(),
            Self::False => "f".to_owned(),
            Self::Int16(val) => val.to_string(),
            Self::Int32(val) => val.to_string(),
            Self::Int64(val) => val.to_string(),
            Self::Float32(val) => val.to_string(),
            Self::Float64(val) => val.to_string(),
            Self::String(val) => val,
        }
    }
}
