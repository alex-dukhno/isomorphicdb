// Copyright 2020 Alex Dukhno
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

mod dynamic_expr;
mod static_expr;

pub use dynamic_expr::DynamicExpressionEvaluation;
pub use static_expr::StaticExpressionEvaluation;

#[derive(Debug, PartialEq)]
pub enum EvalError {
    UndefinedFunction(String, String, String),
    NonValue(String),
}

impl EvalError {
    fn undefined_function<O: ToString, S: ToString>(op: &O, left_type: &S, right_type: &S) -> EvalError {
        EvalError::UndefinedFunction(op.to_string(), left_type.to_string(), right_type.to_string())
    }

    fn not_a_value<V: ToString>(v: &V) -> EvalError {
        EvalError::NonValue(v.to_string())
    }
}

#[cfg(test)]
mod tests;
