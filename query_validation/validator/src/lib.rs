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

use analysis_tree::InsertTreeNode;
use bigdecimal::BigDecimal;
use expr_operators::{Arithmetic, Operation};
use expr_operators::{Bool, ImplicitCastError, InsertOperator, ScalarValue};
use std::collections::HashMap;
use types::SqlType;

#[derive(Debug, PartialEq)]
pub enum ValidationError {
    InvalidInputSyntaxForType {
        sql_type: SqlType,
        value: String,
    }, // Error code: 22P02
    DatatypeMismatch {
        column_type: SqlType,
        source_type: SqlType,
    }, // Error code: 42804
    StringDataRightTruncation(SqlType), // Error code: 22001
    UndefinedFunction {
        left: SqlType,
        op: Operation,
        right: SqlType,
    }, // Error code: 42883
}

impl From<ImplicitCastError> for ValidationError {
    fn from(error: ImplicitCastError) -> Self {
        match error {
            ImplicitCastError::StringDataRightTruncation(sql_type) => {
                ValidationError::StringDataRightTruncation(sql_type)
            }
            ImplicitCastError::DatatypeMismatch {
                column_type,
                source_type,
            } => ValidationError::DatatypeMismatch {
                column_type,
                source_type,
            },
            ImplicitCastError::InvalidInputSyntaxForType { sql_type, value } => {
                ValidationError::InvalidInputSyntaxForType { sql_type, value }
            }
        }
    }
}

impl ValidationError {
    pub fn invalid_input_syntax_for_type<V: ToString>(sql_type: SqlType, value: V) -> ValidationError {
        ValidationError::InvalidInputSyntaxForType {
            sql_type,
            value: value.to_string(),
        }
    }

    pub fn datatype_mismatch(column_type: SqlType, source_type: SqlType) -> ValidationError {
        ValidationError::DatatypeMismatch {
            column_type,
            source_type,
        }
    }

    pub fn undefined_function(left: SqlType, op: Operation, right: SqlType) -> ValidationError {
        ValidationError::UndefinedFunction { left, op, right }
    }
}

pub struct InsertValueValidator;

impl InsertValueValidator {
    pub fn validate(
        &self,
        tree: &mut InsertTreeNode,
        target_type: SqlType,
    ) -> Result<HashMap<usize, SqlType>, ValidationError> {
        let mut params = HashMap::new();
        match tree {
            InsertTreeNode::Item(InsertOperator::Const(constant)) => match (&constant).implicit_cast_to(target_type) {
                Ok(casted) => {
                    *constant = casted;
                    Ok(params)
                }
                Err(error) => Err(error.into()),
            },
            InsertTreeNode::Item(InsertOperator::Param(index)) => {
                params.insert(*index, target_type);
                Ok(params)
            }
            InsertTreeNode::Operation { left, op, right } => Ok(params),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod strict_type_validation_of_constants {
        use super::*;

        #[test]
        fn boolean() {
            let validator = InsertValueValidator;

            assert_eq!(
                validator.validate(
                    &mut InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Bool(Bool(true)))),
                    SqlType::Bool
                ),
                Ok(HashMap::new())
            );
        }

        #[test]
        fn number() {
            let validator = InsertValueValidator;

            assert_eq!(
                validator.validate(
                    &mut InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Number(BigDecimal::from(0)))),
                    SqlType::SmallInt
                ),
                Ok(HashMap::new())
            );
        }

        #[test]
        fn string() {
            let validator = InsertValueValidator;

            assert_eq!(
                validator.validate(
                    &mut InsertTreeNode::Item(InsertOperator::Const(ScalarValue::String("string".to_owned()))),
                    SqlType::VarChar(255)
                ),
                Ok(HashMap::new())
            );
        }
    }

    #[cfg(test)]
    mod implicit_cast {
        use super::*;

        #[test]
        fn string_to_bool_successful_cast() {
            let validator = InsertValueValidator;

            let mut tree = InsertTreeNode::Item(InsertOperator::Const(ScalarValue::String("t".to_owned())));

            assert_eq!(validator.validate(&mut tree, SqlType::Bool), Ok(HashMap::new()));

            assert_eq!(
                tree,
                InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Bool(Bool(true))))
            );
        }

        #[test]
        fn string_to_bool_failure_cast() {
            let validator = InsertValueValidator;

            assert_eq!(
                validator.validate(
                    &mut InsertTreeNode::Item(InsertOperator::Const(ScalarValue::String("abc".to_owned()))),
                    SqlType::Bool
                ),
                Err(ValidationError::invalid_input_syntax_for_type(SqlType::Bool, &"abc"))
            );
        }

        #[test]
        fn num_to_bool() {
            let validator = InsertValueValidator;

            let mut tree = InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Number(BigDecimal::from(0))));

            assert_eq!(
                validator.validate(&mut tree, SqlType::Bool),
                Err(ValidationError::datatype_mismatch(SqlType::Bool, SqlType::Integer))
            );
        }

        #[test]
        fn string_to_num() {
            let validator = InsertValueValidator;

            let mut tree = InsertTreeNode::Item(InsertOperator::Const(ScalarValue::String("123".to_owned())));

            assert_eq!(validator.validate(&mut tree, SqlType::SmallInt), Ok(HashMap::new()));

            assert_eq!(
                tree,
                InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Number(BigDecimal::from(123))))
            );
        }

        #[test]
        fn boolean_to_number() {
            let validator = InsertValueValidator;

            let mut tree = InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Bool(Bool(true))));

            assert_eq!(
                validator.validate(&mut tree, SqlType::SmallInt),
                Err(ValidationError::datatype_mismatch(SqlType::SmallInt, SqlType::Bool))
            );
        }
    }

    #[test]
    fn parameter() {
        let validator = InsertValueValidator;

        let mut tree = InsertTreeNode::Item(InsertOperator::Param(0));

        let mut params = HashMap::new();
        params.insert(0, SqlType::SmallInt);

        assert_eq!(validator.validate(&mut tree, SqlType::SmallInt), Ok(params));
    }

    #[cfg(test)]
    mod operations {
        use super::*;

        #[cfg(test)]
        mod arithmetic {
            use super::*;

            #[test]
            fn numbers() {
                let validator = InsertValueValidator;

                let mut tree = InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Number(
                        BigDecimal::from(1),
                    )))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Number(
                        BigDecimal::from(1),
                    )))),
                };

                assert_eq!(validator.validate(&mut tree, SqlType::SmallInt), Ok(HashMap::new()));
            }

            #[test]
            fn booleans() {
                let validator = InsertValueValidator;

                let mut tree = InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Bool(Bool(
                        true,
                    ))))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(InsertTreeNode::Item(InsertOperator::Const(ScalarValue::Bool(Bool(
                        false,
                    ))))),
                };

                assert_eq!(
                    validator.validate(&mut tree, SqlType::SmallInt),
                    Err(ValidationError::undefined_function(
                        SqlType::Bool,
                        Operation::Arithmetic(Arithmetic::Add),
                        SqlType::Bool
                    ))
                );
            }
        }
    }
}
