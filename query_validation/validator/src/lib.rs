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

use annotated_tree::StaticEvaluationTree;
use expr_operators::{ImplicitCastError, Operation, StaticItem};
use std::collections::HashMap;
use types::{SqlFamilyType, SqlType};

#[derive(Debug, PartialEq)]
pub enum ValidationError {
    InvalidInputSyntaxForType {
        sql_type: SqlType,
        value: String,
    },
    // Error code: 22P02
    DatatypeMismatch {
        column_type: SqlType,
        source_type: Option<SqlFamilyType>,
    },
    // Error code: 42804
    StringDataRightTruncation(SqlType),
    // Error code: 22001
    UndefinedFunction {
        left: Option<SqlFamilyType>,
        op: Operation,
        right: Option<SqlFamilyType>,
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
                source_type: Some(source_type.family()),
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

    pub fn datatype_mismatch(column_type: SqlType, source_type: Option<SqlFamilyType>) -> ValidationError {
        ValidationError::DatatypeMismatch {
            column_type,
            source_type,
        }
    }

    pub fn undefined_function(
        left: Option<SqlFamilyType>,
        op: Operation,
        right: Option<SqlFamilyType>,
    ) -> ValidationError {
        ValidationError::UndefinedFunction { left, op, right }
    }
}

pub struct InsertValueValidator;

impl InsertValueValidator {
    pub fn validate(
        &self,
        tree: &mut StaticEvaluationTree,
        target_type: SqlType,
    ) -> Result<HashMap<usize, Option<SqlType>>, ValidationError> {
        match tree {
            StaticEvaluationTree::Item(StaticItem::Const(constant)) => {
                match (&constant).implicit_cast_to(target_type) {
                    Ok(casted) => {
                        *constant = casted;
                        Ok(HashMap::new())
                    }
                    Err(error) => Err(error.into()),
                }
            }
            StaticEvaluationTree::Item(StaticItem::Param(index)) => {
                let mut params = HashMap::new();
                params.insert(*index, Some(target_type));
                Ok(params)
            }
            StaticEvaluationTree::Operation { left, op, right } => {
                if !op.supported_type_family(left.kind(), right.kind()) {
                    Err(ValidationError::undefined_function(left.kind(), *op, right.kind()))
                } else {
                    match self.find_parent_family_type(left.kind(), right.kind()) {
                        None => Err(ValidationError::undefined_function(left.kind(), *op, right.kind())),
                        Some(family_type) => {
                            if !op.resulted_types().contains(&target_type.family()) {
                                Err(ValidationError::datatype_mismatch(target_type, Some(family_type)))
                            } else {
                                let mut params = HashMap::new();
                                self.validate_inner(left, target_type, family_type, &mut params)?;
                                self.validate_inner(right, target_type, family_type, &mut params)?;
                                Ok(params)
                            }
                        }
                    }
                }
            }
        }
    }

    fn find_parent_family_type(
        &self,
        left: Option<SqlFamilyType>,
        right: Option<SqlFamilyType>,
    ) -> Option<SqlFamilyType> {
        if left == right {
            left
        } else {
            match (left, right) {
                (Some(SqlFamilyType::Bool), Some(SqlFamilyType::String)) => Some(SqlFamilyType::String),
                (Some(SqlFamilyType::String), Some(SqlFamilyType::Bool)) => Some(SqlFamilyType::String),
                (Some(SqlFamilyType::Integer), Some(SqlFamilyType::String)) => Some(SqlFamilyType::String),
                (Some(SqlFamilyType::String), Some(SqlFamilyType::Integer)) => Some(SqlFamilyType::String),
                (Some(SqlFamilyType::Float), Some(SqlFamilyType::String)) => Some(SqlFamilyType::String),
                (Some(SqlFamilyType::String), Some(SqlFamilyType::Float)) => Some(SqlFamilyType::String),
                (Some(SqlFamilyType::Integer), Some(SqlFamilyType::Float)) => Some(SqlFamilyType::Float),
                (Some(SqlFamilyType::Float), Some(SqlFamilyType::Integer)) => Some(SqlFamilyType::Float),
                (Some(SqlFamilyType::Float), Some(SqlFamilyType::Bool)) => None,
                (Some(SqlFamilyType::Bool), Some(SqlFamilyType::Float)) => None,
                (Some(SqlFamilyType::Integer), Some(SqlFamilyType::Bool)) => None,
                (Some(SqlFamilyType::Bool), Some(SqlFamilyType::Integer)) => None,
                _ => None,
            }
        }
    }

    fn validate_inner(
        &self,
        tree: &mut StaticEvaluationTree,
        column_type: SqlType,
        target_type: SqlFamilyType,
        params: &mut HashMap<usize, Option<SqlType>>,
    ) -> Result<(), ValidationError> {
        match tree {
            StaticEvaluationTree::Item(StaticItem::Const(constant)) => {
                match (&constant).implicit_cast_to(column_type) {
                    Ok(casted) => {
                        *constant = casted;
                        Ok(())
                    }
                    Err(error) => Err(error.into()),
                }
            }
            StaticEvaluationTree::Item(StaticItem::Param(index)) => {
                params.insert(*index, None);
                Ok(())
            }
            StaticEvaluationTree::Operation { left, op, right } => {
                match self.find_parent_family_type(left.kind(), right.kind()) {
                    None => Err(ValidationError::undefined_function(left.kind(), *op, right.kind())),
                    Some(family_type) => {
                        if !op.resulted_types().contains(&target_type) {
                            Err(ValidationError::datatype_mismatch(column_type, Some(family_type)))
                        } else {
                            self.validate_inner(left, column_type, family_type, params)?;
                            self.validate_inner(right, column_type, family_type, params)?;
                            Ok(())
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use expr_operators::{Bool, ScalarValue};

    #[cfg(test)]
    mod strict_type_validation_of_constants {
        use super::*;

        #[test]
        fn boolean() {
            let validator = InsertValueValidator;

            assert_eq!(
                validator.validate(
                    &mut StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(Bool(true)))),
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
                    &mut StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(BigDecimal::from(0)))),
                    SqlType::small_int()
                ),
                Ok(HashMap::new())
            );
        }

        #[test]
        fn string() {
            let validator = InsertValueValidator;

            assert_eq!(
                validator.validate(
                    &mut StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::String("string".to_owned()))),
                    SqlType::var_char(255)
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

            let mut tree = StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::String("t".to_owned())));

            assert_eq!(validator.validate(&mut tree, SqlType::Bool), Ok(HashMap::new()));

            assert_eq!(
                tree,
                StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(Bool(true))))
            );
        }

        #[test]
        fn string_to_bool_failure_cast() {
            let validator = InsertValueValidator;

            assert_eq!(
                validator.validate(
                    &mut StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::String("abc".to_owned()))),
                    SqlType::Bool
                ),
                Err(ValidationError::invalid_input_syntax_for_type(SqlType::Bool, &"abc"))
            );
        }

        #[test]
        fn num_to_bool() {
            let validator = InsertValueValidator;

            let mut tree = StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(BigDecimal::from(0))));

            assert_eq!(
                validator.validate(&mut tree, SqlType::Bool),
                Err(ValidationError::datatype_mismatch(
                    SqlType::Bool,
                    Some(SqlType::integer().family())
                ))
            );
        }

        #[test]
        fn string_to_num() {
            let validator = InsertValueValidator;

            let mut tree = StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::String("123".to_owned())));

            assert_eq!(validator.validate(&mut tree, SqlType::small_int()), Ok(HashMap::new()));

            assert_eq!(
                tree,
                StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(BigDecimal::from(123))))
            );
        }

        #[test]
        fn boolean_to_number() {
            let validator = InsertValueValidator;

            let mut tree = StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(Bool(true))));

            assert_eq!(
                validator.validate(&mut tree, SqlType::small_int()),
                Err(ValidationError::datatype_mismatch(
                    SqlType::small_int(),
                    Some(SqlType::Bool.family())
                ))
            );
        }
    }

    #[test]
    fn parameter() {
        let validator = InsertValueValidator;

        let mut tree = StaticEvaluationTree::Item(StaticItem::Param(0));

        let mut params = HashMap::new();
        params.insert(0, Some(SqlType::small_int()));

        assert_eq!(validator.validate(&mut tree, SqlType::small_int()), Ok(params));
    }

    #[cfg(test)]
    mod operations {
        use super::*;

        #[cfg(test)]
        mod arithmetic {
            use super::*;
            use expr_operators::Arithmetic;

            #[test]
            fn numbers() {
                let validator = InsertValueValidator;

                let mut tree = StaticEvaluationTree::Operation {
                    left: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(
                        BigDecimal::from(1),
                    )))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(
                        BigDecimal::from(1),
                    )))),
                };

                assert_eq!(validator.validate(&mut tree, SqlType::small_int()), Ok(HashMap::new()));
            }

            #[test]
            fn booleans() {
                let validator = InsertValueValidator;

                let mut tree = StaticEvaluationTree::Operation {
                    left: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(Bool(
                        true,
                    ))))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(Bool(
                        false,
                    ))))),
                };

                assert_eq!(
                    validator.validate(&mut tree, SqlType::bool()),
                    Err(ValidationError::undefined_function(
                        Some(SqlFamilyType::Bool),
                        Operation::Arithmetic(Arithmetic::Add),
                        Some(SqlFamilyType::Bool)
                    ))
                );
            }

            #[test]
            fn params() {
                let validator = InsertValueValidator;

                let mut tree = StaticEvaluationTree::Operation {
                    left: Box::new(StaticEvaluationTree::Item(StaticItem::Param(1))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(StaticEvaluationTree::Item(StaticItem::Param(2))),
                };

                assert_eq!(
                    validator.validate(&mut tree, SqlType::integer()),
                    Err(ValidationError::undefined_function(
                        None,
                        Operation::Arithmetic(Arithmetic::Add),
                        None
                    ))
                );

                // TODO: Better type inference. Preferable output:
                //     let mut params = HashMap::new();
                //     params.insert(1, Some(SqlType::integer()));
                //     params.insert(2, Some(SqlType::integer()));
                //     assert_eq!(validator.validate(&mut tree, SqlType::integer()), Ok(params));
            }
        }

        #[cfg(test)]
        mod comparison {
            use super::*;
            use expr_operators::Comparison;

            #[ignore]
            #[test]
            fn numbers() {
                let validator = InsertValueValidator;

                let mut tree = StaticEvaluationTree::Operation {
                    left: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(
                        BigDecimal::from(1),
                    )))),
                    op: Operation::Comparison(Comparison::Eq),
                    right: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(
                        BigDecimal::from(1),
                    )))),
                };

                assert_eq!(validator.validate(&mut tree, SqlType::bool()), Ok(HashMap::new()));
            }

            #[test]
            fn booleans() {
                let validator = InsertValueValidator;

                let mut tree = StaticEvaluationTree::Operation {
                    left: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(Bool(
                        true,
                    ))))),
                    op: Operation::Comparison(Comparison::Eq),
                    right: Box::new(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(Bool(
                        false,
                    ))))),
                };

                assert_eq!(validator.validate(&mut tree, SqlType::bool()), Ok(HashMap::new()));
            }
        }
    }
}
