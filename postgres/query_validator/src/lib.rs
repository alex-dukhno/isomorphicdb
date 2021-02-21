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

use ast::{ColumnDef, DataType, Definition, Statement};
use postgres_parser::nodes::{A_Const, CreateSchemaStmt, CreateStmt, DropStmt, IndexElem, IndexStmt, TypeName, Value};
use postgres_parser::sys::{DropBehavior, ObjectType};
use postgres_parser::{Node, SqlStatementScanner};

pub struct QueryValidator;

impl QueryValidator {
    pub fn new() -> QueryValidator {
        QueryValidator
    }

    pub fn validate(&self, scanner: SqlStatementScanner) -> Result<Statement, ()> {
        for scanned_query in scanner.into_iter() {
            match scanned_query.parsetree.unwrap().unwrap() {
                Node::A_ArrayExpr(_) => {}
                Node::A_Const(_) => {}
                Node::A_Expr(_) => {}
                Node::A_Indices(_) => {}
                Node::A_Indirection(_) => {}
                Node::A_Star(_) => {}
                Node::AccessPriv(_) => {}
                Node::Aggref(_) => {}
                Node::Alias(_) => {}
                Node::AlterCollationStmt(_) => {}
                Node::AlterDatabaseSetStmt(_) => {}
                Node::AlterDatabaseStmt(_) => {}
                Node::AlterDefaultPrivilegesStmt(_) => {}
                Node::AlterDomainStmt(_) => {}
                Node::AlterEnumStmt(_) => {}
                Node::AlterEventTrigStmt(_) => {}
                Node::AlterExtensionContentsStmt(_) => {}
                Node::AlterExtensionStmt(_) => {}
                Node::AlterFdwStmt(_) => {}
                Node::AlterForeignServerStmt(_) => {}
                Node::AlterFunctionStmt(_) => {}
                Node::AlterObjectDependsStmt(_) => {}
                Node::AlterObjectSchemaStmt(_) => {}
                Node::AlterOpFamilyStmt(_) => {}
                Node::AlterOperatorStmt(_) => {}
                Node::AlterOwnerStmt(_) => {}
                Node::AlterPolicyStmt(_) => {}
                Node::AlterPublicationStmt(_) => {}
                Node::AlterRoleSetStmt(_) => {}
                Node::AlterRoleStmt(_) => {}
                Node::AlterSeqStmt(_) => {}
                Node::AlterStatsStmt(_) => {}
                Node::AlterSubscriptionStmt(_) => {}
                Node::AlterSystemStmt(_) => {}
                Node::AlterTSConfigurationStmt(_) => {}
                Node::AlterTSDictionaryStmt(_) => {}
                Node::AlterTableCmd(_) => {}
                Node::AlterTableMoveAllStmt(_) => {}
                Node::AlterTableSpaceOptionsStmt(_) => {}
                Node::AlterTableStmt(_) => {}
                Node::AlterTypeStmt(_) => {}
                Node::AlterUserMappingStmt(_) => {}
                Node::AlternativeSubPlan(_) => {}
                Node::ArrayCoerceExpr(_) => {}
                Node::ArrayExpr(_) => {}
                Node::BoolExpr(_) => {}
                Node::BooleanTest(_) => {}
                Node::CallContext(_) => {}
                Node::CallStmt(_) => {}
                Node::CaseExpr(_) => {}
                Node::CaseTestExpr(_) => {}
                Node::CaseWhen(_) => {}
                Node::CheckPointStmt(_) => {}
                Node::ClosePortalStmt(_) => {}
                Node::ClusterStmt(_) => {}
                Node::CoalesceExpr(_) => {}
                Node::CoerceToDomain(_) => {}
                Node::CoerceToDomainValue(_) => {}
                Node::CoerceViaIO(_) => {}
                Node::CollateClause(_) => {}
                Node::CollateExpr(_) => {}
                Node::ColumnDef(_) => {}
                Node::ColumnRef(_) => {}
                Node::CommentStmt(_) => {}
                Node::CommonTableExpr(_) => {}
                Node::CompositeTypeStmt(_) => {}
                Node::Const(_) => {}
                Node::Constraint(_) => {}
                Node::ConstraintsSetStmt(_) => {}
                Node::ConvertRowtypeExpr(_) => {}
                Node::CopyStmt(_) => {}
                Node::CreateAmStmt(_) => {}
                Node::CreateCastStmt(_) => {}
                Node::CreateConversionStmt(_) => {}
                Node::CreateDomainStmt(_) => {}
                Node::CreateEnumStmt(_) => {}
                Node::CreateEventTrigStmt(_) => {}
                Node::CreateExtensionStmt(_) => {}
                Node::CreateFdwStmt(_) => {}
                Node::CreateForeignServerStmt(_) => {}
                Node::CreateForeignTableStmt(_) => {}
                Node::CreateFunctionStmt(_) => {}
                Node::CreateOpClassItem(_) => {}
                Node::CreateOpClassStmt(_) => {}
                Node::CreateOpFamilyStmt(_) => {}
                Node::CreatePLangStmt(_) => {}
                Node::CreatePolicyStmt(_) => {}
                Node::CreatePublicationStmt(_) => {}
                Node::CreateRangeStmt(_) => {}
                Node::CreateRoleStmt(_) => {}
                Node::CreateSchemaStmt(CreateSchemaStmt {
                    schemaname: schema_name,
                    authrole: _auth_role,
                    schemaElts: _schema_elements,
                    if_not_exists,
                }) => {
                    return Ok(Statement::DDL(Definition::CreateSchema {
                        schema_name: schema_name.unwrap(),
                        if_not_exists,
                    }))
                }
                Node::CreateSeqStmt(_) => {}
                Node::CreateStatsStmt(_) => {}
                Node::CreateStmt(CreateStmt {
                    relation: table_name,
                    tableElts: table_elements,
                    inhRelations: _inheritance_tables,
                    partbound: _partition_bound,
                    partspec: _partition_spec,
                    ofTypename: _type_name,
                    constraints: _constraints,
                    options: _options,
                    oncommit: _on_commit,
                    tablespacename: _table_space_name,
                    accessMethod: _access_method,
                    if_not_exists,
                }) => {
                    let mut columns = vec![];
                    for table_element in table_elements.unwrap() {
                        columns.push(self.process_column(table_element)?);
                    }
                    let table_name = table_name.unwrap();
                    return Ok(Statement::DDL(Definition::CreateTable {
                        if_not_exists,
                        schema_name: table_name.schemaname.unwrap_or_else(|| "public".to_owned()),
                        table_name: table_name.relname.unwrap(),
                        columns,
                    }));
                }
                Node::CreateSubscriptionStmt(_) => {}
                Node::CreateTableAsStmt(_) => {}
                Node::CreateTableSpaceStmt(_) => {}
                Node::CreateTransformStmt(_) => {}
                Node::CreateTrigStmt(_) => {}
                Node::CreateUserMappingStmt(_) => {}
                Node::CreatedbStmt(_) => {}
                Node::CurrentOfExpr(_) => {}
                Node::DeallocateStmt(_) => {}
                Node::DeclareCursorStmt(_) => {}
                Node::DefElem(_) => {}
                Node::DefineStmt(_) => {}
                Node::DeleteStmt(_) => {}
                Node::DiscardStmt(_) => {}
                Node::DoStmt(_) => {}
                Node::DropOwnedStmt(_) => {}
                Node::DropRoleStmt(_) => {}
                Node::DropStmt(DropStmt {
                    objects,
                    removeType: remove_type,
                    behavior,
                    missing_ok,
                    concurrent: _concurrent,
                }) => {
                    match remove_type {
                        ObjectType::OBJECT_SCHEMA => {
                            let mut names = vec![];
                            for object in objects.unwrap() {
                                println!("OBJECT - {:?}", object);
                                match object {
                                    Node::Value(Value { string: Some(name), .. }) => names.push(name),
                                    _ => unimplemented!(),
                                }
                            }
                            return Ok(Statement::DDL(Definition::DropSchemas {
                                names,
                                if_exists: missing_ok,
                                cascade: behavior == DropBehavior::DROP_CASCADE,
                            }));
                        }
                        ObjectType::OBJECT_TABLE => {
                            let mut names = vec![];
                            for object in objects.unwrap() {
                                println!("OBJECT - {:?}", object);
                                match object {
                                    Node::List(mut values) => {
                                        if values.len() == 1 {
                                            match values.pop() {
                                                Some(Node::Value(Value { string: Some(name), .. })) => {
                                                    names.push(("public".to_owned(), name))
                                                }
                                                _ => unimplemented!(),
                                            }
                                        } else if values.len() == 2 {
                                            match (values.pop(), values.pop()) {
                                                (
                                                    Some(Node::Value(Value { string: Some(name), .. })),
                                                    Some(Node::Value(Value {
                                                        string: Some(schema), ..
                                                    })),
                                                ) => names.push((schema, name)),
                                                _ => unimplemented!(),
                                            }
                                        } else {
                                            unimplemented!()
                                        }
                                    }
                                    _ => unimplemented!(),
                                }
                            }
                            return Ok(Statement::DDL(Definition::DropTables {
                                names,
                                if_exists: missing_ok,
                                cascade: behavior == DropBehavior::DROP_CASCADE,
                            }));
                        }
                        _ => unimplemented!(),
                    };
                }
                Node::DropSubscriptionStmt(_) => {}
                Node::DropTableSpaceStmt(_) => {}
                Node::DropUserMappingStmt(_) => {}
                Node::DropdbStmt(_) => {}
                Node::ExecuteStmt(_) => {}
                Node::ExplainStmt(_) => {}
                Node::Expr(_) => {}
                Node::FetchStmt(_) => {}
                Node::FieldSelect(_) => {}
                Node::FieldStore(_) => {}
                Node::FromExpr(_) => {}
                Node::FuncCall(_) => {}
                Node::FuncExpr(_) => {}
                Node::FunctionParameter(_) => {}
                Node::GrantRoleStmt(_) => {}
                Node::GrantStmt(_) => {}
                Node::GroupingFunc(_) => {}
                Node::GroupingSet(_) => {}
                Node::ImportForeignSchemaStmt(_) => {}
                Node::IndexElem(_) => {}
                Node::IndexStmt(IndexStmt {
                    idxname: index_name,
                    relation: table_name,
                    accessMethod: _access_method,
                    tableSpace: _table_space,
                    indexParams: index_params,
                    indexIncludingParams: _index_including_params,
                    options: _options,
                    whereClause: _where_clause,
                    excludeOpNames: _exclude_op_names,
                    idxcomment: _index_comment,
                    indexOid: _index_oid,
                    oldNode: _old_node,
                    oldCreateSubid: _old_create_sub_id,
                    oldFirstRelfilenodeSubid: _old_first_rel_file_node_sub_id,
                    unique: _unique,
                    primary: _primary,
                    isconstraint: _is_constraint,
                    deferrable: _deferrable,
                    initdeferred: _init_deferred,
                    transformed: _transformed,
                    concurrent: _concurrent,
                    if_not_exists: _if_not_exists,
                    reset_default_tblspc: _reset_default_table_space,
                }) => {
                    let mut column_names = vec![];
                    for index_param in index_params.unwrap() {
                        println!("INDEX PARAM - {:?}", index_param);
                        match index_param {
                            Node::IndexElem(IndexElem { name: Some(name), .. }) => column_names.push(name),
                            _ => unimplemented!(),
                        }
                    }
                    let table_name = table_name.unwrap();
                    return Ok(Statement::DDL(Definition::CreateIndex {
                        name: index_name.unwrap(),
                        table_name: (
                            table_name.schemaname.unwrap_or_else(|| "public".to_owned()),
                            table_name.relname.unwrap(),
                        ),
                        column_names,
                    }));
                }
                Node::InferClause(_) => {}
                Node::InferenceElem(_) => {}
                Node::InlineCodeBlock(_) => {}
                Node::InsertStmt(_) => {}
                Node::IntoClause(_) => {}
                Node::JoinExpr(_) => {}
                Node::List(_) => {}
                Node::ListenStmt(_) => {}
                Node::LoadStmt(_) => {}
                Node::LockStmt(_) => {}
                Node::LockingClause(_) => {}
                Node::MinMaxExpr(_) => {}
                Node::MultiAssignRef(_) => {}
                Node::NamedArgExpr(_) => {}
                Node::NextValueExpr(_) => {}
                Node::NotifyStmt(_) => {}
                Node::NullTest(_) => {}
                Node::ObjectWithArgs(_) => {}
                Node::OnConflictClause(_) => {}
                Node::OnConflictExpr(_) => {}
                Node::OpExpr(_) => {}
                Node::Param(_) => {}
                Node::ParamRef(_) => {}
                Node::PartitionBoundSpec(_) => {}
                Node::PartitionCmd(_) => {}
                Node::PartitionElem(_) => {}
                Node::PartitionRangeDatum(_) => {}
                Node::PartitionSpec(_) => {}
                Node::PrepareStmt(_) => {}
                Node::RangeFunction(_) => {}
                Node::RangeSubselect(_) => {}
                Node::RangeTableFunc(_) => {}
                Node::RangeTableFuncCol(_) => {}
                Node::RangeTableSample(_) => {}
                Node::RangeTblRef(_) => {}
                Node::RangeVar(_) => {}
                Node::RawStmt(_) => {}
                Node::ReassignOwnedStmt(_) => {}
                Node::RefreshMatViewStmt(_) => {}
                Node::ReindexStmt(_) => {}
                Node::RelabelType(_) => {}
                Node::RenameStmt(_) => {}
                Node::ReplicaIdentityStmt(_) => {}
                Node::ResTarget(_) => {}
                Node::RoleSpec(_) => {}
                Node::RowCompareExpr(_) => {}
                Node::RowExpr(_) => {}
                Node::RowMarkClause(_) => {}
                Node::RuleStmt(_) => {}
                Node::SQLValueFunction(_) => {}
                Node::ScalarArrayOpExpr(_) => {}
                Node::SecLabelStmt(_) => {}
                Node::SelectStmt(_) => {}
                Node::SetOperationStmt(_) => {}
                Node::SetToDefault(_) => {}
                Node::SortBy(_) => {}
                Node::SortGroupClause(_) => {}
                Node::SubLink(_) => {}
                Node::SubscriptingRef(_) => {}
                Node::TableFunc(_) => {}
                Node::TableLikeClause(_) => {}
                Node::TableSampleClause(_) => {}
                Node::TargetEntry(_) => {}
                Node::TransactionStmt(_) => {}
                Node::TriggerTransition(_) => {}
                Node::TruncateStmt(_) => {}
                Node::TypeCast(_) => {}
                Node::TypeName(_) => {}
                Node::UnlistenStmt(_) => {}
                Node::UpdateStmt(_) => {}
                Node::VacuumRelation(_) => {}
                Node::VacuumStmt(_) => {}
                Node::Value(_) => {}
                Node::Var(_) => {}
                Node::VariableSetStmt(_) => {}
                Node::VariableShowStmt(_) => {}
                Node::ViewStmt(_) => {}
                Node::WindowClause(_) => {}
                Node::WindowDef(_) => {}
                Node::WindowFunc(_) => {}
                Node::WithCheckOption(_) => {}
                Node::WithClause(_) => {}
                Node::XmlExpr(_) => {}
                Node::XmlSerialize(_) => {}
            }
        }
        Err(())
    }

    fn process_column(&self, node: Node) -> Result<ColumnDef, ()> {
        if let Node::ColumnDef(column_def) = node {
            let data_type = self.process_type(*column_def.typeName.unwrap())?;
            Ok(ColumnDef {
                name: column_def.colname.unwrap(),
                data_type,
            })
        } else {
            Err(())
        }
    }

    fn process_type(&self, type_name: TypeName) -> Result<DataType, ()> {
        println!("TYPE NAME {:#?}", type_name);
        let name = type_name.names.unwrap();
        let mode = type_name.typmods;
        match &name[1] {
            Node::Value(Value { string, .. }) if string.as_deref() == Some("int2") => Ok(DataType::SmallInt),
            Node::Value(Value { string, .. }) if string.as_deref() == Some("int4") => Ok(DataType::Int),
            Node::Value(Value { string, .. }) if string.as_deref() == Some("int8") => Ok(DataType::BigInt),
            Node::Value(Value { string, .. }) if string.as_deref() == Some("float4") => Ok(DataType::Real),
            Node::Value(Value { string, .. }) if string.as_deref() == Some("float8") => Ok(DataType::Double),
            Node::Value(Value { string, .. }) if string.as_deref() == Some("bool") => Ok(DataType::Bool),
            Node::Value(Value { string, .. }) if string.as_deref() == Some("bpchar") => {
                match mode.as_ref().map(|inner| &inner[0]) {
                    Some(&Node::A_Const(A_Const {
                        val: Value { int: None, .. },
                    })) => Ok(DataType::Char(1)),
                    Some(&Node::A_Const(A_Const {
                        val: Value { int: Some(len), .. },
                    })) => Ok(DataType::Char(len as u32)),
                    _ => unimplemented!(),
                }
            }
            Node::Value(Value { string, .. }) if string.as_deref() == Some("varchar") => {
                match mode.as_ref().map(|inner| &inner[0]) {
                    None => Ok(DataType::VarChar(None)),
                    Some(&Node::A_Const(A_Const {
                        val: Value { int: Some(len), .. },
                    })) => Ok(DataType::VarChar(Some(len as u32))),
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests;
