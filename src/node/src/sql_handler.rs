use async_std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
};
use core::{SystemError, SystemResult};
use protocol::{
    connection::{Connection, Field},
    messages::Message,
    Command,
};

use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use std::ops::Deref;
use storage::{
    persistent::PersistentStorage,
    relational::{Error, Projection, RelationalStorage},
};

type QueryResult = std::result::Result<QueryEvent, Error>;

pub struct Handler<
    R: Read + Send + Sync + Unpin + 'static,
    W: Write + Send + Sync + Unpin + 'static,
    P: PersistentStorage,
> {
    storage: Arc<Mutex<RelationalStorage<P>>>,
    connection: Connection<R, W>,
}

impl<
        R: Read + Send + Sync + Unpin + 'static,
        W: Write + Send + Sync + Unpin + 'static,
        P: PersistentStorage,
    > Handler<R, W, P>
{
    pub fn new(storage: Arc<Mutex<RelationalStorage<P>>>, connection: Connection<R, W>) -> Self {
        Self {
            storage,
            connection,
        }
    }

    #[allow(clippy::match_wild_err_arm)]
    pub async fn handle_query(&mut self) -> SystemResult<bool> {
        match self.connection.send_ready_for_query().await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => unimplemented!(),
            Err(error) => return Err(SystemError::io(error)),
        }
        match self.connection.read_query().await {
            Err(error) => return Err(SystemError::io(error)),
            Ok(Err(e)) => {
                error!("{:?}", e);
                return Ok(false);
            }
            Ok(Ok(Command::Terminate)) => return Ok(false),
            Ok(Ok(Command::Query(query))) => match self.execute(query.clone()).await? {
                Ok(QueryEvent::SchemaCreated) => {
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete("CREATE SCHEMA".to_owned()))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Ok(QueryEvent::SchemaDropped) => {
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete("DROP SCHEMA".to_owned()))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Ok(QueryEvent::TableCreated) => {
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete("CREATE TABLE".to_owned()))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Ok(QueryEvent::TableDropped) => {
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete("DROP TABLE".to_owned()))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Ok(QueryEvent::RecordsInserted(len)) => {
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete(format!(
                            "INSERT 0 {}",
                            len
                        )))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Ok(QueryEvent::RecordSelected(projection)) => {
                    let definition = projection.0;
                    let description = definition
                        .iter()
                        .map(|name| Field::new(name.clone(), 21, 2))
                        .collect();
                    let records = projection.1;
                    let len = records.len();
                    match self.connection.send_row_description(description).await {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                    for record in records {
                        match self.connection.send_row_data(record).await {
                            Ok(()) => {}
                            Err(error) => return Err(SystemError::io(error)),
                        }
                    }
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete(format!("SELECT {}", len)))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Ok(QueryEvent::RecordsUpdated(records_number)) => {
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete(format!(
                            "UPDATE {}",
                            records_number
                        )))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Ok(QueryEvent::RecordsDeleted(records_number)) => {
                    match self
                        .connection
                        .send_command_complete(Message::CommandComplete(format!(
                            "DELETE {}",
                            records_number
                        )))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Err(storage::relational::Error::SchemaAlreadyExists(schema_name)) => {
                    match self
                        .connection
                        .send_command_complete(Message::ErrorResponse(
                            Some("ERROR".to_owned()),
                            Some("42P06".to_owned()),
                            Some(format!("schema \"{}\" already exists", schema_name)),
                        ))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Err(storage::relational::Error::TableAlreadyExists(table_name)) => {
                    match self
                        .connection
                        .send_command_complete(Message::ErrorResponse(
                            Some("ERROR".to_owned()),
                            Some("42P07".to_owned()),
                            Some(format!("table \"{}\" already exists", table_name)),
                        ))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
                Err(storage::relational::Error::NotSupportedOperation(raw_sql_query)) => match self
                    .connection
                    .send_command_complete(Message::ErrorResponse(
                        Some("ERROR".to_owned()),
                        Some("42601".to_owned()),
                        Some(format!(
                            "Currently, Query '{}' can't be executed",
                            raw_sql_query
                        )),
                    ))
                    .await
                {
                    Ok(()) => {}
                    Err(error) => return Err(SystemError::io(error)),
                },
                Err(e) => {
                    match self
                        .connection
                        .send_command_complete(Message::ErrorResponse(
                            Some("ERROR".to_owned()),
                            Some("58000".to_owned()),
                            Some(format!(
                                "Unhandled error during executing query: '{}'\nThe error is: {:#?}",
                                query, e
                            )),
                        ))
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => return Err(SystemError::io(error)),
                    }
                }
            },
        }
        Ok(true)
    }

    #[allow(clippy::match_wild_err_arm)]
    async fn execute(&mut self, raw_sql_query: String) -> SystemResult<QueryResult> {
        let statement = Parser::parse_sql(&PostgreSqlDialect {}, raw_sql_query.as_str())
            .unwrap()
            .pop()
            .unwrap();
        eprintln!("STATEMENT = {:?}", statement);
        match statement {
            sqlparser::ast::Statement::CreateTable {
                mut name, columns, ..
            } => {
                let table_name = name.0.pop().unwrap().to_string();
                let schema_name = name.0.pop().unwrap().to_string();
                match (*self.storage.lock().await).create_table(
                    schema_name,
                    table_name,
                    columns.into_iter().map(|c| c.name.to_string()).collect(),
                )? {
                    Ok(_) => Ok(Ok(QueryEvent::TableCreated)),
                    Err(e) => Ok(Err(e)),
                }
            }
            sqlparser::ast::Statement::CreateSchema { schema_name, .. } => {
                match (*self.storage.lock().await).create_schema(schema_name.to_string())? {
                    Ok(_) => Ok(Ok(QueryEvent::SchemaCreated)),
                    Err(e) => Ok(Err(e)),
                }
            }
            sqlparser::ast::Statement::Drop {
                object_type, names, ..
            } => match object_type {
                sqlparser::ast::ObjectType::Table => {
                    let table_name = names[0].0[1].to_string();
                    let schema_name = names[0].0[0].to_string();
                    match (*self.storage.lock().await).drop_table(schema_name, table_name)? {
                        Ok(_) => Ok(Ok(QueryEvent::TableDropped)),
                        Err(_e) => Ok(Err(storage::relational::Error::NotSupportedOperation(
                            raw_sql_query,
                        ))),
                    }
                }
                sqlparser::ast::ObjectType::Schema => {
                    let schema_name = names[0].0[0].to_string();
                    match (*self.storage.lock().await).drop_schema(schema_name)? {
                        Ok(_) => Ok(Ok(QueryEvent::SchemaDropped)),
                        Err(_e) => Ok(Err(storage::relational::Error::NotSupportedOperation(
                            raw_sql_query,
                        ))),
                    }
                }
                _ => Ok(Err(storage::relational::Error::NotSupportedOperation(
                    raw_sql_query,
                ))),
            },
            sqlparser::ast::Statement::Insert {
                mut table_name,
                source,
                ..
            } => {
                let name = table_name.0.pop().unwrap().to_string();
                let schema_name = table_name.0.pop().unwrap().to_string();
                let sqlparser::ast::Query { body, .. } = &*source;
                if let sqlparser::ast::SetExpr::Values(values) = &body {
                    let values = &values.0;
                    let to_insert: Vec<Vec<String>> = values
                        .into_iter()
                        .map(|v| v.iter().map(|v| v.to_string()).collect())
                        .collect();
                    let len = to_insert.len();
                    match (*self.storage.lock().await).insert_into(schema_name, name, to_insert)? {
                        Ok(_) => Ok(Ok(QueryEvent::RecordsInserted(len))),
                        Err(_) => Ok(Err(storage::relational::Error::NotSupportedOperation(
                            raw_sql_query,
                        ))),
                    }
                } else {
                    Ok(Err(storage::relational::Error::NotSupportedOperation(
                        raw_sql_query,
                    )))
                }
            }
            sqlparser::ast::Statement::Query(query) => {
                let sqlparser::ast::Query { body, .. } = *query;
                if let sqlparser::ast::SetExpr::Select(select) = body {
                    let sqlparser::ast::Select {
                        projection, from, ..
                    } = select.deref();
                    let sqlparser::ast::TableWithJoins { relation, .. } = &from[0];
                    let (schema_name, table_name) = match relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => {
                            let table_name = name.0[1].to_string();
                            let schema_name = name.0[0].to_string();
                            (schema_name, table_name)
                        }
                        _ => {
                            return Ok(Err(storage::relational::Error::NotSupportedOperation(
                                raw_sql_query,
                            )))
                        }
                    };
                    let table_columns = {
                        let projection = projection.clone();
                        let mut columns = vec![];
                        for item in projection {
                            match item {
                                sqlparser::ast::SelectItem::Wildcard => {
                                    match (*self.storage.lock().await)
                                        .table_columns(schema_name.clone(), table_name.clone())?
                                    {
                                        Ok(all_columns) => columns.extend(all_columns),
                                        Err(_e) => {
                                            return Ok(Err(
                                                storage::relational::Error::NotSupportedOperation(
                                                    raw_sql_query,
                                                ),
                                            ))
                                        }
                                    }
                                }
                                sqlparser::ast::SelectItem::UnnamedExpr(
                                    sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident {
                                        value,
                                        ..
                                    }),
                                ) => columns.push(value.clone()),
                                _ => {
                                    return Ok(Err(
                                        storage::relational::Error::NotSupportedOperation(
                                            raw_sql_query,
                                        ),
                                    ))
                                }
                            }
                        }
                        columns
                    };
                    match (*self.storage.lock().await).select_all_from(
                        schema_name,
                        table_name,
                        table_columns,
                    )? {
                        Ok(records) => Ok(Ok(QueryEvent::RecordSelected(records))),
                        _ => Ok(Err(storage::relational::Error::NotSupportedOperation(
                            raw_sql_query,
                        ))),
                    }
                } else {
                    Ok(Err(storage::relational::Error::NotSupportedOperation(
                        raw_sql_query,
                    )))
                }
            }
            sqlparser::ast::Statement::Update {
                table_name,
                assignments,
                ..
            } => {
                let schema_name = table_name.0[0].to_string();
                let table_name = table_name.0[1].to_string();
                let sqlparser::ast::Assignment { value, .. } = &assignments[0];
                if let sqlparser::ast::Expr::Value(value) = value {
                    if let sqlparser::ast::Value::Number(value) = value {
                        match (*self.storage.lock().await).update_all(
                            schema_name,
                            table_name,
                            value.to_string(),
                        )? {
                            Ok(records_number) => {
                                Ok(Ok(QueryEvent::RecordsUpdated(records_number)))
                            }
                            Err(_) => Ok(Err(storage::relational::Error::NotSupportedOperation(
                                raw_sql_query,
                            ))),
                        }
                    } else {
                        Ok(Err(storage::relational::Error::NotSupportedOperation(
                            raw_sql_query,
                        )))
                    }
                } else {
                    Ok(Err(storage::relational::Error::NotSupportedOperation(
                        raw_sql_query,
                    )))
                }
            }
            sqlparser::ast::Statement::Delete { table_name, .. } => {
                let schema_name = table_name.0[0].to_string();
                let table_name = table_name.0[1].to_string();
                match (*self.storage.lock().await).delete_all_from(schema_name, table_name)? {
                    Ok(records_number) => Ok(Ok(QueryEvent::RecordsDeleted(records_number))),
                    Err(_) => Ok(Err(storage::relational::Error::NotSupportedOperation(
                        raw_sql_query,
                    ))),
                }
            }
            _ => Ok(Err(storage::relational::Error::NotSupportedOperation(
                raw_sql_query,
            ))),
        }
    }
}

enum QueryEvent {
    SchemaCreated,
    SchemaDropped,
    TableCreated,
    TableDropped,
    RecordsInserted(usize),
    RecordSelected(Projection),
    RecordsUpdated(usize),
    RecordsDeleted(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::{channel::Channel, messages::Message, supported_version, Params, SslMode},
        storage::relational::RelationalStorage,
    };
    use bytes::BytesMut;
    use test_helpers::{async_io, frontend};

    #[async_std::test]
    async fn create_schema_query() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![frontend::Message::Query(
            "create schema schema_name;",
        )
        .as_vec()
        .as_slice()])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn create_schema_with_the_same_name() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42P06".to_owned()),
                Some("schema \"schema_name\" already exists".to_owned()),
            )
            .as_vec()
            .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn drop_schema() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("drop schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("DROP SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn create_table() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_name smallint);")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn drop_table() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_name smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("drop table schema_name.table_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_name smallint);")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("DROP TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn insert_and_select_single_row() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_test smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (123);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![("column_test".to_owned(), 21, 2)])
                .as_vec()
                .as_slice(),
        );
        expected_content
            .extend_from_slice(Message::DataRow(vec!["123".to_owned()]).as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 1".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn insert_and_select_multiple_rows() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_test smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (123);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (456);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![("column_test".to_owned(), 21, 2)])
                .as_vec()
                .as_slice(),
        );
        expected_content
            .extend_from_slice(Message::DataRow(vec!["123".to_owned()]).as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![("column_test".to_owned(), 21, 2)])
                .as_vec()
                .as_slice(),
        );
        expected_content
            .extend_from_slice(Message::DataRow(vec!["123".to_owned()]).as_vec().as_slice());
        expected_content
            .extend_from_slice(Message::DataRow(vec!["456".to_owned()]).as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 2".to_owned())
                .as_vec()
                .as_slice(),
        );
        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn update_all_records() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_test smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (123);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (456);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("update schema_name.table_name set column_test=789;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![("column_test".to_owned(), 21, 2)])
                .as_vec()
                .as_slice(),
        );
        expected_content
            .extend_from_slice(Message::DataRow(vec!["123".to_owned()]).as_vec().as_slice());
        expected_content
            .extend_from_slice(Message::DataRow(vec!["456".to_owned()]).as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 2".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("UPDATE 2".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![("column_test".to_owned(), 21, 2)])
                .as_vec()
                .as_slice(),
        );
        expected_content
            .extend_from_slice(Message::DataRow(vec!["789".to_owned()]).as_vec().as_slice());
        expected_content
            .extend_from_slice(Message::DataRow(vec!["789".to_owned()]).as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 2".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn delete_all_records() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_test smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (123);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (456);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("delete from schema_name.table_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;
        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![("column_test".to_owned(), 21, 2)])
                .as_vec()
                .as_slice(),
        );
        expected_content
            .extend_from_slice(Message::DataRow(vec!["123".to_owned()]).as_vec().as_slice());
        expected_content
            .extend_from_slice(Message::DataRow(vec!["456".to_owned()]).as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 2".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("DELETE 2".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![("column_test".to_owned(), 21, 2)])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 0".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn select_all_from_table_with_multiple_columns() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("insert into schema_name.table_name values (123, 456, 789);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;

        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 1".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![
                ("column_1".to_owned(), 21, 2),
                ("column_2".to_owned(), 21, 2),
                ("column_3".to_owned(), 21, 2),
            ])
            .as_vec()
            .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::DataRow(vec!["123".to_owned(), "456".to_owned(), "789".to_owned()])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 1".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn insert_multiple_rows() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query(
                "insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);",
            )
            .as_vec()
            .as_slice(),
            frontend::Message::Query("select * from schema_name.table_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;

        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 3".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![
                ("column_1".to_owned(), 21, 2),
                ("column_2".to_owned(), 21, 2),
                ("column_3".to_owned(), 21, 2),
            ])
            .as_vec()
            .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::DataRow(vec!["1".to_owned(), "4".to_owned(), "7".to_owned()])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::DataRow(vec!["2".to_owned(), "5".to_owned(), "8".to_owned()])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::DataRow(vec!["3".to_owned(), "6".to_owned(), "9".to_owned()])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 3".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn select_not_all_columns() -> SystemResult<()> {
        let test_case = async_io::TestCase::with_content(vec![
            frontend::Message::Query("create schema schema_name;")
                .as_vec()
                .as_slice(),
            frontend::Message::Query("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
                .as_vec()
                .as_slice(),
            frontend::Message::Query(
                "insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);",
            )
            .as_vec()
            .as_slice(),
            frontend::Message::Query("select column_3, column_2 from schema_name.table_name;")
                .as_vec()
                .as_slice(),
        ])
        .await;

        let mut handler = Handler::new(
            in_memory_storage(),
            Connection::new(
                (supported_version(), Params(vec![]), SslMode::Disable),
                Channel::new(test_case.clone(), test_case.clone()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE TABLE".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("INSERT 0 3".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::RowDescription(vec![
                ("column_3".to_owned(), 21, 2),
                ("column_2".to_owned(), 21, 2),
            ])
            .as_vec()
            .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::DataRow(vec!["7".to_owned(), "4".to_owned()])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::DataRow(vec!["8".to_owned(), "5".to_owned()])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::DataRow(vec!["9".to_owned(), "6".to_owned()])
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT 3".to_owned())
                .as_vec()
                .as_slice(),
        );

        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    use crate::storage::in_memory_for_tests_only::InMemoryStorage;

    fn in_memory_storage() -> Arc<Mutex<RelationalStorage<InMemoryStorage>>> {
        Arc::new(Mutex::new(
            RelationalStorage::new(InMemoryStorage::default()).unwrap(),
        ))
    }
}
