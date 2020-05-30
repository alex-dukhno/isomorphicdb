use crate::protocol::connection::{Connection, Field};
use crate::storage;
use futures::io;
use std::io::{Read, Write};

use crate::protocol::messages::Message;
use crate::protocol::Command;
use piper::{Arc, Mutex};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::Deref;

pub struct Handler<
    R: Read + Send + Sync + Unpin + 'static,
    W: Write + Send + Sync + Unpin + 'static,
    S: storage::Storage,
> {
    storage: Arc<Mutex<S>>,
    connection: Connection<R, W>,
}

impl<
        R: Read + Send + Sync + Unpin + 'static,
        W: Write + Send + Sync + Unpin + 'static,
        S: storage::Storage,
    > Handler<R, W, S>
{
    pub fn new(storage: Arc<Mutex<S>>, connection: Connection<R, W>) -> Self {
        Self {
            storage,
            connection,
        }
    }

    pub async fn handle_query(&mut self) -> io::Result<bool> {
        self.connection.send_ready_for_query().await?;
        match self.connection.read_query().await? {
            Err(e) => unimplemented!(),
            Ok(Command::Terminate) => return Ok(false),
            Ok(Command::Query(query)) => {
                match self.execute(query) {
                    Ok(QueryResult::SchemaCreated) => {
                        self.connection
                            .send_command_complete(Message::CommandComplete(
                                "CREATE SCHEMA".to_owned(),
                            ))
                            .await?;
                    }
                    Ok(QueryResult::SchemaDropped) => {
                        self.connection
                            .send_command_complete(Message::CommandComplete(
                                "DROP SCHEMA".to_owned(),
                            ))
                            .await?;
                    }
                    Ok(QueryResult::TableCreated) => {
                        self.connection
                            .send_command_complete(Message::CommandComplete(
                                "CREATE TABLE".to_owned(),
                            ))
                            .await?;
                    }
                    Ok(QueryResult::TableDropped) => {
                        self.connection
                            .send_command_complete(Message::CommandComplete(
                                "DROP TABLE".to_owned(),
                            ))
                            .await?;
                    }
                    Ok(QueryResult::RecordInserted) => {
                        self.connection
                            .send_command_complete(Message::CommandComplete(
                                "INSERT 0 1".to_owned(),
                            ))
                            .await?;
                    }
                    Ok(QueryResult::Select(records)) => {
                        let len = records.len();
                        self.connection
                            .send_row_description(vec![Field::new(
                                "column_test".to_owned(),
                                21, // int2 type code
                                2,
                            )])
                            .await?;
                        for record in records {
                            self.connection.send_row_data(vec![vec![record]]).await?;
                        }
                        self.connection
                            .send_command_complete(Message::CommandComplete(format!(
                                "SELECT {}",
                                len
                            )))
                            .await?;
                    }
                    Ok(QueryResult::Update(records_number)) => {
                        self.connection
                            .send_command_complete(Message::CommandComplete(format!(
                                "UPDATE {}",
                                records_number
                            )))
                            .await?;
                    }
                    Ok(QueryResult::Delete(records_number)) => {
                        self.connection
                            .send_command_complete(Message::CommandComplete(format!(
                                "DELETE {}",
                                records_number
                            )))
                            .await?;
                    }
                    Err(storage::Error::SchemaAlreadyExists(schema_name)) => {
                        self.connection
                            .send_command_complete(Message::ErrorResponse(
                                Some("ERROR".to_owned()),
                                Some("42P06".to_owned()),
                                Some(format!("schema \"{}\" already exists", schema_name)),
                            ))
                            .await?
                    }
                    Err(storage::Error::TableAlreadyExists(table_name)) => {
                        self.connection
                            .send_command_complete(Message::ErrorResponse(
                                Some("ERROR".to_owned()),
                                Some("42P07".to_owned()),
                                Some(format!("table \"{}\" already exists", table_name)),
                            ))
                            .await?
                    }
                    Err(e) => unimplemented!(),
                }
            }
        }
        Ok(true)
    }

    fn execute(&mut self, query: String) -> Result<QueryResult, storage::Error> {
        let statement = Parser::parse_sql(&PostgreSqlDialect {}, query)
            .unwrap()
            .pop()
            .unwrap();
        debug!("STATEMENT = {:?}", statement);
        match statement {
            sqlparser::ast::Statement::CreateTable { mut name, .. } => {
                let table_name = name.0.pop().unwrap().to_string();
                let schema_name = name.0.pop().unwrap().to_string();
                match self.storage.lock().create_table(schema_name, table_name) {
                    Ok(_) => Ok(QueryResult::TableCreated),
                    Err(e) => Err(e),
                }
            }
            sqlparser::ast::Statement::CreateSchema { schema_name, .. } => {
                match self.storage.lock().create_schema(schema_name.to_string()) {
                    Ok(_) => Ok(QueryResult::SchemaCreated),
                    Err(e) => Err(e),
                }
            }
            sqlparser::ast::Statement::Drop {
                object_type, names, ..
            } => match object_type {
                sqlparser::ast::ObjectType::Table => {
                    let table_name = names[0].0[1].to_string();
                    let schema_name = names[0].0[0].to_string();
                    match self.storage.lock().drop_table(schema_name, table_name) {
                        Ok(_) => Ok(QueryResult::TableDropped),
                        Err(e) => unimplemented!(),
                    }
                }
                sqlparser::ast::ObjectType::Schema => {
                    let schema_name = names[0].0[0].to_string();
                    match self.storage.lock().drop_schema(schema_name) {
                        Ok(_) => Ok(QueryResult::SchemaDropped),
                        Err(e) => unimplemented!(),
                    }
                }
                _ => unimplemented!(),
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
                    if let sqlparser::ast::Expr::Value(value) = &values[0][0] {
                        if let sqlparser::ast::Value::Number(value) = value {
                            match self.storage.lock().insert_into(
                                schema_name,
                                name,
                                value.to_string(),
                            ) {
                                Ok(_) => Ok(QueryResult::RecordInserted),
                                Err(_) => unimplemented!(),
                            }
                        } else {
                            unimplemented!()
                        }
                    } else {
                        unimplemented!()
                    }
                } else {
                    unimplemented!()
                }
            }
            sqlparser::ast::Statement::Query(query) => {
                let sqlparser::ast::Query { body, .. } = &*query;
                if let sqlparser::ast::SetExpr::Select(select) = &body {
                    let sqlparser::ast::Select { from, .. } = select.deref();

                    let sqlparser::ast::TableWithJoins { relation, .. } = &from[0];
                    let (schema_name, table_name) = match relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => {
                            let table_name = name.0[1].to_string();
                            let schema_name = name.0[0].to_string();
                            (schema_name, table_name)
                        }
                        _ => unimplemented!(),
                    };
                    match self.storage.lock().select_all_from(schema_name, table_name) {
                        Ok(records) => Ok(QueryResult::Select(records)),
                        _ => unreachable!(),
                    }
                } else {
                    unimplemented!()
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
                        match self.storage.lock().update_all(
                            schema_name,
                            table_name,
                            value.to_string(),
                        ) {
                            Ok(records_number) => Ok(QueryResult::Update(records_number)),
                            Err(_) => unimplemented!(),
                        }
                    } else {
                        unimplemented!()
                    }
                } else {
                    unimplemented!()
                }
            }
            sqlparser::ast::Statement::Delete { table_name, .. } => {
                let schema_name = table_name.0[0].to_string();
                let table_name = table_name.0[1].to_string();
                match self.storage.lock().delete_all_from(schema_name, table_name) {
                    Ok(records_number) => Ok(QueryResult::Delete(records_number)),
                    Err(_) => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}

enum QueryResult {
    SchemaCreated,
    SchemaDropped,
    TableCreated,
    TableDropped,
    RecordInserted,
    Select(Vec<String>),
    Update(usize),
    Delete(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::Message;
    use crate::protocol::Stream;
    use bytes::BytesMut;
    use smol::Async;
    use std::fs::File;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::NamedTempFile;

    fn empty_file() -> NamedTempFile {
        NamedTempFile::new().expect("Failed to create tempfile")
    }

    fn file_with(content: Vec<&[u8]>) -> File {
        let mut file = empty_file();
        for bytes in content {
            file.write(bytes);
        }
        file.seek(SeekFrom::Start(0))
            .expect("set position at the beginning of a file");
        file.into_file()
    }

    fn stream(file: File) -> Stream<File> {
        Arc::new(Mutex::new(
            Async::new(file).expect("Failed to create asynchronous stream"),
        ))
    }

    fn storage(
        create_schemas_responses: Vec<storage::Result<()>>,
        create_table_responses: Vec<storage::Result<()>>,
        select_results: Vec<storage::Result<Vec<String>>>,
    ) -> Arc<Mutex<MockStorage>> {
        Arc::new(Mutex::new(MockStorage {
            create_schemas_responses,
            create_table_responses,
            select_results,
        }))
    }

    #[async_std::test]
    async fn create_schema_query() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(vec![Ok(())], vec![], vec![]),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        expected_content.extend_from_slice(
            Message::CommandComplete("CREATE SCHEMA".to_owned())
                .as_vec()
                .as_slice(),
        );
        assert_eq!(expected_content, content);

        Ok(())
    }

    #[async_std::test]
    async fn create_schema_with_the_same_name() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(
                vec![
                    Err(storage::Error::SchemaAlreadyExists(
                        "schema_name".to_owned(),
                    )),
                    Ok(()),
                ],
                vec![],
                vec![],
            ),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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
        assert_eq!(expected_content, content);

        Ok(())
    }

    #[async_std::test]
    async fn drop_schema() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(vec![Ok(()), Ok(())], vec![], vec![]),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 29],
                    b"drop schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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
        assert_eq!(expected_content, content);

        Ok(())
    }

    #[async_std::test]
    async fn create_table() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(vec![Ok(())], vec![Ok(())], vec![]),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 64],
                    b"create table schema_name.table_name (column_name smallint);\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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
        assert_eq!(expected_content, content);

        Ok(())
    }

    #[async_std::test]
    async fn drop_table() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(vec![Ok(())], vec![Ok(()), Ok(())], vec![]),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 64],
                    b"create table schema_name.table_name (column_name smallint);\0",
                    &[81],
                    &[0, 0, 0, 39],
                    b"drop table schema_name.table_name;\0",
                    &[81],
                    &[0, 0, 0, 64],
                    b"create table schema_name.table_name (column_name smallint);\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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
        assert_eq!(expected_content, content);

        Ok(())
    }

    #[async_std::test]
    async fn insert_and_select_single_row() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(vec![Ok(())], vec![Ok(())], vec![Ok(vec!["123".to_owned()])]),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 64],
                    b"create table schema_name.table_name (column_name smallint);\0",
                    &[81],
                    &[0, 0, 0, 53],
                    b"insert into schema_name.table_name values (123);\0",
                    &[81],
                    &[0, 0, 0, 42],
                    b"select * from schema_name.table_name;\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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
        assert_eq!(expected_content.to_vec(), content);

        Ok(())
    }

    #[async_std::test]
    async fn insert_and_select_multiple_rows() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(
                vec![Ok(())],
                vec![Ok(())],
                vec![
                    Ok(vec!["123".to_owned(), "456".to_owned()]),
                    Ok(vec!["123".to_owned()]),
                ],
            ),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 64],
                    b"create table schema_name.table_name (column_name smallint);\0",
                    &[81],
                    &[0, 0, 0, 53],
                    b"insert into schema_name.table_name values (123);\0",
                    &[81],
                    &[0, 0, 0, 42],
                    b"select * from schema_name.table_name;\0",
                    &[81],
                    &[0, 0, 0, 53],
                    b"insert into schema_name.table_name values (456);\0",
                    &[81],
                    &[0, 0, 0, 42],
                    b"select * from schema_name.table_name;\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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
        let mut content_buff = BytesMut::new();
        content_buff.extend_from_slice(&content);
        assert_eq!(expected_content, content_buff);

        Ok(())
    }

    #[async_std::test]
    async fn update_all_records() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(
                vec![Ok(())],
                vec![Ok(())],
                vec![
                    Ok(vec!["789".to_owned(), "789".to_owned()]),
                    Ok(vec!["123".to_owned(), "456".to_owned()]),
                ],
            ),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 64],
                    b"create table schema_name.table_name (column_name smallint);\0",
                    &[81],
                    &[0, 0, 0, 53],
                    b"insert into schema_name.table_name values (123);\0",
                    &[81],
                    &[0, 0, 0, 53],
                    b"insert into schema_name.table_name values (456);\0",
                    &[81],
                    &[0, 0, 0, 42],
                    b"select * from schema_name.table_name;\0",
                    &[81],
                    &[0, 0, 0, 55],
                    b"update schema_name.table_name set column_test=789;\0",
                    &[81],
                    &[0, 0, 0, 42],
                    b"select * from schema_name.table_name;\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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

        let mut content_buff = BytesMut::new();
        content_buff.extend_from_slice(&content);
        assert_eq!(expected_content, content_buff);

        Ok(())
    }

    #[async_std::test]
    async fn delete_all_records() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut handler = Handler::new(
            storage(
                vec![Ok(())],
                vec![Ok(())],
                vec![Ok(vec![]), Ok(vec!["123".to_owned(), "456".to_owned()])],
            ),
            Connection::new(
                stream(file_with(vec![
                    &[81],
                    &[0, 0, 0, 31],
                    b"create schema schema_name;\0",
                    &[81],
                    &[0, 0, 0, 64],
                    b"create table schema_name.table_name (column_name smallint);\0",
                    &[81],
                    &[0, 0, 0, 53],
                    b"insert into schema_name.table_name values (123);\0",
                    &[81],
                    &[0, 0, 0, 53],
                    b"insert into schema_name.table_name values (456);\0",
                    &[81],
                    &[0, 0, 0, 42],
                    b"select * from schema_name.table_name;\0",
                    &[81],
                    &[0, 0, 0, 40],
                    b"delete from schema_name.table_name;\0",
                    &[81],
                    &[0, 0, 0, 42],
                    b"select * from schema_name.table_name;\0",
                ])),
                stream(write_content.into_file()),
            ),
        );

        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;
        handler.handle_query().await?;

        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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

        let mut content_buff = BytesMut::new();
        content_buff.extend_from_slice(&content);
        assert_eq!(expected_content, content_buff);

        Ok(())
    }

    struct MockStorage {
        create_schemas_responses: Vec<storage::Result<()>>,
        create_table_responses: Vec<storage::Result<()>>,
        select_results: Vec<storage::Result<Vec<String>>>,
    }

    impl storage::Storage for MockStorage {
        fn create_schema(&mut self, schema_name: String) -> storage::Result<()> {
            self.create_schemas_responses.pop().unwrap()
        }

        fn drop_schema(&mut self, schema_name: String) -> storage::Result<()> {
            Ok(())
        }

        fn create_table(&mut self, schema_name: String, table_name: String) -> storage::Result<()> {
            self.create_table_responses.pop().unwrap()
        }

        fn drop_table(&mut self, schema_name: String, table_name: String) -> storage::Result<()> {
            Ok(())
        }

        fn insert_into(
            &mut self,
            schema_name: String,
            table_name: String,
            value: String,
        ) -> storage::Result<()> {
            Ok(())
        }

        fn select_all_from(
            &mut self,
            schema_name: String,
            table_name: String,
        ) -> storage::Result<Vec<String>> {
            self.select_results.pop().unwrap()
        }

        fn update_all(
            &mut self,
            schema_name: String,
            table_name: String,
            value: String,
        ) -> storage::Result<usize> {
            Ok(2)
        }

        fn delete_all_from(
            &mut self,
            schema_name: String,
            table_name: String,
        ) -> storage::Result<usize> {
            Ok(2)
        }
    }
}
