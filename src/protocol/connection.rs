use async_std::io::prelude::*;
use async_std::io::{self};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::BytesMut;

use crate::protocol::Command;
use crate::{
    protocol::messages::Message,
    protocol::{Error, Result},
};

#[derive(Debug)]
pub struct Connection<
    R: Read + Send + Sync + Unpin + 'static,
    W: Write + Send + Sync + Unpin + 'static,
> {
    reader: R,
    writer: W,
}

impl<R: Read + Send + Sync + Unpin + 'static, W: Write + Send + Sync + Unpin + 'static>
    Connection<R, W>
{
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    pub async fn send_ready_for_query(&mut self) -> io::Result<()> {
        trace!("send ready for query message");
        self.writer
            .write_all(Message::ReadyForQuery.as_vec().as_slice())
            .await?;
        Ok(())
    }

    pub async fn read_query(&mut self) -> io::Result<Result<Command>> {
        let mut type_code_buff = [0u8; 1];
        match self.reader.read_exact(&mut type_code_buff).await {
            Ok(_) => {
                debug!("FOR TEST type code = {:?}", type_code_buff);
                trace!(
                    "type code = {:?}",
                    String::from_utf8(type_code_buff.to_vec())
                );
                if &type_code_buff == b"X" {
                    Ok(Ok(Command::Terminate))
                } else {
                    let mut len_buff = [0u8; 4];
                    match self.reader.read_exact(&mut len_buff).await {
                        Ok(_) => {
                            debug!("FOR TEST len = {:?}", len_buff);
                            let len = NetworkEndian::read_i32(&len_buff);
                            let mut sql_buff = BytesMut::with_capacity(len as usize);
                            sql_buff.extend(0..((len as u8) - 4));
                            match self.reader.read_exact(&mut sql_buff).await {
                                Ok(_) => {
                                    debug!("FOR TEST sql = {:?}", sql_buff);
                                    let sql =
                                        String::from_utf8(sql_buff[..sql_buff.len() - 1].to_vec())
                                            .unwrap();
                                    trace!("SQL = {}", sql);
                                    Ok(Ok(Command::Query(sql)))
                                }
                                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                                    trace!("Unexpected EOF {:?}", e);
                                    Ok(Err(Error))
                                }
                                Err(e) => {
                                    error!("{:?}", e);
                                    Err(e)
                                }
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            trace!("Unexpected EOF {:?}", e);
                            Ok(Err(Error))
                        }
                        Err(e) => {
                            error!("{:?}", e);
                            Err(e)
                        }
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                trace!("Unexpected EOF {:?}", e);
                Ok(Err(Error))
            }
            Err(e) => {
                error!("{:?}", e);
                Err(e)
            }
        }
    }

    pub async fn send_row_description(&mut self, fields: Vec<Field>) -> io::Result<()> {
        self.writer
            .write_all(
                Message::RowDescription(
                    fields
                        .into_iter()
                        .map(|f| (f.name, f.type_id, f.type_size))
                        .collect(),
                )
                .as_vec()
                .as_slice(),
            )
            .await?;
        trace!("row description is sent");
        Ok(())
    }

    pub async fn send_row_data(&mut self, rows: Vec<Vec<String>>) -> io::Result<()> {
        for row in rows {
            self.writer
                .write_all(Message::DataRow(row).as_vec().as_slice())
                .await?;
        }
        Ok(())
    }

    pub async fn send_command_complete(&mut self, message: Message) -> io::Result<()> {
        self.writer.write_all(message.as_vec().as_slice()).await?;
        trace!("end of the command is sent");
        Ok(())
    }
}

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub type_id: i32,
    pub type_size: i16,
}

impl Field {
    pub fn new(name: String, type_id: i32, type_size: i16) -> Self {
        Self {
            name,
            type_id,
            type_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn send_ready_for_query() -> io::Result<()> {
        let test_case = test_helpers::TestCase::empty().await;
        let mut connection = Connection::new(test_case.clone(), test_case.clone());

        let ready_for_query = connection.send_ready_for_query().await?;

        assert_eq!(ready_for_query, ());

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());

        assert_eq!(expected_content, actual_content);

        Ok(())
    }

    #[cfg(test)]
    mod read_query {
        use super::*;

        #[async_std::test]
        async fn read_termination_command() -> io::Result<()> {
            let test_case = test_helpers::TestCase::with_content(vec![&[88], &[0, 0, 0, 4]]).await;
            let mut connection = Connection::new(test_case.clone(), test_case.clone());

            let query = connection.read_query().await?;

            assert_eq!(query, Ok(Command::Terminate));

            Ok(())
        }

        #[async_std::test]
        async fn read_query_successfully() -> io::Result<()> {
            let test_case =
                test_helpers::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"])
                    .await;
            let mut connection = Connection::new(test_case.clone(), test_case.clone());

            let query = connection.read_query().await?;

            assert_eq!(query, Ok(Command::Query("select 1;".to_owned())));

            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_type_code_of_query_request() -> io::Result<()> {
            let test_case = test_helpers::TestCase::with_content(vec![]).await;
            let mut connection = Connection::new(test_case.clone(), test_case.clone());

            let query = connection.read_query().await?;

            assert_eq!(query, Err(Error));

            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_length_of_query() -> io::Result<()> {
            let test_case = test_helpers::TestCase::with_content(vec![&[81]]).await;
            let mut connection = Connection::new(test_case.clone(), test_case.clone());

            let query = connection.read_query().await?;

            assert_eq!(query, Err(Error));

            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_query_string() -> io::Result<()> {
            let test_case =
                test_helpers::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"sel;\0"]).await;
            let mut connection = Connection::new(test_case.clone(), test_case.clone());

            let query = connection.read_query().await?;

            assert_eq!(query, Err(Error));

            Ok(())
        }
    }

    #[async_std::test]
    async fn send_field_description_query() -> io::Result<()> {
        let test_case = test_helpers::TestCase::empty().await;
        let mut connection = Connection::new(test_case.clone(), test_case.clone());
        let fields = vec![
            Field::new(
                "c1".to_owned(),
                23, // int4 type code
                4,
            ),
            Field::new("c2".to_owned(), 23, 4),
        ];

        connection.send_row_description(fields.clone()).await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(
            Message::RowDescription(
                fields
                    .into_iter()
                    .map(|f| (f.name, f.type_id, f.type_size))
                    .collect(),
            )
            .as_vec()
            .as_slice(),
        );

        assert_eq!(expected_content, actual_content);

        Ok(())
    }

    #[async_std::test]
    async fn send_rows_data() -> io::Result<()> {
        let test_case = test_helpers::TestCase::empty().await;
        let mut connection = Connection::new(test_case.clone(), test_case.clone());

        let rows = vec![
            vec!["1".to_owned(), "2".to_owned()],
            vec!["3".to_owned(), "4".to_owned()],
            vec!["5".to_owned(), "6".to_owned()],
        ];
        connection.send_row_data(rows.clone()).await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        for row in rows {
            expected_content.extend_from_slice(Message::DataRow(row).as_vec().as_slice());
        }

        assert_eq!(expected_content, actual_content);

        Ok(())
    }

    #[async_std::test]
    async fn send_command_complete() -> io::Result<()> {
        let test_case = test_helpers::TestCase::empty().await;
        let mut connection = Connection::new(test_case.clone(), test_case.clone());
        connection
            .send_command_complete(Message::CommandComplete("SELECT".to_owned()))
            .await?;

        let actual_content = test_case.read_result().await;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT".to_owned())
                .as_vec()
                .as_slice(),
        );
        assert_eq!(actual_content, expected_content);

        Ok(())
    }
}
