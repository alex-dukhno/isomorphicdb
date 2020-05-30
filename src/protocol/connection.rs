use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, BytesMut};
use futures::io::{self, AsyncReadExt, AsyncWriteExt};
use std::io::{Read, Write};

use crate::protocol::Command;
use crate::{
    protocol::messages::Message,
    protocol::{Error, Result, Stream},
};

#[derive(Debug)]
pub struct Connection<
    R: Read + Send + Sync + Unpin + 'static,
    W: Write + Send + Sync + Unpin + 'static,
> {
    reader: Stream<R>,
    writer: Stream<W>,
}

impl<R: Read + Send + Sync + Unpin + 'static, W: Write + Send + Sync + Unpin + 'static>
    Connection<R, W>
{
    pub fn new(reader: Stream<R>, writer: Stream<W>) -> Self {
        Self { reader, writer }
    }

    pub async fn send_ready_for_query(&mut self) -> io::Result<()> {
        trace!("send ready for query message");
        self.writer
            .write(Message::ReadyForQuery.as_vec().as_slice())
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
            .write(
                Message::RowDescription(
                    fields
                        .into_iter()
                        .map(|f| (f.name, f.type_id, f.type_size))
                        .collect(),
                )
                .as_vec()
                .as_slice(),
            )
            .await;
        trace!("row description is sent");
        Ok(())
    }

    pub async fn send_row_data(&mut self, rows: Vec<Vec<String>>) -> io::Result<()> {
        for row in rows {
            self.writer
                .write(Message::DataRow(row).as_vec().as_slice())
                .await?;
        }
        Ok(())
    }

    pub async fn send_command_complete(&mut self, message: Message) -> io::Result<()> {
        self.writer.write(message.as_vec().as_slice()).await?;
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
    use piper::{Arc, Mutex};
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

    #[async_std::test]
    async fn send_ready_for_query() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut connection = Connection::new(
            stream(empty_file().into_file()),
            stream(write_content.into_file()),
        );
        let ready_for_query = connection.send_ready_for_query().await?;
        assert_eq!(ready_for_query, ());
        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        assert_eq!(expected_content, content);
        Ok(())
    }

    #[cfg(test)]
    mod read_query {
        use super::*;

        #[async_std::test]
        async fn read_termination_command() -> io::Result<()> {
            let mut connection = Connection::new(
                stream(file_with(vec![&[88], &[0, 0, 0, 4]])),
                stream(empty_file().into_file()),
            );
            let query = connection.read_query().await?;
            assert_eq!(query, Ok(Command::Terminate));
            Ok(())
        }

        #[async_std::test]
        async fn read_query_successfully() -> io::Result<()> {
            let mut connection = Connection::new(
                stream(file_with(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"])),
                stream(empty_file().into_file()),
            );
            let query = connection.read_query().await?;
            assert_eq!(query, Ok(Command::Query("select 1;".to_owned())));
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_type_code_of_query_request() -> io::Result<()> {
            let mut connection =
                Connection::new(stream(file_with(vec![])), stream(empty_file().into_file()));
            let query = connection.read_query().await?;
            assert_eq!(query, Err(Error));
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_length_of_query() -> io::Result<()> {
            let mut connection = Connection::new(
                stream(file_with(vec![&[81]])),
                stream(empty_file().into_file()),
            );
            let query = connection.read_query().await?;
            assert_eq!(query, Err(Error));
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_query_string() -> io::Result<()> {
            let mut connection = Connection::new(
                stream(file_with(vec![&[81], &[0, 0, 0, 14], b"sel;\0"])),
                stream(empty_file().into_file()),
            );
            let query = connection.read_query().await?;
            assert_eq!(query, Err(Error));
            Ok(())
        }
    }

    #[async_std::test]
    async fn send_field_description_query() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut connection = Connection::new(
            stream(empty_file().into_file()),
            stream(write_content.into_file()),
        );
        let fields = vec![
            Field::new(
                "c1".to_owned(),
                23, // int4 type code
                4,
            ),
            Field::new("c2".to_owned(), 23, 4),
        ];
        connection.send_row_description(fields.clone()).await?;
        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
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
        assert_eq!(expected_content, content);
        Ok(())
    }

    #[async_std::test]
    async fn send_rows_data() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut connection = Connection::new(
            stream(empty_file().into_file()),
            stream(write_content.into_file()),
        );
        let rows = vec![
            vec!["1".to_owned(), "2".to_owned()],
            vec!["3".to_owned(), "4".to_owned()],
            vec!["5".to_owned(), "6".to_owned()],
        ];
        connection.send_row_data(rows.clone()).await?;
        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
        let mut expected_content = BytesMut::new();
        for row in rows {
            expected_content.extend_from_slice(Message::DataRow(row).as_vec().as_slice());
        }
        assert_eq!(expected_content, content);
        Ok(())
    }

    #[async_std::test]
    async fn send_command_complete() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut connection = Connection::new(
            stream(empty_file().into_file()),
            stream(write_content.into_file()),
        );
        connection
            .send_command_complete(Message::CommandComplete("SELECT".to_owned()))
            .await?;
        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(
            Message::CommandComplete("SELECT".to_owned())
                .as_vec()
                .as_slice(),
        );
        assert_eq!(expected_content, content);
        Ok(())
    }
}
