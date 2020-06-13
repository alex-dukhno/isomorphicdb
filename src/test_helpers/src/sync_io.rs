use bytes::BytesMut;
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
};
use tempfile::NamedTempFile;

fn empty_file_named() -> NamedTempFile {
    NamedTempFile::new().expect("Failed to create tempfile")
}

fn empty_file() -> NamedTempFile {
    empty_file_named()
}

fn file_with(content: Vec<&[u8]>) -> NamedTempFile {
    let named_temp_file = empty_file_named();
    let mut file = named_temp_file.reopen().expect("file with content");
    for bytes in content {
        file.write_all(bytes).unwrap();
    }
    file.seek(SeekFrom::Start(0))
        .expect("set position at the beginning of a file");
    named_temp_file
}

#[derive(Debug)]
pub struct TestCase {
    request: File,
    response: File,
}

impl TestCase {
    pub fn empty() -> (Self, Self) {
        let response = empty_file();
        Self::couple(empty_file(), response)
    }

    pub fn with_content(content: Vec<&[u8]>) -> (Self, Self) {
        let temp_file = file_with(content);
        let response = empty_file();
        Self::couple(temp_file, response)
    }

    fn couple(request: NamedTempFile, response: NamedTempFile) -> (Self, Self) {
        (
            TestCase::single(
                request.reopen().expect("open file"),
                response.reopen().expect("open file"),
            ),
            TestCase::single(
                request.reopen().expect("open file"),
                response.reopen().expect("open file"),
            ),
        )
    }

    fn single(request: File, response: File) -> Self {
        TestCase { request, response }
    }

    pub fn read_result(&mut self) -> BytesMut {
        let mut result = Vec::new();
        self.response.seek(SeekFrom::Start(0)).unwrap();
        self.response.read_to_end(&mut result).unwrap();

        BytesMut::from(result.as_slice())
    }
}

impl Read for TestCase {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.request.read(buf)
    }
}

impl Write for TestCase {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.response.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.response.flush()
    }
}
