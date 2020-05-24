use async_std::fs::File;
use async_std::io::prelude::*;
use async_std::io::{self, SeekFrom};
use async_std::sync::Arc;
use async_std::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Mutex;

#[cfg(test)]
mod sql;

/// An async testcase that can be written to and read from.
#[derive(Clone, Debug)]
pub struct TestCase {
    reader: Arc<Mutex<File>>,
    writer: Arc<Mutex<File>>,
}

impl TestCase {
    /// Create a new instance.
    pub async fn new(reader: &[u8], writer: &[u8]) -> TestCase {
        use std::io::Write;

        let mut temp = tempfile::tempfile().expect("Failed writer create tempfile");
        temp.write(reader)
            .expect("Could not write writer dest file");
        let mut file: File = temp.into();
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let reader = Arc::new(Mutex::new(file.into()));

        let mut temp = tempfile::tempfile().expect("Failed writer create tempfile");
        temp.write(writer)
            .expect("Could not write writer dest file");
        let mut file: File = temp.into();
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let writer = Arc::new(Mutex::new(file.into()));

        TestCase { reader, writer }
    }
}

impl Read for TestCase {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.reader.lock().unwrap()).poll_read(cx, buf)
    }
}

impl Write for TestCase {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.writer.lock().unwrap()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.writer.lock().unwrap()).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.writer.lock().unwrap()).poll_close(cx)
    }
}
