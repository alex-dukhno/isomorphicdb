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

use async_dup::Arc;
use async_mutex::Mutex;
use blocking::Unblock;
use futures_lite::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite};
use std::{
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    pin::Pin,
    task::{Context, Poll},
};
use tempfile::NamedTempFile;

pub(crate) fn empty_file_named() -> NamedTempFile {
    NamedTempFile::new().expect("Failed to create tempfile")
}

fn file_with(content: Vec<&[u8]>) -> (File, NamedTempFile) {
    let named_temp_file = empty_file_named();
    let mut file = named_temp_file.reopen().expect("file with content");
    for bytes in content {
        file.write_all(bytes).unwrap();
    }
    file.seek(SeekFrom::Start(0))
        .expect("set position at the beginning of a file");
    (named_temp_file.reopen().expect("reopen file").into(), named_temp_file)
}

#[derive(Debug)]
pub struct TestCase {
    request: Mutex<Unblock<File>>,
    response: Mutex<Unblock<File>>,
    request_path: Arc<NamedTempFile>,
    response_path: Arc<NamedTempFile>,
}

impl Clone for TestCase {
    fn clone(&self) -> Self {
        TestCase {
            request: Mutex::new(Unblock::new(self.request_path.reopen().expect("reopen file").into())),
            response: Mutex::new(Unblock::new(self.response_path.reopen().expect("reopen file").into())),
            request_path: self.request_path.clone(),
            response_path: self.response_path.clone(),
        }
    }
}

impl TestCase {
    pub fn with_content(content: Vec<&[u8]>) -> Self {
        Self::new(file_with(content))
    }

    pub fn new(req: (File, NamedTempFile)) -> TestCase {
        let (request, request_path) = req;
        let temp = NamedTempFile::new().expect("Failed to create tempfile");
        let response = temp.reopen().expect("file with content");
        let result = Mutex::new(Unblock::new(response));

        TestCase {
            request: Mutex::new(Unblock::new(request)),
            response: result,
            request_path: Arc::new(request_path),
            response_path: Arc::new(temp),
        }
    }

    pub async fn read_result(&self) -> Vec<u8> {
        let mut result = Vec::new();
        let file = &mut *(self.response.lock()).await;
        file.seek(SeekFrom::Start(0))
            .await
            .expect("start at the beginning of the file");
        file.read_to_end(&mut result).await.expect("read all content");

        result
    }
}

impl AsyncRead for TestCase {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        Pin::new((&mut self.get_mut().request).get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for TestCase {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new((&mut self.get_mut().response).get_mut()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new((&mut self.get_mut().response).get_mut()).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new((&mut self.get_mut().response).get_mut()).poll_close(cx)
    }
}
