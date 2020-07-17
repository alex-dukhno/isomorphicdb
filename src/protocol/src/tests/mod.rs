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

use crate::Channel;
use futures_util::io::{AsyncRead, AsyncWrite};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

mod async_io;
#[cfg(test)]
mod connection;
#[cfg(test)]
mod hand_shake;
#[cfg(test)]
mod pg_frontend;

struct MockChannel {
    socket: async_io::TestCase,
}

impl MockChannel {
    pub fn new(socket: async_io::TestCase) -> Self {
        Self { socket }
    }
}

impl Channel for MockChannel {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_read(cx, buf)
    }

    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_close(cx)
    }
}
