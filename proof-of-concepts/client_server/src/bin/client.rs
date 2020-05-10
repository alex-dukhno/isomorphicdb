#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::borrow::{Borrow, BorrowMut};
use std::io;
use std::io::{BufRead, Read, Write};
use std::net::TcpStream;
use std::string::FromUtf8Error;

const NETWORK_BUFFER_SIZE: usize = 1024;

fn main() -> io::Result<()> {
    pretty_env_logger::init();

    info!("Starting client");

    let mut stream = TcpStream::connect("127.0.0.1:7000")
        .expect("Server is not running. Please start server first") // TODO error handling
        ;
    let mut buffer = Buffer::new(stream);

    let mut command = String::new();

    let stdin = io::stdin();
    let mut handle = stdin.lock();

    loop {
        handle.read_line(&mut command)?; // TODO error handling
        debug!("typed command {:?}", command);

        if command.trim() == "exit" {
            break;
        }

        buffer.send(command.trim().as_bytes())?; // TODO error handling

        debug!("command send to server");

        buffer.wait()?;

        let code = buffer.server_code();
        debug!("{} server result code", code);

        match code {
            // SQL engine error
            u8::MAX => println!("{:?}", buffer.content_as_string()),
            // Table has been created
            1 => println!("{:?}", buffer.content_as_string()),
            // inserts, updates or deletes
            2 => println!("{:?}", buffer.content_as_string()),
            // select
            3 => {
                println!("{:?}", buffer.content_as_string());
                // for _ in 0..size {
                // let value: Result<Int, Box<ErrorKind>> = bincode::deserialize(&buffer[1..len]);

                // }
            }
            code => {
                println!("Unknown server code {:?}. Exiting!", code);
                break;
            }
        }
        command = String::new();
    }

    Ok(())
}

struct Buffer<S: Read + Write> {
    bytes: [u8; NETWORK_BUFFER_SIZE],
    length: usize,
    consumed: usize,
    source: S,
}

impl<S: Read + Write> Buffer<S> {
    pub fn new(source: S) -> Self {
        Self {
            bytes: [0 as u8; NETWORK_BUFFER_SIZE],
            length: 0,
            consumed: 0,
            source,
        }
    }

    pub fn send(&mut self, data: &[u8]) -> io::Result<()> {
        self.source.write_all(data)?;
        self.source.flush()
    }

    pub fn wait(&mut self) -> io::Result<()> {
        let result = self.source.read(self.bytes.borrow_mut()).map(|len| {
            self.length += len;
        });
        trace!(
            "Received from server {:?}",
            self.bytes[self.consumed..self.length].borrow()
        );
        result
    }

    pub fn server_code(&mut self) -> u8 {
        if self.length == self.consumed {
            self.wait();
        }
        let code = self.bytes[0];
        self.consumed += 1;
        if self.length == self.consumed {
            self.length = 0;
            self.consumed = 0;
        }
        code
    }

    pub fn content_as_string(&mut self) -> Result<String, FromUtf8Error> {
        if self.length == self.consumed {
            self.wait();
        }
        let str = String::from_utf8(self.bytes[self.consumed..self.length].to_vec());
        self.length = 0;
        self.consumed = 0;
        str
    }
}
