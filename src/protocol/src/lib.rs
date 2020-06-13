extern crate log;

pub mod listener;
pub mod messages;

pub type Version = i32;
pub type Params = Vec<(String, String)>;
pub type Result<T> = std::result::Result<T, Error>;

pub const VERSION_1: Version = 0x10000;
pub const VERSION_2: Version = 0x20000;
pub const VERSION_3: Version = 0x30000;
pub const VERSION_CANCEL: Version = (1234 << 16) + 5678;
pub const VERSION_SSL: Version = (1234 << 16) + 5679;
pub const VERSION_GSSENC: Version = (1234 << 16) + 5680;

pub fn supported_version() -> Version {
    VERSION_3
}

#[derive(Debug, PartialEq)]
pub struct Error;

#[derive(Debug, PartialEq)]
pub enum Command {
    Query(String),
    Terminate,
}
