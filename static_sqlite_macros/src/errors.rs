#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    MissingColumn(String),
    MissingTable(String),
}

pub type Result<T> = core::result::Result<T, Error>;
