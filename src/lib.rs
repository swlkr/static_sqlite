pub use ffi::{Sqlite, Value};
pub use static_sqlite_macros::sql;
use std::ffi::NulError;
extern crate self as static_sqlite;
mod ffi;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("null error: {0}")]
    Null(#[from] NulError),
    #[error("sqlite error: {0}")]
    Sqlite(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() -> Result<()> {
        let conn = Sqlite::open(":memory:")?;

        let sql =
            "create table tests (id integer primary key, txt text, inte integer, rl real, blb blob)";
        let _ = conn.execute(sql, &[])?;
        let sql = "insert into tests (txt, inte, rl, blb) values (?, ?, ?, ?)";
        let params = vec![
            Value::Text("txt".to_string()),
            Value::Integer(30),
            Value::Real(50000.50),
            Value::Blob(vec![0xFF, 0x00, 0xFF, 0x00]),
        ];

        conn.execute(sql, &params)?;

        Ok(())
    }
}
