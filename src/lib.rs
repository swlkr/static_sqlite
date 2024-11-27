pub use ffi::{Error, FromRow, Result, Savepoint, Value};
pub use static_sqlite_macros::sql;
extern crate self as static_sqlite;

pub mod ffi;
pub mod sync;
pub mod tokio;

pub use sync::savepoint;
pub use tokio::*;

impl FromRow for () {
    fn from_row(_columns: Vec<(String, Value)>) -> Result<Self> {
        Ok(())
    }
}
