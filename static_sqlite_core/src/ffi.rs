use static_sqlite_ffi::{
    sqlite3, sqlite3_bind_blob, sqlite3_bind_double, sqlite3_bind_int64, sqlite3_bind_null,
    sqlite3_bind_parameter_count, sqlite3_bind_parameter_name, sqlite3_bind_text, sqlite3_changes,
    sqlite3_close, sqlite3_column_bytes, sqlite3_column_count, sqlite3_column_double,
    sqlite3_column_int64, sqlite3_column_name, sqlite3_column_origin_name,
    sqlite3_column_table_name, sqlite3_column_text, sqlite3_column_type, sqlite3_errmsg,
    sqlite3_finalize, sqlite3_open, sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt,
};

use std::{
    ffi::{c_char, c_int, CStr, CString, NulError},
    num::TryFromIntError,
    ops::Deref,
    str::Utf8Error,
};

const SQLITE_ROW: i32 = static_sqlite_ffi::SQLITE_ROW as i32;
const SQLITE_DONE: i32 = static_sqlite_ffi::SQLITE_DONE as i32;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("null error: {0}")]
    Null(#[from] NulError),
    #[error("cstring error: {0}")]
    TryFromInt(#[from] TryFromIntError),
    #[error("sqlite error: {0}")]
    Sqlite(String),
    #[error("UNIQUE constraint failed: {0}")]
    UniqueConstraint(String),
    #[error("sqlite file closed")]
    ConnectionClosed,
    #[error("sqlite row not found")]
    RowNotFound,
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Sqlite {
    db: *mut static_sqlite_ffi::sqlite3,
}

unsafe impl Sync for Sqlite {}
unsafe impl Send for Sqlite {}

impl Sqlite {
    pub fn open(path: &str) -> Result<Self> {
        let c_path = CString::new(path)?;
        let mut db: *mut sqlite3 = core::ptr::null_mut();

        unsafe {
            if sqlite3_open(c_path.as_ptr(), &mut db) != 0 {
                let error = CStr::from_ptr(static_sqlite_ffi::sqlite3_errmsg(db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            }
        }

        Ok(Sqlite { db })
    }

    pub fn prepare(&self, sql: &str, params: &[Value]) -> Result<*mut sqlite3_stmt> {
        let c_sql = CString::new(sql)?;
        let mut stmt: *mut sqlite3_stmt = core::ptr::null_mut();
        unsafe {
            if sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) != 0
            {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            } else {
                for (i, param) in params.iter().enumerate() {
                    match param {
                        Value::Text(s) => {
                            let s = s.as_str();
                            sqlite3_bind_text(
                                stmt,
                                (i + 1) as i32,
                                s.as_ptr() as *const _,
                                s.len() as c_int,
                                None,
                            );
                        }
                        Value::Integer(n) => {
                            sqlite3_bind_int64(stmt, (i + 1) as i32, *n);
                        }
                        Value::Real(f) => {
                            sqlite3_bind_double(stmt, (i + 1) as i32, *f);
                        }
                        Value::Blob(b) => {
                            sqlite3_bind_blob(
                                stmt,
                                (i + 1) as i32,
                                b.as_ptr() as *const _,
                                b.len() as c_int,
                                None,
                            );
                        }
                        Value::Null => {
                            sqlite3_bind_null(stmt, (i + 1) as i32);
                        }
                    }
                }

                return Ok(stmt);
            }
        }
    }

    pub fn execute(&self, sql: &str, params: Vec<Value>) -> Result<i32> {
        unsafe {
            let stmt = self.prepare(&sql, &params)?;

            loop {
                match sqlite3_step(stmt) {
                    SQLITE_ROW | SQLITE_DONE => {
                        break;
                    }
                    _ => {
                        let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                            .to_string_lossy()
                            .into_owned();
                        return Err(Error::Sqlite(error));
                    }
                }
            }

            if sqlite3_finalize(stmt) != 0 {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            }

            let changes = sqlite3_changes(self.db);
            Ok(changes)
        }
    }

    pub fn execute_all(&self, sql: &str) -> Result<i32> {
        self.execute(sql, vec![])
    }

    pub fn query<T: FromRow>(&self, sql: &'static str, params: &[Value]) -> Result<Vec<T>> {
        unsafe {
            let stmt = self.prepare(sql, params)?;
            let mut rows = Vec::new();
            while sqlite3_step(stmt) == SQLITE_ROW {
                let column_count = sqlite3_column_count(stmt);
                let mut values: Vec<(String, Value)> = vec![];

                for i in 0..column_count {
                    let name = CStr::from_ptr(sqlite3_column_name(stmt, i))
                        .to_string_lossy()
                        .into_owned();

                    let value = match sqlite3_column_type(stmt, i) {
                        1 => Value::Integer(sqlite3_column_int64(stmt, i)),
                        2 => Value::Real(sqlite3_column_double(stmt, i)),
                        3 => {
                            let text =
                                CStr::from_ptr(sqlite3_column_text(stmt, i) as *const c_char)
                                    .to_string_lossy()
                                    .into_owned();
                            Value::Text(text)
                        }
                        4 => {
                            let len = sqlite3_column_bytes(stmt, i) as usize;
                            let ptr = sqlite3_column_text(stmt, i);
                            let slice = std::slice::from_raw_parts(ptr, len);
                            Value::Blob(slice.to_vec())
                        }
                        _ => Value::Null,
                    };

                    values.push((name, value));
                }

                let row = T::from_row(values)?;
                rows.push(row);
            }

            if sqlite3_finalize(stmt) != 0 {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                if error.starts_with("UNIQUE constraint failed: ") {
                    return Err(Error::UniqueConstraint(
                        error.replace("UNIQUE constraint failed: ", ""),
                    ));
                } else {
                    return Err(Error::Sqlite(error));
                }
            }

            Ok(rows)
        }
    }

    pub fn rows(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<(String, Value)>>> {
        unsafe {
            let stmt = self.prepare(sql, params)?;
            let mut rows = Vec::new();
            while sqlite3_step(stmt) == SQLITE_ROW {
                let column_count = sqlite3_column_count(stmt);
                let mut values: Vec<(String, Value)> = vec![];

                for i in 0..column_count {
                    let name = CStr::from_ptr(sqlite3_column_name(stmt, i))
                        .to_string_lossy()
                        .into_owned();

                    let value = match sqlite3_column_type(stmt, i) {
                        1 => Value::Integer(sqlite3_column_int64(stmt, i)),
                        2 => Value::Real(sqlite3_column_double(stmt, i)),
                        3 => {
                            let text =
                                CStr::from_ptr(sqlite3_column_text(stmt, i) as *const c_char)
                                    .to_string_lossy()
                                    .into_owned();
                            Value::Text(text)
                        }
                        4 => {
                            let len = sqlite3_column_bytes(stmt, i) as usize;
                            let ptr = sqlite3_column_text(stmt, i);
                            let slice = std::slice::from_raw_parts(ptr, len);
                            Value::Blob(slice.to_vec())
                        }
                        _ => Value::Null,
                    };

                    values.push((name, value));
                }

                rows.push(values);
            }

            if sqlite3_finalize(stmt) != 0 {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                if error.starts_with("UNIQUE constraint failed: ") {
                    return Err(Error::UniqueConstraint(
                        error.replace("UNIQUE constraint failed: ", ""),
                    ));
                } else {
                    return Err(Error::Sqlite(error));
                }
            }

            Ok(rows)
        }
    }

    pub fn savepoint<'a>(&'a self, sqlite: &'a Sqlite, name: &'a str) -> Result<Savepoint<'a>> {
        Savepoint::new(sqlite, name)
    }

    pub fn column_names(&self, sql: &str) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        unsafe {
            let stmt = self.prepare(sql, &[])?;
            let count = sqlite3_column_count(stmt);
            for i in 0..count {
                let name_ptr = sqlite3_column_origin_name(stmt, i);

                if !name_ptr.is_null() {
                    let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
                    columns.push(name);
                }
            }
        }

        Ok(columns)
    }

    pub fn aliased_column_names(&self, sql: &str) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        unsafe {
            let stmt = self.prepare(sql, &[])?;
            let count = sqlite3_column_count(stmt);
            for i in 0..count {
                let name_ptr = sqlite3_column_name(stmt, i);

                if !name_ptr.is_null() {
                    let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
                    columns.push(name);
                }
            }
        }

        Ok(columns)
    }

    pub fn table_names(&self, sql: &str) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        unsafe {
            let stmt = self.prepare(sql, &[])?;
            let count = sqlite3_column_count(stmt);
            for i in 0..count {
                let name_ptr = sqlite3_column_table_name(stmt, i);

                if !name_ptr.is_null() {
                    let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
                    tables.push(name);
                }
            }
        }

        Ok(tables)
    }

    pub fn bind_param_names(&self, sql: &str) -> Result<Vec<String>> {
        let mut params = Vec::new();

        unsafe {
            let stmt = self.prepare(sql, &[])?;
            let param_count = sqlite3_bind_parameter_count(stmt);

            for i in 1..param_count + 1 {
                let name_ptr = sqlite3_bind_parameter_name(stmt, i);
                if !name_ptr.is_null() {
                    let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
                    params.push(name);
                }
            }
            self.finalize(stmt)?;
        }

        Ok(params)
    }

    fn finalize(&self, stmt: *mut sqlite3_stmt) -> Result<()> {
        unsafe {
            if sqlite3_finalize(stmt) != 0 {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            }
        }
        Ok(())
    }
}

impl Drop for Sqlite {
    fn drop(&mut self) {
        unsafe {
            sqlite3_close(self.db);
        }
    }
}

#[derive(Debug)]
pub struct Savepoint<'a> {
    sqlite: &'a Sqlite,
    name: &'a str,
}

impl<'a> Savepoint<'a> {
    pub fn new(sqlite: &'a Sqlite, name: &'a str) -> Result<Savepoint<'a>> {
        let sql = format!("savepoint {}", name);
        let _stmt = sqlite.prepare(&sql, &[])?;
        Ok(Self { sqlite, name })
    }

    pub fn release(&self) -> Result<()> {
        let sql = format!("release savepoint {}", self.name);
        let _stmt = self.prepare(&sql, &[])?;
        Ok(())
    }
}

impl<'a> Deref for Savepoint<'a> {
    type Target = Sqlite;

    fn deref(&self) -> &Self::Target {
        self.sqlite
    }
}

impl<'a> Drop for Savepoint<'a> {
    fn drop(&mut self) {
        self.release().expect("release savepoint failed")
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Text(String),
    Integer(i64),
    Real(f64),
    Blob(Vec<u8>),
    Null,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Text,
    Integer,
    Real,
    Blob,
    Null,
}

pub trait FromRow: Sized {
    fn from_row(columns: Vec<(String, Value)>) -> Result<Self>;
}

impl TryFrom<Value> for String {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Text(s) => Ok(s),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Integer(val) => Ok(val),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Real(val) => Ok(val),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Blob(val) => Ok(val),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}

impl TryFrom<Value> for Option<String> {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Text(s) => Ok(Some(s)),
            Value::Null => Ok(None),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}
impl TryFrom<Value> for Option<i64> {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Integer(val) => Ok(Some(val)),
            Value::Null => Ok(None),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}
impl TryFrom<Value> for Option<f64> {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Real(val) => Ok(Some(val)),
            Value::Null => Ok(None),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}
impl TryFrom<Value> for Option<Vec<u8>> {
    type Error = Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Blob(val) => Ok(Some(val)),
            Value::Null => Ok(None),
            _ => Err(Error::Sqlite("column type mismatch".into())),
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Text(value.into())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value)
    }
}

impl From<Option<&str>> for Value {
    fn from(value: Option<&str>) -> Self {
        match value {
            Some(val) => Value::Text(val.into()),
            None => Value::Null,
        }
    }
}

impl From<Option<String>> for Value {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(val) => Value::Text(val),
            None => Value::Null,
        }
    }
}

impl From<Option<i64>> for Value {
    fn from(value: Option<i64>) -> Self {
        match value {
            Some(val) => Value::Integer(val),
            None => Value::Null,
        }
    }
}
impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Integer(value)
    }
}

impl From<Option<f64>> for Value {
    fn from(value: Option<f64>) -> Self {
        match value {
            Some(val) => Value::Real(val),
            None => Value::Null,
        }
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Real(value)
    }
}

impl From<Option<Vec<u8>>> for Value {
    fn from(value: Option<Vec<u8>>) -> Self {
        match value {
            Some(val) => Value::Blob(val),
            None => Value::Null,
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Blob(value)
    }
}

impl From<()> for Value {
    fn from(_value: ()) -> Self {
        Value::Null
    }
}
