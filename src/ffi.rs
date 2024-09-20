use std::{
    ffi::{c_char, c_int, c_long, c_void, CStr, CString, NulError},
    num::TryFromIntError,
    ops::Deref,
    os::raw::c_double,
};

#[link(name = "sqlite3")]
extern "C" {
    fn sqlite3_open(filename: *const c_char, ppDb: *mut *mut c_void) -> c_int;
    fn sqlite3_close(db: *mut c_void) -> c_int;
    fn sqlite3_prepare_v2(
        db: *mut c_void,
        zSql: *const c_char,
        nByte: c_int,
        ppStmt: *mut *mut c_void,
        pzTail: *mut *const c_char,
    ) -> c_int;
    fn sqlite3_bind_text(
        stmt: *mut c_void,
        index: c_int,
        text: *const c_char,
        len: c_int,
        destructor: *const c_void,
    ) -> c_int;
    fn sqlite3_bind_int(stmt: *mut c_void, index: c_int, value: c_long) -> c_int;
    fn sqlite3_bind_double(stmt: *mut c_void, index: c_int, value: c_double) -> c_int;
    fn sqlite3_bind_blob(
        stmt: *mut c_void,
        index: c_int,
        value: *const c_void,
        len: c_int,
        destructor: *const c_void,
    ) -> c_int;
    fn sqlite3_bind_null(stmt: *mut c_void, index: c_int) -> c_int;
    fn sqlite3_step(stmt: *mut c_void) -> c_int;
    fn sqlite3_finalize(stmt: *mut c_void) -> c_int;
    fn sqlite3_errmsg(db: *mut c_void) -> *const c_char;
    fn sqlite3_column_count(stmt: *mut c_void) -> c_int;
    fn sqlite3_column_type(stmt: *mut c_void, iCol: c_int) -> c_int;
    fn sqlite3_column_name(stmt: *mut c_void, N: c_int) -> *const c_char;
    fn sqlite3_column_int64(stmt: *mut c_void, iCol: c_int) -> i64;
    fn sqlite3_column_double(stmt: *mut c_void, iCol: c_int) -> f64;
    fn sqlite3_column_text(stmt: *mut c_void, iCol: c_int) -> *const u8;
    fn sqlite3_column_bytes(stmt: *mut c_void, iCol: c_int) -> c_int;
    fn sqlite3_changes(db: *mut c_void) -> c_int;
}

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
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Sqlite {
    db: *mut c_void,
}

impl Sqlite {
    pub(crate) fn open(path: &str) -> Result<Self> {
        let c_path = CString::new(path)?;
        let mut db: *mut c_void = std::ptr::null_mut();

        unsafe {
            if sqlite3_open(c_path.as_ptr(), &mut db) != 0 {
                let error = CStr::from_ptr(sqlite3_errmsg(db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            }
        }

        Ok(Sqlite { db })
    }

    pub(crate) fn prepare(&self, sql: &str, params: &[Value]) -> Result<*mut c_void> {
        let c_sql = CString::new(sql)?;
        let mut stmt: *mut c_void = std::ptr::null_mut();
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
                                std::ptr::null(),
                            );
                        }
                        Value::Integer(n) => {
                            sqlite3_bind_int(stmt, (i + 1) as i32, *n);
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
                                std::ptr::null(),
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

    pub(crate) fn execute(&self, sql: &str, params: &[Value]) -> Result<i32> {
        unsafe {
            let stmt = self.prepare(sql, params)?;

            while sqlite3_step(stmt) != 101 {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            }

            sqlite3_finalize(stmt);

            let changes = sqlite3_changes(self.db);
            Ok(changes)
        }
    }

    pub(crate) fn query<T: FromRow>(&self, sql: &str, params: &[Value]) -> Result<Vec<T>> {
        unsafe {
            let stmt = self.prepare(sql, params)?;
            let mut rows = Vec::new();
            while sqlite3_step(stmt) == 100 {
                // SQLITE_ROW
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
            sqlite3_finalize(stmt);

            Ok(rows)
        }
    }

    pub(crate) fn rows(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<(String, Value)>>> {
        unsafe {
            let stmt = self.prepare(sql, params)?;
            let mut rows = Vec::new();
            while sqlite3_step(stmt) == 100 {
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
            sqlite3_finalize(stmt);

            Ok(rows)
        }
    }

    pub(crate) fn savepoint<'a>(
        &'a self,
        sqlite: &'a Sqlite,
        name: &'a str,
    ) -> Result<Savepoint<'a>> {
        Savepoint::new(sqlite, name)
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
    pub(crate) fn new(sqlite: &'a Sqlite, name: &'a str) -> Result<Self> {
        let sql = format!("savepoint {}", name);
        let _stmt = sqlite.prepare(&sql, &[])?;
        Ok(Self { sqlite, name })
    }

    pub(crate) fn release(&self) -> Result<()> {
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

pub enum Value {
    Text(String),
    Integer(i64),
    Real(f64),
    Blob(Vec<u8>),
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
