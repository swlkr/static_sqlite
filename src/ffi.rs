use std::{
    ffi::{c_char, c_int, c_void, CStr, CString, NulError},
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
    fn sqlite3_bind_int(stmt: *mut c_void, index: c_int, value: c_int) -> c_int;
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

    pub(crate) fn prepare(&self, sql: &str) -> Result<*mut c_void> {
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
                return Ok(stmt);
            }
        }
    }

    pub(crate) fn execute(&self, sql: &str, params: &[Value]) -> Result<()> {
        unsafe {
            let stmt = self.prepare(sql)?;

            for (i, param) in params.iter().enumerate() {
                match param {
                    Value::Text(s) => {
                        let s = s.as_str();
                        // let c_str = CString::new(&s)?;
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

            if sqlite3_step(stmt) != 101 {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            }

            sqlite3_finalize(stmt);
        }

        Ok(())
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
        let _stmt = sqlite.prepare(&sql)?;
        Ok(Self { sqlite, name })
    }

    pub(crate) fn release(&self) -> Result<()> {
        let sql = format!("release savepoint {}", self.name);
        let _stmt = self.prepare(&sql)?;
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
    Integer(i32),
    Real(f64),
    Blob(Vec<u8>),
    Null,
}

pub fn text(val: impl ToString) -> Value {
    Value::Text(val.to_string())
}

pub fn integer(val: i32) -> Value {
    Value::Integer(val)
}

pub fn real(val: f64) -> Value {
    Value::Real(val)
}

pub fn blob(val: Vec<u8>) -> Value {
    Value::Blob(val)
}

#[allow(non_upper_case_globals)]
pub const null: Value = Value::Null;
