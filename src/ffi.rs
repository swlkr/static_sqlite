use std::{
    ffi::{c_char, c_int, c_void, CStr, CString},
    os::raw::c_double,
};

use crate::Error;

#[link(name = "sqlite3")]
extern "C" {
    fn sqlite3_open(filename: *const c_char, ppDb: *mut *mut c_void) -> c_int;
    pub fn sqlite3_close(db: *mut c_void) -> c_int;
    pub fn sqlite3_prepare_v2(
        db: *mut c_void,
        zSql: *const c_char,
        nByte: c_int,
        ppStmt: *mut *mut c_void,
        pzTail: *mut *const c_char,
    ) -> c_int;
    pub fn sqlite3_bind_text(
        stmt: *mut c_void,
        index: c_int,
        text: *const c_char,
        n: c_int,
        destructor: *const c_void,
    ) -> c_int;
    pub fn sqlite3_bind_int(stmt: *mut c_void, index: c_int, value: c_int) -> c_int;
    pub fn sqlite3_bind_double(stmt: *mut c_void, index: c_int, value: c_double) -> c_int;
    pub fn sqlite3_bind_blob(
        stmt: *mut c_void,
        index: c_int,
        value: *const c_void,
        n: c_int,
        destructor: *const c_void,
    ) -> c_int;
    pub fn sqlite3_step(stmt: *mut c_void) -> c_int;
    pub fn sqlite3_finalize(stmt: *mut c_void) -> c_int;
    pub fn sqlite3_errmsg(db: *mut c_void) -> *const c_char;
}

pub struct Sqlite {
    db: *mut c_void,
}

impl Sqlite {
    pub fn open(path: &str) -> Result<Self, Error> {
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

    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<(), Error> {
        let c_sql = CString::new(sql)?;
        let mut stmt: *mut c_void = std::ptr::null_mut();

        unsafe {
            if sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) != 0
            {
                let error = CStr::from_ptr(sqlite3_errmsg(self.db))
                    .to_string_lossy()
                    .into_owned();
                return Err(Error::Sqlite(error));
            }

            for (i, param) in params.iter().enumerate() {
                match param {
                    Value::Text(s) => {
                        let c_str = CString::new(s.as_str())?;
                        sqlite3_bind_text(
                            stmt,
                            (i + 1) as i32,
                            c_str.as_ptr(),
                            -1,
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
                            b.as_ptr() as *const c_void,
                            b.len() as i32,
                            std::ptr::null(),
                        );
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
}

impl Drop for Sqlite {
    fn drop(&mut self) {
        unsafe {
            sqlite3_close(self.db);
        }
    }
}

pub enum Value {
    Text(String),
    Integer(i32),
    Real(f64),
    Blob(Vec<u8>),
}
