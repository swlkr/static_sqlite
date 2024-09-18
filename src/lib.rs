pub use ffi::{blob, integer, null, real, text, Error, Result, Savepoint, Sqlite, Value};
pub use static_sqlite_macros::sql;
extern crate self as static_sqlite;

mod ffi;

pub fn open(path: &str) -> Result<Sqlite> {
    Sqlite::open(path)
}

pub fn execute(conn: &Sqlite, sql: &str, params: &[Value]) -> Result<()> {
    conn.execute(sql, params)
}

pub fn savepoint<'a>(conn: &'a Sqlite, name: &'a str) -> Result<Savepoint<'a>> {
    conn.savepoint(conn, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migrate(db: &Sqlite) -> Result<()> {
        let create_migrations =
            "create table if not exists migrations (version integer primary key)";
        let create_tests=
            "create table if not exists tests (id integer primary key, txt text, inte integer, rl real, blb blob, null_text text)";

        let _ = execute(db, &create_migrations, &[])?;
        let _ = execute(db, &create_tests, &[])?;

        Ok(())
    }

    #[test]
    fn it_works() -> Result<()> {
        let db = static_sqlite::open(":memory:")?;

        let _ = migrate(&db)?;

        let sql = "insert into tests (txt, inte, rl, blb, null_text) values (?, ?, ?, ?, ?)";
        let params = vec![
            text("txt"),
            integer(1),
            real(1.),
            blob(vec![0xFF, 0x00, 0xFF, 0x00]),
            null,
        ];

        let _ = execute(&db, sql, &params)?;

        Ok(())
    }
}
