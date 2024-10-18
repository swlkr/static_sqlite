pub use crate::ffi::Sqlite;
use crate::{FromRow, Result, Savepoint, Value};

#[allow(unused)]
pub fn open(path: &str) -> Result<Sqlite> {
    Sqlite::open(path)
}

pub fn execute(conn: &Sqlite, sql: &str, params: Vec<Value>) -> Result<i32> {
    conn.execute(sql, params)
}

#[allow(unused)]
pub fn query<T: FromRow + Send + 'static>(
    conn: &Sqlite,
    sql: &str,
    params: &[Value],
) -> Result<Vec<T>> {
    conn.query(sql, params)
}

pub fn rows(conn: &Sqlite, sql: &str, params: &[Value]) -> Result<Vec<Vec<(String, Value)>>> {
    conn.rows(sql, params)
}

pub fn savepoint<'a>(conn: &'a Sqlite, name: &'a str) -> Result<Savepoint<'a>> {
    conn.savepoint(conn, name)
}

pub(crate) fn user_version(db: &Sqlite) -> Result<i64> {
    let rws = rows(&db, "PRAGMA user_version", &[])?;
    match rws.into_iter().nth(0) {
        Some(cols) => match cols.into_iter().nth(0) {
            Some(pair) => pair.1.try_into(),
            None => Ok(0),
        },
        None => Ok(0),
    }
}

pub(crate) fn set_user_version(db: &Sqlite, version: usize) -> Result<()> {
    let _ = execute(&db, &format!("PRAGMA user_version = {version}"), vec![])?;
    Ok(())
}

pub fn migrate<F>(db: &Sqlite, migrations: &[F]) -> Result<()>
where
    F: Fn(&Sqlite) -> Result<()>,
{
    let sp = savepoint(db, "migrate")?;
    let version = user_version(&sp)?;
    let pending_migrations = &migrations[(version as usize)..];
    for migration in pending_migrations {
        migration(&sp)?;
    }
    set_user_version(&sp, migrations.len())?;

    Ok(())
}
