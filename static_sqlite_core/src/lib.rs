mod ffi;
pub use ffi::{DataType, Error, FromRow, Result, Savepoint, Sqlite, Value};

pub fn open(path: &str) -> Result<Sqlite> {
    Sqlite::open(path)
}

pub fn execute(conn: &Sqlite, sql: &str, params: Vec<Value>) -> Result<i32> {
    conn.execute(sql, params)
}

pub fn execute_all(conn: &Sqlite, sql: &str) -> Result<i32> {
    conn.execute(sql, vec![])
}

pub fn query<T: FromRow + Send + 'static>(
    conn: &Sqlite,
    sql: &'static str,
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

impl FromRow for () {
    fn from_row(_columns: Vec<(String, Value)>) -> Result<Self> {
        Ok(())
    }
}

pub trait FirstRow<T>
where
    T: FromRow,
{
    fn first_row(self) -> Result<T>;
}

impl<T> FirstRow<T> for Vec<T>
where
    T: FromRow,
{
    fn first_row(self) -> Result<T> {
        match self.into_iter().nth(0) {
            Some(row) => Ok(row),
            None => Err(Error::RowNotFound),
        }
    }
}
