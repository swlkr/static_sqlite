pub use ffi::{Error, FromRow, Result, Savepoint, Sqlite, Value};
pub use static_sqlite_macros::sql;
extern crate self as static_sqlite;

mod ffi;

pub fn open(path: &str) -> Result<Sqlite> {
    Sqlite::open(path)
}

pub fn execute(conn: &Sqlite, sql: &str, params: &[Value]) -> Result<i32> {
    conn.execute(sql, params)
}

pub fn query<T: FromRow>(conn: &Sqlite, sql: &str, params: &[Value]) -> Result<Vec<T>> {
    conn.query(sql, params)
}

pub fn rows(conn: &Sqlite, sql: &str, params: &[Value]) -> Result<Vec<Vec<(String, Value)>>> {
    conn.rows(sql, params)
}

pub fn savepoint<'a>(conn: &'a Sqlite, name: &'a str) -> Result<Savepoint<'a>> {
    conn.savepoint(conn, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    sql! {
        let create_rows = r#"
            create table rows (
                id integer primary key,
                not_null_text text not null,
                not_null_integer integer not null,
                not_null_real real not null,
                not_null_blob blob not null,
                null_text text,
                null_integer integer,
                null_real real,
                null_blob blob,
                nullable_text text,
                nullable_integer integer,
                nullable_real real,
                nullable_blob blob
            )
        "# as Row;

        let insert_row = r#"
            insert into rows (
                not_null_text,
                not_null_integer,
                not_null_real,
                not_null_blob,
                null_text,
                null_integer,
                null_real,
                null_blob,
                nullable_text,
                nullable_integer,
                nullable_real,
                nullable_blob
            )
            values (
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?
            )
            returning *
        "#;
    }

    fn user_version(db: &Sqlite) -> Result<i64> {
        let rws = rows(&db, "PRAGMA user_version", &[])?;
        match rws.into_iter().nth(0) {
            Some(cols) => match cols.into_iter().nth(0) {
                Some(pair) => pair.1.try_into(),
                None => Ok(0),
            },
            None => Ok(0),
        }
    }

    fn set_user_version(db: &Sqlite, version: i64) -> Result<()> {
        let _ = execute(&db, &format!("PRAGMA user_version = {version}"), &[])?;
        Ok(())
    }

    fn migrate(db: &Sqlite) -> Result<()> {
        let sp = savepoint(db, "migrate")?;
        let version = user_version(&sp)?;
        match version {
            0 => {
                create_rows(&sp)?;
            }
            _ => {}
        }

        set_user_version(&sp, 1)?;

        Ok(())
    }

    fn db(path: &str) -> Result<Sqlite> {
        let db = static_sqlite::open(path)?;
        let _ = execute(&db, "PRAGMA journal_mode = wal;", &[])?;
        let _ = execute(&db, "PRAGMA synchronous = normal;", &[])?;
        let _ = execute(&db, "PRAGMA foreign_keys = on;", &[])?;
        Ok(db)
    }

    #[test]
    fn it_works() -> Result<()> {
        let db = db(":memory:")?;

        let _ = migrate(&db)?;

        let row = insert_row(
            &db,
            "not_null_text".into(),
            1,
            1.0,
            vec![0xBE, 0xEF],
            None,
            None,
            None,
            None,
            Some("nullable_text".into()),
            Some(2),
            Some(2.0),
            Some(vec![0xFE, 0xED]),
        )?;

        assert_eq!(
            row,
            Row {
                id: 1,
                not_null_text: "not_null_text".into(),
                not_null_integer: 1,
                not_null_real: 1.,
                not_null_blob: vec![0xBE, 0xEF],
                null_text: None,
                null_integer: None,
                null_real: None,
                null_blob: None,
                nullable_text: Some("nullable_text".into()),
                nullable_integer: Some(2),
                nullable_real: Some(2.),
                nullable_blob: Some(vec![0xFE, 0xED]),
            }
        );

        Ok(())
    }
}
