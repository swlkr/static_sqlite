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
        let create_migrations = r#"
            create table if not exists migrations (version integer primary key)
        "# as Migration;

        let latest_migration = r#"
            select version
            from migrations
            order by version desc
            limit 1
        "#;

        let update_migration = r#"
            insert into migrations (version)
            values (?)
            on conflict (version)
            do update set version = excluded.version + 1
            returning *
        "#;

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

    fn migrate(db: &Sqlite) -> Result<()> {
        let sp = savepoint(db, "migrate")?;
        let _ = create_migrations(&sp)?;
        let version = latest_migration(&sp)?.unwrap_or_default().version;
        match version {
            0 => {
                let _ = create_rows(&sp)?;
            }
            _ => {}
        }
        let _ = update_migration(&sp, version + 1)?;

        Ok(())
    }

    #[test]
    fn it_works() -> Result<()> {
        let db = static_sqlite::open(":memory:")?;

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
