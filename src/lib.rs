pub use ffi::{Error, FromRow, Result, Savepoint, Value};
pub use static_sqlite_macros::sql;
extern crate self as static_sqlite;

mod ffi;
mod sync;
mod tokio;

pub use sync::migrate;
pub use tokio::*;

#[cfg(test)]
mod tests {
    use super::{migrate, sql, Result, Sqlite};

    sql! {
        let create_users = r#"
            create table users (
                id integer not null primary key,
                email text not null unique
            )
        "# as User;

        let create_rows = r#"
            create table rows (
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

    async fn db(path: &str) -> Result<Sqlite> {
        let sqlite = static_sqlite::open(path).await?;
        sqlite.call(|db| {
            db.execute_all(
                r#"
                    PRAGMA journal_mode = wal;
                    PRAGMA synchronous = normal;
                    PRAGMA foreign_keys = on;
                    PRAGMA busy_timeout = 5000;
                    PRAGMA cache_size = -64000;
                    PRAGMA strict = on;
                "#
            )?;

            let migrations = &[
                create_rows,
                create_users
            ];
            migrate(db, migrations)
        }).await?;

        Ok(sqlite)
    }

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let db = db(":memory:").await?;

        let row = insert_row(
            db,
            "not_null_text",
            1,
            1.0,
            vec![0xBE, 0xEF],
            None::<String>,
            None,
            None,
            None,
            Some("nullable_text"),
            Some(2),
            Some(2.0),
            Some(vec![0xFE, 0xED]),
        ).await?;

        assert_eq!(
            row,
            Row {
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
