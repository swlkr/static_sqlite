pub use ffi::{blob, integer, null, real, text, Error, FromRow, Result, Savepoint, Sqlite, Value};
pub use static_sqlite_macros::sql;
extern crate self as static_sqlite;

mod ffi;

pub fn open(path: &str) -> Result<Sqlite> {
    Sqlite::open(path)
}

pub fn execute(conn: &Sqlite, sql: &str, params: &[Value]) -> Result<()> {
    conn.execute(sql, params)
}

pub fn query<T: FromRow>(conn: &Sqlite, sql: &str, params: &[Value]) -> Result<Vec<T>> {
    conn.query(sql, params)
}

pub fn savepoint<'a>(conn: &'a Sqlite, name: &'a str) -> Result<Savepoint<'a>> {
    conn.savepoint(conn, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migrate(db: &Sqlite) -> Result<()> {
        let create_migrations = "create table migrations (version integer primary key)";
        let create_rows = "
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
            )";

        let sp = savepoint(db, "migrate")?;
        let _ = execute(&sp, create_migrations, &[])?;
        let _ = execute(&sp, create_rows, &[])?;

        Ok(())
    }

    #[test]
    fn it_works() -> Result<()> {
        let db = static_sqlite::open(":memory:")?;

        let _ = migrate(&db)?;

        let sql = "
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
            returning *";
        let params = vec![
            text("not_null_text"),
            integer(1),
            real(1.),
            blob(vec![0xFF, 0x00, 0xFF, 0x00]),
            null,
            null,
            null,
            null,
            text("nullable_text"),
            integer(2),
            real(2.),
            blob(vec![0x00, 0xFF, 0x00, 0xFF]),
        ];

        #[derive(PartialEq, Debug, Default)]
        struct Row {
            not_null_text: String,
            not_null_integer: i64,
            not_null_real: f64,
            not_null_blob: Vec<u8>,
            null_text: Option<String>,
            null_integer: Option<i64>,
            null_real: Option<f64>,
            null_blob: Option<Vec<u8>>,
            nullable_text: Option<String>,
            nullable_integer: Option<i64>,
            nullable_real: Option<f64>,
            nullable_blob: Option<Vec<u8>>,
        }

        impl FromRow for Row {
            fn from_row(columns: Vec<(String, Value)>) -> Result<Self> {
                let mut row = Row::default();
                for (column, value) in columns {
                    match column.as_str() {
                        "not_null_text" => row.not_null_text = value.try_into()?,
                        "not_null_integer" => row.not_null_integer = value.try_into()?,
                        "not_null_real" => row.not_null_real = value.try_into()?,
                        "not_null_blob" => row.not_null_blob = value.try_into()?,
                        "null_text" => row.null_text = value.try_into()?,
                        "null_integer" => row.null_integer = value.try_into()?,
                        "null_real" => row.null_real = value.try_into()?,
                        "null_blob" => row.null_blob = value.try_into()?,
                        "nullable_text" => row.nullable_text = value.try_into()?,
                        "nullable_integer" => row.nullable_integer = value.try_into()?,
                        "nullable_real" => row.nullable_real = value.try_into()?,
                        "nullable_blob" => row.nullable_blob = value.try_into()?,
                        _ => {}
                    }
                }

                Ok(row)
            }
        }

        let tests: Vec<Row> = query(&db, sql, &params)?;

        assert_eq!(
            tests.into_iter().nth(0).unwrap(),
            Row {
                not_null_text: "not_null_text".into(),
                not_null_integer: 1,
                not_null_real: 1.,
                not_null_blob: vec![0xFF, 0x00, 0xFF, 0x00],
                null_text: None,
                null_integer: None,
                null_real: None,
                null_blob: None,
                nullable_text: Some("nullable_text".into()),
                nullable_integer: Some(2),
                nullable_real: Some(2.),
                nullable_blob: Some(vec![0x00, 0xFF, 0x00, 0xFF])
            }
        );

        Ok(())
    }
}
