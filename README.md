# static_sqlite

static_sqlite is a zero dependency way to map your sql to rust functions and structs easily.

# Install

```sh
cargo add static_sqlite
```

# Quickstart

```rust
use static_sqlite::{sql, Result, FromRow, savepoint};

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

    let upsert_migration = r#"
        insert into migrations (version)
        values (?)
        on conflict (version)
        do update set version = excluded.version + 1
        returning *
    "#;

    let create_users = r#"
        create table users (
            id integer primary key,
            email text unique not null,
            created_at integer not null default(unixepoch())
        )
    "# as User;

    let insert_user = r#"
        insert into users (email)
        values (?)
        returning *
    "#;
}

fn migrate(db: &Sqlite) -> Result<()> {
    let sp = savepoint(db, "migrate")?;
    let _ = create_migrations(&sp)?;
    let version = latest_migration(&sp)?.unwrap_or_default().version;
    match version {
        0 => {
            let _ = create_users(&sp)?;
        }
        _ => {}
    }
    let _ = upsert_migrations(&sp, version + 1)?;

    Ok(())
}

fn main() -> Result<()> {
    let db = static_sqlite::open("db.sqlite3")?;
    let _ = migrate(&db);

    let user = insert_user(&db, "readme@example.com".into())?;

    assert_eq!(User { id: 1, email: "readme@example.com".into(), created_at: 0 }, user)
}

```

Treesitter injection for sql syntax highlighting:

```
((macro_invocation
   macro:
     [
       (scoped_identifier
         name: (_) @_macro_name)
       (identifier) @_macro_name
     ]
   (token_tree
     (identifier)
     (raw_string_literal
       (string_content) @injection.content)))
 (#eq? @_macro_name "sql")
 (#set! injection.language "sql")
 (#set! injection.include-children))
```

Happy hacking!
