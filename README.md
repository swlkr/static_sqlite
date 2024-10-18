# static_sqlite

An easy way to map sql to rust functions and structs

# Quickstart

```rust
use static_sqlite::{sql, Result, migrate};

sql! {
    let create_users = r#"
        create table users (
            id integer primary key,
            name text unique not null
        )
    "# as User;

    let insert_user = r#"
        insert into users (email)
        values (?)
        returning *
    "#;
}

#[tokio::main]
async fn main() -> Result<()> {
    let sqlite = static_sqlite::open("db.sqlite3").await?;
    sqlite.call(|db| {
      let migrations = &[create_users];
      migrate(db, migrations)
    }).await?;

    let user = insert_user(sqlite, "swlkr").await?;

    assert_eq!(user.id, 1);
    assert_eq!(user.name, "swlkr");

    Ok(())
}
```

# Use

```sh
cargo add --git https://github.com/swlkr/static_sqlite
```

# Treesitter

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
