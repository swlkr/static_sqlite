# static_sqlite

An easy way to map sql to rust functions and structs

# Quickstart

```rust
use static_sqlite::{sql, Result, migrate};

sql! {
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

fn main() -> Result<()> {
    let db = static_sqlite::open("db.sqlite3")?;
    let migrations = &[create_users];
    migrate(&db, migrations);

    let user = insert_user(&db, "readme@example.com".into())?;

    assert_eq!(User { id: 1, email: "readme@example.com".into(), created_at: 0 }, user)
}

```

# Use

```sh
cargo add static_sqlite
```

# Treesitter injection in macro syntax highlighting

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
