# static_sqlite

An easy way to map sql to rust functions and structs

# Quickstart

```rust
use static_sqlite::{sql, Result, self};

sql! {
    let migrate = r#"
        create table User (
            id integer primary key,
            name text unique not null
        );

        alter table User
        add column created_at integer;

        alter table User
        drop column created_at;
    "#;

    let insert_user = r#"
        insert into User (name)
        values (:name)
        returning *
    "#;
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = static_sqlite::open("db.sqlite3").await?;
    let _ = migrate(&db).await?;
    let users = insert_user(&db, "swlkr").await?;
    let user = users.first().unwrap();

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
