use static_sqlite::{sql, Result, Sqlite};

// #[tokio::test]
// async fn migrate_table_works() -> Result<()> {
//     sql! {
//         let migrate = r#"
//             create table User (
//                 id integer primary key,
//                 email text unique not null
//             );

//             alter table User add column created_at integer;
//         "#;

//         let insert_user = r#"
//             insert into User (email) values (:email) on conflict (email) do update set email = excluded.email
//         "#;

//         let insert_user_created = r#"
//             insert into User (email, created_at) values (:email, :created_at) on conflict (email) do update set email = excluded.email returning *
//         "#;
//     }

//     let db = static_sqlite::open(":memory:").await?;
//     migrate(&db).await?;
//     let email = "s@s.com";
//     insert_user(&db, email).await?;

//     let email = "f@f.com";
//     let users = insert_user_created(&db, email, Some(1)).await?;
//     let user = users.first().unwrap();

//     assert_eq!(user.id, 2);
//     assert_eq!(user.email, "f@f.com");
//     assert_eq!(user.created_at, Some(1));

//     Ok(())
// }

// #[tokio::test]
// async fn option_type_works() -> Result<()> {
//     sql! {
//         let migrate = r#"
//             create table Row (
//                 txt text
//             )
//         "#;

//         let insert_row = r#"
//             insert into Row (txt) values (:txt) returning *
//         "#;
//     }

//     let db = static_sqlite::open(":memory:").await?;
//     let _k = migrate(&db).await?;
//     let txt = Some("txt".to_string());
//     let rows = insert_row(&db, txt).await?;
//     let row = rows.first().unwrap();

//     assert_eq!(row.txt, Some("txt".into()));

//     Ok(())
// }

// #[tokio::test]
// async fn it_works() -> Result<()> {
//     sql! {
//         let migrations = r#"
//             create table User (
//                 id integer primary key,
//                 email text not null unique
//             );

//             create table Row (
//                 not_null_text text not null,
//                 not_null_integer integer not null,
//                 not_null_real real not null,
//                 not_null_blob blob not null,
//                 null_text text,
//                 null_integer integer,
//                 null_real real,
//                 null_blob blob
//             );

//             alter table Row add column nullable_text text;
//             alter table Row add column nullable_integer integer;
//             alter table Row add column nullable_real real;
//             alter table Row add column nullable_blob blob;
//         "#;

//         let insert_row = r#"
//             insert into Row (
//                 not_null_text,
//                 not_null_integer,
//                 not_null_real,
//                 not_null_blob,
//                 null_text,
//                 null_integer,
//                 null_real,
//                 null_blob,
//                 nullable_text,
//                 nullable_integer,
//                 nullable_real,
//                 nullable_blob
//             )
//             values (
//                 :not_null_text, :not_null_integer, :not_null_real, :not_null_blob,
//                 :null_text, :null_integer, :null_real, :null_blob,
//                 :nullable_text, :nullable_integer, :nullable_real, :nullable_blob
//             )
//             returning *
//         "#;
//     }

//     async fn db(path: &str) -> Result<Sqlite> {
//         let sqlite = static_sqlite::open(path).await?;
//         static_sqlite::execute_all(
//             &sqlite,
//             r#"
//             pragma journal_mode = wal;
//             pragma synchronous = normal;
//             pragma foreign_keys = on;
//             pragma busy_timeout = 5000;
//             pragma cache_size = -64000;
//             pragma strict = on;
//         "#,
//         )
//         .await?;
//         migrations(&sqlite).await?;
//         Ok(sqlite)
//     }

//     let db = db(":memory:").await?;

//     let rows = insert_row(
//         &db,
//         "not_null_text",
//         1,
//         1.0,
//         vec![0xBE, 0xEF],
//         None::<String>,
//         None,
//         None,
//         None,
//         Some("nullable_text"),
//         Some(2),
//         Some(2.0),
//         Some(vec![0xFE, 0xED]),
//     )
//     .await?;
//     let row = rows.into_iter().nth(0).unwrap();

//     assert_eq!(
//         row,
//         InsertRow {
//             not_null_text: "not_null_text".into(),
//             not_null_integer: 1,
//             not_null_real: 1.,
//             not_null_blob: vec![0xBE, 0xEF],
//             null_text: None,
//             null_integer: None,
//             null_real: None,
//             null_blob: None,
//             nullable_text: Some("nullable_text".into()),
//             nullable_integer: Some(2),
//             nullable_real: Some(2.),
//             nullable_blob: Some(vec![0xFE, 0xED]),
//         }
//     );

//     Ok(())
// }

// #[tokio::test]
// async fn readme_works() -> Result<()> {
//     sql! {
//         let migrate = r#"
//             create table User (
//                 id integer primary key,
//                 name text unique not null
//             );

//             alter table User
//             add column created_at integer;

//             alter table User
//             drop column created_at;
//         "#;

//         let insert_user = r#"
//             insert into User (name)
//             values (:name)
//             returning *
//         "#;
//     }

//     let db = static_sqlite::open(":memory:").await?;
//     let _ = migrate(&db).await?;
//     let rows= insert_user(&db, "swlkr").await?;
//     let user = rows.first().unwrap();

//     assert_eq!(user.id, 1);
//     assert_eq!(user.name, "swlkr");

//     Ok(())
// }

// #[tokio::test]
// async fn crud_works() -> Result<()> {
//     sql! {
//         let migrate = r#"
//             create table User (
//                 id integer primary key,
//                 name text unique not null
//             );
//         "#;

//         let insert_user = r#"
//             insert into User (name)
//             values (:name)
//             returning *
//         "#;

//         let update_user = r#"
//             update User set name = :name where id = :id returning *
//         "#;

//         let delete_user = r#"
//             delete from User where id = :id
//         "#;

//         let all_users = r#"
//             select id, name from User
//         "#;
//     }

//     let db = static_sqlite::open(":memory:").await?;
//     let _ = migrate(&db).await?;
//     let rows= insert_user(&db, "swlkr").await?;
//     let user = rows.first().unwrap();
//     assert_eq!(user.id, 1);
//     assert_eq!(user.name, "swlkr");

//     let users = all_users(&db).await?;
//     assert_eq!(users.len(), 1);
//     let user = users.first().unwrap();
//     assert_eq!(user.id, 1);
//     assert_eq!(user.name, "swlkr");

//     let users= update_user(&db, "swlkr2", 1).await?;
//     let user = users.first().unwrap();
//     assert_eq!(user.id, 1);
//     assert_eq!(user.name, "swlkr2");

//     delete_user(&db, 1).await?;
//     let users = all_users(&db).await?;
//     assert_eq!(users.len(), 0);

//     Ok(())
// }

// #[test]
// fn ui() {
//     let t = trybuild::TestCases::new();
//     t.compile_fail("tests/ui/*.rs");
// }

#[tokio::test]
async fn where_clause_works() -> Result<()> {
    sql! {
        let migrate = r#"
            create table User (
                id integer primary key,
                email text unique not null
            );

            create table Session (
                id integer primary key,
                user_id integer not null references User(id)
            )
        "#;

        let select_user = r#"
            select User.* from User join Session on Session.user_id = User.id where Session.id = :id
        "#;
    }
    let db = static_sqlite::open(":memory:").await?;
    migrate(&db).await?;
    let users = select_user(&db, 1).await?;
    assert_eq!(users.len(), 0);

    Ok(())
}
