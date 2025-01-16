use static_sqlite::sql;

sql! {
    let migrate = r#"
         create table User (
             id integer primary key,
             email text unique not null
         );

         create table Todo (
             id integer primary key,
             user_id integer not null references Users(id)
         );
     "#;
}
