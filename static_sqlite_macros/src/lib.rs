use std::collections::HashMap;
use std::ops::ControlFlow;

use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use sqlparser::ast::{
    visit_relations, Delete, FromTable, Insert, SelectItem, TableFactor, TableWithJoins,
};
use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};
use syn::{parse_macro_input, Error, LitStr, LocalInit, PatIdent, Result};

mod errors;
mod names;

use static_sqlite_core::{self as sqlite, Sqlite};

/// Make rust structs and functions from sql
///
/// ```
/// # use static_sqlite::{sql, Result};
/// #
/// sql! {
///  let migrate = r#"
///    create table Row (id integer primary key);
///  "#;
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let db = static_sqlite::open("db.sqlite3")?;
///   let _ = migrate(&db).await?;
///
///   Ok(())
/// }
/// ```
///
/// The migration sql string is required. You can call it whatever you want
/// but you should have some sql in there, because that is what the rest of macro
/// uses to determine the schema. This macro is tokens in tokens out, there are no
/// side effects, no files written. Just tokens. Which is why you need to either
/// define your sqlite schema in that migrate fn. Each sql statement in that migrate
/// string is a migration. Migrations are executed top to bottom.
/// Each let ident becomes a function and each create/alter table sql statement becomes
/// a struct.
///
/// ```
/// # use static_sqlite::sql;
/// #
/// sql! {
///   let migrate = r#"
///     create table Row (id integer primary key);
///     alter table Row add column updated_at integer;
///   "#;
///
///   let insert_row = r#"
///     insert into Row (updated_at)
///     values (?)
///     returning *
///   "#;
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let db = static_sqlite::open("db.sqlite3")?;
///   let _ = migrate(&db).await?;
///   let row = insert_row(&db, 0).await?;
///
///   Ok(())
/// }
/// ```
#[proc_macro]
pub fn sql(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let SqlExprs(sql_exprs) = parse_macro_input!(input as SqlExprs);
    match sql_macro(sql_exprs) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// There are four things we want to do
// 1. Parse the sql, return any parsing errors (this happens when parsing the input impl syn::parse::Parse for SqlExprs)
// 2. Run the sql against an in memory sqlite db, looking for any sqlite errors
// 3. Generate the structs from the migrate expr (the one with only ddl sql statements)
// 4. Generate the fns from the other idents in the sql! macro
fn sql_macro(exprs: Vec<SqlExpr>) -> syn::Result<TokenStream> {
    let (migrate_expr, exprs) = split_exprs(&exprs)?;
    let db = match sqlite::open(":memory:") {
        Ok(db) => db,
        Err(err) => return Err(syn::Error::new(Span::call_site(), err)),
    };
    if let Err(err) = db.execute_all("PRAGMA foreign_keys = ON;") {
        return Err(syn::Error::new(Span::call_site(), err));
    };
    // validate migrate expr
    for stmt in &migrate_expr.statements {
        if let Err(err) = db.execute_all(&stmt.to_string()) {
            return Err(syn::Error::new(migrate_expr.ident.span(), err));
        }
    }
    let _ = names::validate_migrate_expr(&migrate_expr)?;
    let migrate_fn = migrate_fn(&migrate_expr);
    let schema = schema(&db);
    let structs = structs_tokens(migrate_expr.ident.span(), &schema);
    let fns = fn_tokens(&db, &schema, &exprs)?;
    let traits = trait_tokens(&schema, &exprs);
    let output = quote! {
        #(#structs)*
        #(#fns)*
        #(#traits)*
        #migrate_fn
    };

    Ok(output)
}

fn trait_parts(statement: &Statement) -> Option<(String, Option<&[SelectItem]>)> {
    match &statement {
        Statement::Insert(Insert {
            table_name,
            returning,
            ..
        }) => Some((
            table_name.to_string(),
            returning.as_ref().map(|x| x.as_slice()),
        )),
        Statement::Update {
            table, returning, ..
        } => match &table.relation {
            TableFactor::Table { name, .. } => {
                Some((name.to_string(), returning.as_ref().map(|x| x.as_slice())))
            }
            _ => None,
        },
        Statement::Delete(Delete {
            from, returning, ..
        }) => match from {
            FromTable::WithFromKeyword(table) => match &table[..] {
                [TableWithJoins { relation, .. }] => match relation {
                    TableFactor::Table { name, .. } => {
                        Some((name.to_string(), returning.as_ref().map(|x| x.as_slice())))
                    }
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        },
        Statement::Query(query) => match query.body.as_ref() {
            sqlparser::ast::SetExpr::Select(ref select) => {
                let table = &select.from;
                match &table[..] {
                    [TableWithJoins { relation, .. }] => match relation {
                        TableFactor::Table { name, .. } => {
                            Some((name.to_string(), Some(select.projection.as_slice())))
                        }
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        },
        _ => None,
    }
}

fn trait_tokens(schema: &HashMap<String, Vec<SchemaRow>>, exprs: &[&SqlExpr]) -> Vec<TokenStream> {
    exprs
        .iter()
        .flat_map(|expr| {
            let ident = &expr.ident;
            let query_ident = snake_to_pascal_case(&ident);
            expr.statements
                .iter()
                .filter_map(trait_parts)
                .map(|(table_name, returning)| match returning {
                    Some(select_items) => {
                        let fields: Vec<_> = select_items
                            .iter()
                            .flat_map(|si| match si {
                                sqlparser::ast::SelectItem::UnnamedExpr(sql_expr) => match sql_expr
                                {
                                    sqlparser::ast::Expr::Identifier(ident) => {
                                        vec![Ident::new(&ident.to_string(), expr.ident.span())]
                                    }
                                    _ => todo!(),
                                },
                                sqlparser::ast::SelectItem::ExprWithAlias {
                                    expr: sql_expr,
                                    alias: _,
                                } => match sql_expr {
                                    sqlparser::ast::Expr::Identifier(ident) => {
                                        vec![Ident::new(&ident.to_string(), expr.ident.span())]
                                    }
                                    _ => todo!(),
                                },
                                sqlparser::ast::SelectItem::QualifiedWildcard(
                                    object_name,
                                    _wildcard_additional_options,
                                ) => match schema.get(&object_name.to_string()) {
                                    Some(rows) => rows
                                        .iter()
                                        .map(|row| Ident::new(&row.column_name, expr.ident.span()))
                                        .collect::<Vec<_>>(),
                                    None => todo!(),
                                },
                                sqlparser::ast::SelectItem::Wildcard(
                                    _wildcard_additional_options,
                                ) => match schema.get(&table_name) {
                                    Some(rows) => rows
                                        .iter()
                                        .map(|row| Ident::new(&row.column_name, expr.ident.span()))
                                        .collect::<Vec<_>>(),
                                    None => todo!(),
                                },
                            })
                            .collect();
                        let table_ident = Ident::new(&table_name, expr.ident.span());
                        quote! {
                            impl From<#query_ident> for #table_ident {
                                fn from(#query_ident { #(#fields,)* }: #query_ident) -> Self {
                                    Self {
                                        #(#fields,)*
                                        ..Default::default()
                                    }
                                }
                            }
                        }
                    }
                    None => quote! {},
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

fn input_column_names(db: &Sqlite, expr: &SqlExpr) -> syn::Result<Vec<String>> {
    let mut output = vec![];
    match db.bind_param_names(&expr.sql) {
        Ok(names) => output.extend(names),
        Err(err) => return Err(syn::Error::new(expr.ident.span(), err.to_string())),
    }
    Ok(output)
}

fn output_column_names(db: &Sqlite, expr: &SqlExpr) -> syn::Result<Vec<String>> {
    let mut output = vec![];
    match db.aliased_column_names(&expr.sql) {
        Ok(names) => output.extend(names),
        Err(err) => return Err(syn::Error::new(expr.ident.span(), err.to_string())),
    }
    Ok(output)
}

fn table_names(db: &Sqlite, expr: &SqlExpr) -> syn::Result<Vec<String>> {
    let mut output = vec![];
    for stmt in &expr.statements {
        match stmt {
            Statement::Insert(Insert { table_name, .. }) => output.push(table_name.to_string()),
            Statement::Update { table, .. } => output.extend(table_with_joins(&table)),
            Statement::Delete(Delete { tables, from, .. }) => {
                output.extend(tables.iter().map(|table| table.to_string()));
                match from {
                    sqlparser::ast::FromTable::WithFromKeyword(vec) => {
                        output.extend(vec.iter().flat_map(table_with_joins));
                    }
                    sqlparser::ast::FromTable::WithoutKeyword(vec) => {
                        output.extend(vec.iter().flat_map(table_with_joins));
                    }
                };
            }
            // easier to grab the table names from the sqlite c api
            Statement::Query(_) => output.extend(db.table_names(&expr.sql).unwrap_or_default()),
            _ => todo!(),
        }
    }
    Ok(output)
}

fn table_with_joins(table: &TableWithJoins) -> Vec<String> {
    let mut output = vec![];
    output.extend(table_factor_tables(&table.relation));

    for join in &table.joins {
        output.extend(table_factor_tables(&join.relation));
    }

    output
}

fn table_factor_tables(table_factor: &TableFactor) -> Vec<String> {
    match table_factor {
        TableFactor::Table { name, .. } => vec![name.to_string()],
        _ => todo!(),
    }
}

#[derive(Debug, Default, Clone)]
struct SchemaRow {
    table_name: String,
    column_name: String,
    column_type: String,
    not_null: i64,
    pk: i64,
}

impl static_sqlite_core::FromRow for SchemaRow {
    fn from_row(
        columns: Vec<(String, static_sqlite_core::Value)>,
    ) -> static_sqlite_core::Result<Self> {
        let mut row = SchemaRow::default();
        for (name, value) in columns {
            match name.as_str() {
                "table_name" => row.table_name = value.try_into()?,
                "column_name" => row.column_name = value.try_into()?,
                "column_type" => row.column_type = value.try_into()?,
                "not_null" => row.not_null = value.try_into()?,
                "pk" => row.pk = value.try_into()?,
                _ => {}
            }
        }

        Ok(row)
    }
}

fn schema(db: &Sqlite) -> HashMap<String, Vec<SchemaRow>> {
    let rows: static_sqlite_core::Result<Vec<SchemaRow>> = db.query(
        r#"
        select
            m.tbl_name as table_name,
            p.name as column_name,
            p."notnull" as not_null,
            p.pk,
            p.type as column_type
        from sqlite_master m
        join pragma_table_info(m.name) p
        where m.type = 'table'
            and m.tbl_name not like 'sqlite_%'
        order by
            m.tbl_name,
            p.cid;"#,
        &[],
    );

    match rows {
        Ok(rows) => rows.into_iter().fold(HashMap::new(), |mut acc, row| {
            acc.entry(row.table_name.clone())
                .or_insert_with(Vec::new)
                .push(row);
            acc
        }),
        Err(_) => todo!(),
    }
}

// Splits the SqlExpr into migrate (the first found ddl only one) and the others
fn split_exprs<'a>(exprs: &'a Vec<SqlExpr>) -> Result<(&'a SqlExpr, Vec<&'a SqlExpr>)> {
    // we need to find the one expr that has all ddl statements
    // treat this as the migrate fn
    // this also grabs the db schema
    let mut iter = exprs.iter();
    let migrate_expr = iter.find(|expr| is_ddl(expr)).ok_or(syn::Error::new(
        Span::call_site(),
        r#"You need a migration fn. Try this:
              let migrate = r\#"create table YourTable (id integer primary key);"\#;
                            "#,
    ))?;
    let exprs = iter.filter(|ex| !is_ddl(ex)).collect();

    Ok((migrate_expr, exprs))
}

/// Generates the migrate fn tokens
///
/// It uses a sqlite savepoint to either
/// migrate everything or rollback the transaction
/// if something failed
fn migrate_fn(expr: &SqlExpr) -> TokenStream {
    let SqlExpr { ident, sql, .. } = expr;

    quote! {
        pub async fn #ident(sqlite: &static_sqlite::Sqlite) -> Result<()> {
            let sql = #sql.to_string();
            let _ = static_sqlite::execute_all(&sqlite, "create table if not exists __migrations__ (sql text primary key not null);".into()).await?;
            for stmt in sql.split(";").filter(|s| !s.trim().is_empty()) {
                let mig: String = stmt.chars().filter(|c| !c.is_whitespace()).collect();
                let changed = static_sqlite::execute(&sqlite, "insert into __migrations__ (sql) values (:sql) on conflict (sql) do nothing".into(), vec![static_sqlite::Value::Text(mig)]).await?;
                if changed != 0 {
                    let _k = static_sqlite::execute(&sqlite, stmt.to_string(), vec![]).await?;
                }
            }
            return Ok(());
        }
    }
}

fn fn_tokens(db: &Sqlite, schema: &Schema, exprs: &[&SqlExpr]) -> Result<Vec<TokenStream>> {
    let mut output = vec![];
    for expr in exprs {
        if let None = expr.statements.last() {
            return Err(syn::Error::new(
                expr.ident.span(),
                "At least one sql statement is required",
            ));
        }

        let inputs = input_column_names(db, expr)?;
        let inputs: Vec<_> = inputs
            .iter()
            .map(|input| input.replacen(":", "", 1))
            .collect();
        let mut table_names = table_names(db, expr)?;
        // get joined table names that might not exist in select clause
        table_names.extend(join_table_names(expr));
        let mut schema_rows = vec![];
        for table_name in &table_names {
            match schema.get(table_name) {
                Some(rows) => schema_rows.extend(rows),
                None => {}
            };
        }
        let input_schema_rows: Vec<&&SchemaRow> = inputs
            .iter()
            .filter_map(|col_name| schema_rows.iter().find(|row| &row.column_name == col_name))
            .collect();
        let fn_args = input_schema_rows
            .iter()
            .map(|field| {
                let field_type = match field.column_type.as_str() {
                    "BLOB" => quote! { Vec<u8> },
                    "INTEGER" => quote! { i64 },
                    "REAL" | "DOUBLE" => quote! { f64 },
                    "TEXT" => quote! { impl ToString },
                    _ => unimplemented!("Sqlite fn arg not supported"),
                };
                let field_name = Ident::new(&field.column_name, expr.ident.span());
                let not_null = field.not_null;
                let pk = field.pk;
                match (pk, not_null) {
                    (0, 0) => quote! { #field_name: Option<#field_type> },
                    _ => quote! { #field_name: #field_type },
                }
            })
            .collect::<Vec<TokenStream>>();
        let params = input_schema_rows
            .iter()
            .map(|field| {
                let not_null = field.not_null;
                let name = Ident::new(&field.column_name, expr.ident.span());
                match field.column_type.as_str() {
                    "BLOB" => {
                        quote! { #name.into() }
                    }
                    "INTEGER" => quote! { #name.into() },
                    "REAL" | "DOUBLE" => quote! { #name.into() },
                    "TEXT" => match not_null {
                        1 => quote! {
                            #name.to_string().into()
                        },
                        0 => quote! {
                            match #name {
                                Some(val) => val.to_string().into(),
                                None => static_sqlite::Value::Null
                            }
                        },
                        _ => unreachable!(),
                    },
                    _ => unimplemented!("Sqlite param not supported"),
                }
            })
            .collect::<Vec<TokenStream>>();
        let ident = &expr.ident;
        let outputs = output_column_names(db, expr)?;
        let pascal_case = snake_to_pascal_case(&ident);
        let cols: Vec<SchemaRow> = outputs
            .iter()
            .filter_map(|col_name| {
                schema_rows
                    .iter()
                    .find(|row| &row.column_name == col_name)
                    .cloned()
                    .cloned()
            })
            .collect();
        let struct_tokens = struct_tokens(expr.ident.span(), &pascal_case, &cols);
        let sql = &expr.sql;
        output.push(quote! {
            #struct_tokens

            #[doc = #sql]
            pub async fn #ident(db: &static_sqlite::Sqlite, #(#fn_args),*) -> Result<Vec<#pascal_case>> {
                let rows: Vec<#pascal_case> = static_sqlite::query(db, #sql, vec![#(#params,)*]).await?;
                Ok(rows)
            }
        })
    }
    Ok(output)
}

fn join_table_names(expr: &&SqlExpr) -> Vec<String> {
    let mut output = vec![];
    visit_relations(&expr.statements, |rel| {
        output.push(rel.to_string());
        ControlFlow::<()>::Continue(())
    });
    output
}

fn snake_to_pascal_case(input: &syn::Ident) -> syn::Ident {
    let s = input.to_string();
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = true;

    for ch in s.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.extend(ch.to_uppercase());
            capitalize_next = false;
        } else {
            result.extend(ch.to_lowercase());
        }
    }

    syn::Ident::new(&result, input.span())
}

type Schema = HashMap<String, Vec<SchemaRow>>;

fn structs_tokens(span: Span, schema: &Schema) -> Vec<TokenStream> {
    schema
        .iter()
        .map(|(table, cols)| {
            let ident = proc_macro2::Ident::new(&table, span);
            struct_tokens(span, &ident, cols)
        })
        .collect()
}

fn struct_tokens(span: Span, ident: &Ident, cols: &Vec<SchemaRow>) -> TokenStream {
    let struct_fields = cols.iter().map(|row| {
        let field_type = field_type(row);
        let name = Ident::new(&row.column_name, span);
        let optional = match (row.not_null, row.pk) {
            (0, 0) => true,
            (0, 1) | (1, 0) | (1, 1) => false,
            _ => unreachable!(),
        };

        match optional {
            true => quote! { pub #name: Option<#field_type> },
            false => quote! { pub #name: #field_type },
        }
    });
    let match_stmt = cols.iter().map(|field| {
        let name = Ident::new(&field.column_name, span);
        let lit_str = LitStr::new(&field.column_name, span);

        quote! {
            #lit_str => row.#name = value.try_into()?
        }
    });
    let tokens = quote! {
        #[derive(Default, Debug, Clone, PartialEq)]
        pub struct #ident { #(#struct_fields),* }

        impl static_sqlite::FromRow for #ident {
            fn from_row(columns: Vec<(String, static_sqlite::Value)>) -> static_sqlite::Result<Self> {
                let mut row = #ident::default();
                for (column, value) in columns {
                    match column.as_str() {
                        #(#match_stmt,)*
                        _ => {}
                    }
                }

                Ok(row)
            }
        }
    };

    tokens
}

fn field_type(row: &SchemaRow) -> TokenStream {
    match row.column_type.as_str() {
        "BLOB" => quote! { Vec<u8> },
        "INTEGER" => quote! { i64 },
        "REAL" | "DOUBLE" => quote! { f64 },
        "TEXT" => quote! { String },
        _ => todo!("field_type"),
    }
}

#[derive(Clone, Debug)]
struct SqlExpr {
    ident: Ident,
    sql: String,
    statements: Vec<Statement>,
}

#[derive(Debug)]
struct SqlExprs(pub Vec<SqlExpr>);

impl syn::parse::Parse for SqlExprs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut sql_exprs: Vec<SqlExpr> = Vec::new();
        while !input.is_empty() {
            let stmt: syn::Stmt = input.parse()?;
            let sql_expr = sql_expr(stmt)?;
            sql_exprs.push(sql_expr);
        }
        Ok(SqlExprs(sql_exprs))
    }
}

fn sql_expr(stmt: syn::Stmt) -> Result<SqlExpr> {
    match stmt {
        syn::Stmt::Local(syn::Local { pat, init, .. }) => {
            let ident = match pat {
                syn::Pat::Ident(PatIdent { ident, .. }) => ident,
                _ => unimplemented!("Only idents are supported for sql"),
            };
            let sql = match init {
                Some(LocalInit { expr, .. }) => match *expr {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(lit_str),
                        ..
                    }) => lit_str.value(),
                    _ => return Err(Error::new_spanned(ident, "sql is missing")),
                },
                None => return Err(Error::new_spanned(ident, "sql is missing")),
            };
            let statements = match Parser::parse_sql(&SQLiteDialect {}, &sql) {
                Ok(ast) => ast,
                Err(err) => {
                    // TODO: better error handling
                    return Err(Error::new_spanned(ident, err.to_string()));
                }
            };
            Ok(SqlExpr {
                ident,
                sql,
                statements,
            })
        }
        syn::Stmt::Item(_) => todo!("todo item"),
        syn::Stmt::Expr(_, _) => todo!("todo expr"),
        syn::Stmt::Macro(_) => todo!("todo macro"),
    }
}

fn is_ddl(expr: &SqlExpr) -> bool {
    expr.statements.iter().all(|stmt| match stmt {
        Statement::CreateView { .. }
        | Statement::CreateTable { .. }
        | Statement::CreateVirtualTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateRole { .. }
        | Statement::AlterTable { .. }
        | Statement::AlterIndex { .. }
        | Statement::AlterView { .. }
        | Statement::AlterRole { .. }
        | Statement::Drop { .. }
        | Statement::DropFunction { .. }
        | Statement::CreateExtension { .. }
        | Statement::SetNamesDefault {}
        | Statement::ShowFunctions { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowCollation { .. }
        | Statement::Use { .. }
        | Statement::StartTransaction { .. }
        | Statement::SetTransaction { .. }
        | Statement::Comment { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::CreateSchema { .. }
        | Statement::CreateDatabase { .. }
        | Statement::CreateFunction { .. }
        | Statement::CreateProcedure { .. }
        | Statement::CreateMacro { .. }
        | Statement::CreateStage { .. }
        | Statement::Prepare { .. }
        | Statement::ExplainTable { .. }
        | Statement::Explain { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::CreateSequence { .. }
        | Statement::CreateType { .. }
        | Statement::Pragma { .. } => true,
        _ => false,
    })
}
