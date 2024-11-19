use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use sqlparser::ast::{ColumnDef, DataType, ObjectName, SelectItem, TableFactor, TableWithJoins};
use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};
use syn::{parse_macro_input, Error, LocalInit, PatIdent, Result};

mod schema;

use schema::{db_schema, placeholder_len, query_schema, query_table_names, Column, Schema, Table};

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

// Use those table, column names/types for static typing as well
// as fn generation and struct generation
fn sql_macro(exprs: Vec<SqlExpr>) -> Result<TokenStream> {
    // we need to find the one expr that has all ddl statements
    // treat this as the migrate fn
    // this also grabs the db schema
    let (db_schema, migrate_fn, migrate_expr) = migrate_expr(&exprs)?;

    let structs = struct_tokens(&db_schema, migrate_expr)?;

    let fns = exprs
        .iter()
        .map(|expr| {
            let span = expr.ident.span();
            let schema = query_schema(span, &expr.statements)?;
            let fn_tokens = fn_tokens(span, expr, &db_schema, &schema)?;
            Ok(fn_tokens)
        })
        .collect::<Result<Vec<_>>>()?;

    let output = quote! {
        #(#structs)*

        #(#fns)*

        #migrate_fn
    };

    Ok(output)
}

// This wraps the two fns that have to deal with the database schema

// db_schema goes through the migrate expr statements and comes up with
// a set of tables and columns to compares each query against for type checking

// migrate_fn generates the migrate fn which you'll have to call to migrate
// the db
fn migrate_expr<'a>(exprs: &'a Vec<SqlExpr>) -> Result<(Schema<'a>, TokenStream, &'a SqlExpr)> {
    match exprs.iter().find(|expr| is_ddl(expr)) {
        Some(migrate_expr) => {
            // first we need the tables and columns defined across all
            // exprs (but probably just the migrate fn)
            let db_schema = db_schema(migrate_expr)?;
            let migrate_fn = migrate_fn(migrate_expr);

            Ok((db_schema, migrate_fn, migrate_expr))
        }
        None => {
            return Err(syn::Error::new(
                Span::call_site(),
                r#"You need a migration fn. Try this:
              let migrate = r\#"create table YourTable (id integer primary key);"\#;
                            "#,
            ))
        }
    }
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
            let _k = sqlite.call(move |conn| {
                let sp = static_sqlite::savepoint(conn, "migrate")?;
                let _k = static_sqlite::sync::execute_all(&sp, "create table if not exists __migrations__ (sql text primary key not null);");
                for stmt in sql.split(";").filter(|s| !s.trim().is_empty()) {
                    let mig: String = stmt.chars().filter(|c| !c.is_whitespace()).collect();
                    let changed = static_sqlite::sync::execute(&sp, "insert into __migrations__ (sql) values (?) on conflict (sql) do nothing", vec![static_sqlite::Value::Text(mig)])?;
                    if changed != 0 {
                        let _k = static_sqlite::sync::execute_all(&sp, stmt)?;
                    }
                }
                Ok(())
            }).await?;
            return Ok(());
        }
    }
}

/// Validates a query schema against the db schema
///
/// Makes sure all tables and columns in the query_schema
/// exist in the db_schema
fn validate_query(
    db_schema: &Schema<'_>,
    query_schema: &Schema<'_>,
    span: Span,
) -> Option<TokenStream> {
    let tokens = query_schema
        .0
        .iter()
        .filter_map(|(table, query_columns)| match db_schema.0.get(table) {
            Some(columns) => {
                let extra_columns = query_columns
                    .iter()
                    .filter(|qc| {
                        !columns
                            .iter()
                            .map(|col| col.name)
                            .collect::<Vec<_>>()
                            .contains(&qc.name)
                    })
                    .collect::<Vec<_>>();

                if !extra_columns.is_empty() {
                    Some(
                        Error::new(
                            span,
                            format!(
                                r#"Column {} doesn't exist"#,
                                extra_columns
                                    .iter()
                                    .map(|col| col.name.to_string())
                                    .collect::<Vec<_>>()
                                    .join("\n"),
                            ),
                        )
                        .to_compile_error(),
                    )
                } else {
                    None
                }
            }
            None => Some(
                Error::new(span, format!("Table {} doesn't exist", table.0)).to_compile_error(),
            ),
        })
        .collect::<Vec<_>>();

    match tokens.is_empty() {
        true => None,
        false => Some(quote! { #(#tokens)* }),
    }
}

fn fn_tokens(
    span: Span,
    expr: &SqlExpr,
    db_schema: &Schema<'_>,
    query_schema: &Schema<'_>,
) -> Result<TokenStream> {
    let SqlExpr {
        ident,
        sql,
        statements,
    } = expr;
    let validated_tokens = validate_query(db_schema, query_schema, span);
    match validated_tokens {
        Some(tokens) => return Ok(tokens),
        None => {}
    };
    let span = ident.span();
    let statement = statements
        .last()
        .ok_or(Error::new(span, "Need at least one sql statement"))?;
    let placeholder_len = placeholder_len(&statement);
    let placeholder_cols = placeholder_columns(db_schema, query_schema);
    match validate_placeholders(span, &placeholder_cols, placeholder_len) {
        Some(tokens) => return Ok(tokens),
        None => {}
    };
    if !is_ddl(expr) {
        let fn_args = fn_arg_tokens(span, &placeholder_cols);
        let params = param_tokens(span, &placeholder_cols);
        let (return_stmt, return_ty) = return_tokens(statement);
        let generic = generic_token(statement);

        Ok(quote! {
            #[doc = #sql]
            pub async fn #ident(sqlite: &static_sqlite::Sqlite, #(#fn_args),*) -> static_sqlite::Result<#return_ty> {
                let sql = #sql.to_string();
                let rows: Vec<#generic> = sqlite.call(move |conn| conn.query(#sql, #params)).await?;

                #return_stmt
            }
        })
    } else {
        Ok(quote! {})
    }
}

fn validate_placeholders(
    span: Span,
    placeholder_cols: &[&Column<'_>],
    placeholder_len: usize,
) -> Option<TokenStream> {
    if placeholder_cols.len() != placeholder_len {
        return Some(
            Error::new(
                span,
                format!(
                    "{} {} != {} {}",
                    placeholder_len,
                    match placeholder_len {
                        1 => "placeholder",
                        _ => "placeholders",
                    },
                    placeholder_cols.len(),
                    match placeholder_cols.len() {
                        1 => "column",
                        _ => "columns",
                    },
                ),
            )
            .to_compile_error(),
        );
    } else {
        None
    }
}

fn generic_token(stmt: &Statement) -> TokenStream {
    statement_table(stmt)
}

fn placeholder_columns<'a>(
    db_schema: &'a Schema<'_>,
    schema: &'a Schema<'_>,
) -> Vec<&'a Column<'a>> {
    schema
        .0
        .iter()
        .flat_map(|(table, columns)| {
            let db_columns = match db_schema.0.get(table) {
                Some(db_columns) => db_columns,
                None => todo!(),
            };
            columns
                .iter()
                .filter_map(
                    |col| match db_columns.iter().find(|db_col| db_col.name == col.name) {
                        Some(db_col) => match col.placeholder {
                            Some(_) => Some(db_col),
                            None => None,
                        },
                        None => None,
                    },
                )
                .collect::<Vec<_>>()
        })
        .collect()
}

fn fn_arg_tokens<'a>(span: Span, placeholders: &'a Vec<&'a Column<'a>>) -> Vec<TokenStream> {
    placeholders
        .iter()
        .map(|Column { name, def, .. }| {
            let name = syn::Ident::new(&name.to_string(), span);
            let not_null = match def {
                Some(def) => def.options.iter().any(|opt| match &opt.option {
                    sqlparser::ast::ColumnOption::NotNull => true,
                    sqlparser::ast::ColumnOption::Unique { is_primary, .. } => *is_primary,
                    _ => false,
                }),
                None => false,
            };
            let field_type = match def {
                Some(ColumnDef { data_type, .. }) => match data_type {
                    DataType::Blob(_) => quote! { Vec<u8> },
                    DataType::Integer(_) => quote! { i64 },
                    DataType::Real => quote! { f64 },
                    DataType::Text => {
                        quote! { impl ToString + Send + Sync + 'static }
                    }
                    _ => todo!("fn_args insert statement data_type"),
                },
                None => todo!("fn_args insert statement data_type no column def"),
            };

            match not_null {
                true => quote! { #name: #field_type },
                false => quote! { #name: Option<#field_type> },
            }
        })
        .collect()
}

fn struct_tokens(schema: &Schema<'_>, expr: &SqlExpr) -> Result<Vec<TokenStream>> {
    let span = expr.ident.span();

    schema.0.iter().map(|(table, columns)| {
        let column_defs = columns.iter().filter_map(|col| col.def).collect::<Vec<_>>();
        let ident = struct_ident(span, table);
        let fields = struct_fields(span, &column_defs);
        let match_stmt = match_tokens(span, &column_defs);

        Ok(match ident {
            Some(ident) => quote! {
                #[derive(Default, Debug, Clone, PartialEq)]
                pub struct #ident { #(#fields,)* }

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
            },
            None => quote! {},
        })
    }).collect()
}

fn struct_ident(span: Span, Table(ObjectName(parts)): &Table<'_>) -> Option<Ident> {
    let ident = match parts.as_slice() {
        [_, table, _] => Some(table),
        [table, _] => Some(table),
        [table] => Some(table),
        _ => None,
    };

    match ident {
        Some(ident) => Some(Ident::new(&ident.to_string(), span)),
        None => None,
    }
}

fn struct_fields(span: Span, columns: &Vec<&ColumnDef>) -> Vec<TokenStream> {
    columns
        .iter()
        .map(|def| {
            let not_null = def.options.iter().any(|opt| match &opt.option {
                sqlparser::ast::ColumnOption::NotNull => true,
                sqlparser::ast::ColumnOption::Unique { is_primary, .. } => *is_primary,
                _ => false,
            });
            let field_type = match def.data_type {
                DataType::Blob(_) => quote! { Vec<u8> },
                DataType::Integer(_) => quote! { i64 },
                DataType::Real | DataType::Double => quote! { f64 },
                DataType::Text => quote! { String },
                _ => todo!("struct_fields insert statement data_type"),
            };
            let name = Ident::new(&def.name.to_string(), span);

            match not_null {
                true => quote! { #name: #field_type },
                false => quote! { #name: Option<#field_type> },
            }
        })
        .collect()
}

fn match_tokens(span: Span, columns: &Vec<&ColumnDef>) -> Vec<TokenStream> {
    columns
        .iter()
        .map(|def| {
            let lit_str = &def.name.to_string();
            let ident = Ident::new(lit_str, span);

            quote! {
                #lit_str => row.#ident = value.try_into()?
            }
        })
        .collect()
}

fn return_tokens(stmt: &Statement) -> (TokenStream, TokenStream) {
    let return_ty = return_ty(stmt);
    let return_stmt = return_stmt(stmt);

    (return_stmt, return_ty)
}

fn return_stmt(stmt: &Statement) -> TokenStream {
    let single_row = quote! {
        match rows.into_iter().nth(0) {
            Some(row) => Ok(row),
            None => Err(static_sqlite::Error::RowNotFound)
        }
    };
    let nothing = quote! {Ok(())};

    let rows = quote! {
    Ok(rows)};
    match stmt {
        Statement::Insert { returning, .. }
        | Statement::Update { returning, .. }
        | Statement::Delete { returning, .. } => match returning {
            Some(_) => single_row,
            None => nothing,
        },
        Statement::Query(query) => {
            let limit = match query.limit.as_ref() {
                Some(expr) => match expr {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(limit, _)) => {
                        limit.parse().ok()
                    }
                    _ => None,
                },
                None => None,
            };

            match limit {
                Some(1) => single_row,
                Some(_) | None => rows,
            }
        }
        _ => todo!("fn return_stmt"),
    }
}

fn return_ty(stmt: &Statement) -> TokenStream {
    match stmt {
        Statement::Insert {
            table_name: ObjectName(parts),
            returning,
            ..
        } => return_ty_from_returning_or_parts(returning, parts),
        Statement::Update {
            table: TableWithJoins { relation, .. },
            returning,
            ..
        } => match relation {
            TableFactor::Table {
                name: ObjectName(parts),
                ..
            } => return_ty_from_returning_or_parts(returning, parts),
            _ => todo!("return_ty update relation TableFactor"),
        },
        Statement::Delete {
            from, returning, ..
        } => match from.first() {
            Some(TableWithJoins {
                relation:
                    TableFactor::Table {
                        name: ObjectName(parts),
                        ..
                    },
                ..
            }) => return_ty_from_returning_or_parts(returning, parts),
            _ => todo!("return_ty delete without a from"),
        },
        Statement::Query(query) => {
            let tables = query_table_names(query);
            match tables.first() {
                Some(ObjectName(parts)) => {
                    let ident = parts_ident(parts);
                    let limit = match query.limit.as_ref() {
                        Some(expr) => match expr {
                            sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                                limit,
                                _,
                            )) => limit.parse().ok(),
                            _ => None,
                        },
                        None => None,
                    };

                    match limit {
                        Some(1) => ident,
                        Some(_) | None => quote! { Vec<#ident> },
                    }
                }
                None => todo!("fn return_ty mutliple from tables"),
            }
        }
        statement => todo!("return_ty other statements {}", statement),
    }
}

fn return_ty_from_returning_or_parts(
    returning: &Option<Vec<SelectItem>>,
    fn_parts: &Vec<sqlparser::ast::Ident>,
) -> TokenStream {
    let parts = match returning {
        Some(cols) => match cols.as_slice() {
            [SelectItem::QualifiedWildcard(ObjectName(parts), _)] => Some(parts),
            [SelectItem::Wildcard(_)] => Some(fn_parts),
            _ => todo!("return_ty insert statement"),
        },
        None => None,
    };

    match parts {
        Some(parts) => parts_ident(parts),
        None => quote! { () },
    }
}

fn parts_ident(parts: &Vec<sqlparser::ast::Ident>) -> TokenStream {
    let ident = match parts.as_slice() {
        [_schema, table, _] => Some(table),
        [table, _] => Some(table),
        [table] => Some(table),
        _ => None,
    };

    match ident {
        Some(ident) => {
            let ident = Ident::new(&ident.to_string(), Span::call_site());
            quote! { #ident }
        }
        None => quote! { () },
    }
}

fn statement_table(stmt: &Statement) -> TokenStream {
    match stmt {
        Statement::Insert {
            table_name: ObjectName(parts),
            returning,
            ..
        } => return_ty_from_returning_or_parts(returning, parts),
        Statement::Update {
            table: TableWithJoins { relation, .. },
            returning,
            ..
        } => match relation {
            TableFactor::Table {
                name: ObjectName(parts),
                ..
            } => return_ty_from_returning_or_parts(returning, parts),
            _ => todo!("return_ty update relation TableFactor"),
        },
        Statement::Delete {
            from, returning, ..
        } => match from.first() {
            Some(TableWithJoins {
                relation:
                    TableFactor::Table {
                        name: ObjectName(parts),
                        ..
                    },
                ..
            }) => return_ty_from_returning_or_parts(returning, parts),
            _ => todo!("return_ty delete without a from"),
        },
        Statement::Query(query) => {
            let tables = query_table_names(query);
            match tables.first() {
                Some(ObjectName(parts)) => parts_ident(parts),
                None => todo!("multiple table names in statement table"),
            }
        }
        stmt => todo!("statement table other statements {}", stmt),
    }
}

fn param_tokens<'a>(span: Span, placeholders: &'a Vec<&'a Column<'a>>) -> TokenStream {
    let tokens: Vec<TokenStream> = placeholders
        .iter()
        .map(|Column { name, def, .. }| {
            let name = Ident::new(&name.to_string(), span);
            let def = match def {
                Some(def) => def,
                None => todo!("params col without def"),
            };
            let not_null = def.options.iter().any(|opt| match &opt.option {
                sqlparser::ast::ColumnOption::NotNull => true,
                sqlparser::ast::ColumnOption::Unique { is_primary, .. } => *is_primary,
                _ => false,
            });
            match &def.data_type {
                DataType::Blob(_) => {
                    quote! { #name.into() }
                }
                DataType::Integer(_x) => quote! { #name.into() },
                DataType::Real | DataType::Double => quote! { #name.into() },
                DataType::Text => match not_null {
                    true => quote! {
                        #name.to_string().into()
                    },
                    false => quote! {
                        match #name {
                            Some(val) => val.to_string().into(),
                            None => static_sqlite::Value::Null
                        }
                    },
                },
                _ => todo!("fn_args insert statement data_type"),
            }
        })
        .collect();

    quote! { &[#(#tokens,)*] }
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
