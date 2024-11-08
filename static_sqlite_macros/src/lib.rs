use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use sqlparser::ast::{
    self, Assignment, ColumnDef, DataType, ObjectName, SelectItem, TableFactor, TableWithJoins,
};
use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};
use syn::{parse_macro_input, Error, LocalInit, PatIdent, Result};

#[proc_macro]
pub fn sql(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let SqlExprs(sql_exprs) = parse_macro_input!(input as SqlExprs);
    match sql_macro(sql_exprs) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// Returns the generated rust fns and structs from a vec of sql exprs

// This keeps track of a few different bits of state from each expr and from
// across exprs for schema wide static type checking

// More specifically, this has two stages:

// Take all SqlExprs and grab the tables and
// column names/types from any create table / alter table statements
// alter table statements are evaluated in order from top to bottom
// to infer the schema.

fn migration_data(
    exprs: &Vec<SqlExpr>,
) -> Result<(TokenStream, Vec<TokenStream>, Vec<schema::Column>)> {
    // find the expr that has all ddl statements if there is one
    let migration_expr = exprs.iter().find(|ex| is_ddl(ex));
    let migrate_fn = match migration_expr {
        Some(SqlExpr { ident, sql, .. }) => {
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
        None => quote! {},
    };
    let columns = schema::columns(&exprs);
    let structs = match migration_expr {
        Some(ex) => struct_tokens(&columns, ex),
        None => {
            return Err(syn::Error::new(
                Span::call_site(),
                r#"You need a migration fn. Try this:
  let migrate = r\#"create table YourTable (id integer primary key);"\#;
                "#,
            ))
        }
    }?;

    Ok((migrate_fn, structs, columns))
}

// Use those table, column names/types for static typing as well
// as fn generation and struct generation
fn sql_macro(exprs: Vec<SqlExpr>) -> Result<TokenStream> {
    // we need to find the one expr that has all ddl statements
    // treat this as the migration fn
    // this fn also returns the columns used for struct fields and args
    // in the other in other fns
    let (migrate_fn, structs, columns) = migration_data(&exprs)?;

    let fns = exprs
        .iter()
        .filter(|expr| !is_ddl(expr))
        .map(|ex| fn_tokens(&columns, ex))
        .collect::<Result<Vec<_>>>()?;

    let output = quote! {
        #(#structs)*

        #(#fns)*

        #migrate_fn
    };

    Ok(output)
}

fn last_statment(expr: &SqlExpr) -> Result<&Statement> {
    match expr.statements.last() {
        Some(stmt) => Ok(stmt),
        None => {
            return Err(Error::new(
                expr.ident.span(),
                "Try adding at least one sql statement",
            ))
        }
    }
}

fn fn_tokens(schema_columns: &Vec<schema::Column>, expr: &SqlExpr) -> Result<TokenStream> {
    let statement = last_statment(&expr)?;
    let sql = &expr.sql;
    let ident = &expr.ident;
    let span = expr.ident.span();
    let columns = stmt_columns(span, schema_columns, statement)?;

    let tokens = match &statement {
        Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
            let generic = match fn_generic(&statement) {
                Some(generic) => quote! { #generic },
                None => quote! { () },
            };
            let return_ty = return_ty(&statement);
            let fn_args = fn_args(&columns);
            let params = params(&columns);

            quote! {
                #[doc = #sql]
                pub async fn #ident(sqlite: &static_sqlite::Sqlite, #(#fn_args),*) -> Result<#return_ty> {
                    let sql = #sql.to_string();
                    let rows: Vec<#generic> = sqlite.call(move |conn| conn.query(#sql, #params)).await?;

                    match rows.into_iter().nth(0) {
                        Some(row) => Ok(row),
                        None => Err(static_sqlite::Error::RowNotFound)
                    }
                }
            }
        }
        Statement::CreateTable { .. } | Statement::AlterTable { .. } => quote! {},
        _ => todo!("fn_tokens statement not defined yet"),
    };

    Ok(tokens)
}

fn struct_tokens(schema_columns: &Vec<schema::Column>, expr: &SqlExpr) -> Result<Vec<TokenStream>> {
    let tokens = expr.statements.iter().map(|stmt| {
        let ident = struct_ident(expr.ident.span(), stmt);
        let fields = struct_fields(schema_columns, stmt);
        let match_stmt = match_tokens(expr.ident.span(), schema_columns, stmt)?;

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
    }).collect::<Result<Vec<TokenStream>>>()?;

    Ok(tokens)
}

fn struct_ident(span: Span, stmt: &sqlparser::ast::Statement) -> Option<Ident> {
    match stmt {
        Statement::CreateTable {
            name: ObjectName(parts),
            ..
        } => match parts.last() {
            Some(ident) => Some(Ident::new(&ident.to_string(), span)),
            None => None,
        },
        _ => None,
    }
}

fn table_name(stmt: &sqlparser::ast::Statement) -> Option<&ObjectName> {
    match stmt {
        Statement::CreateTable { name, .. } => Some(name),
        Statement::AlterTable { name, .. } => Some(name),
        _ => None,
    }
}

fn fn_generic(stmt: &sqlparser::ast::Statement) -> Option<Ident> {
    match stmt {
        Statement::CreateTable { .. } => None,
        Statement::Insert {
            returning,
            table_name: ObjectName(parts),
            ..
        } => match returning {
            Some(cols) => match &cols[..] {
                [SelectItem::QualifiedWildcard(ObjectName(parts), _)] => ident_from(parts.last()),
                [SelectItem::Wildcard(_)] => ident_from(parts.last()),
                _ => todo!("fn_generic insert statement returning"),
            },
            None => None,
        },
        Statement::Update {
            returning, table, ..
        } => {
            let table_name = match &table.relation {
                TableFactor::Table {
                    name: ObjectName(parts),
                    ..
                } => ident_from(parts.last()),
                _ => todo!("fn_generic update table not implemented yet"),
            };
            match returning {
                Some(cols) => match &cols[..] {
                    [SelectItem::QualifiedWildcard(ObjectName(parts), _)] => {
                        ident_from(parts.last())
                    }
                    [SelectItem::Wildcard(_)] => table_name,
                    _ => todo!("fn_generic insert statement returning"),
                },
                None => None,
            }
        }
        Statement::Delete {
            returning, from, ..
        } => {
            let table_name = match from.first() {
                Some(TableWithJoins {
                    relation:
                        TableFactor::Table {
                            name: ObjectName(parts),
                            ..
                        },
                    ..
                }) => ident_from(parts.last()),
                _ => todo!("fn_generic update table not implemented yet"),
            };
            match returning {
                Some(cols) => match &cols[..] {
                    [SelectItem::QualifiedWildcard(ObjectName(parts), _)] => {
                        ident_from(parts.last())
                    }
                    [SelectItem::Wildcard(_)] => table_name,
                    _ => todo!("fn_generic insert statement returning"),
                },
                None => None,
            }
        }
        _ => todo!("fn_generic other statements"),
    }
}

fn ident_from(part: Option<&sqlparser::ast::Ident>) -> Option<Ident> {
    match part {
        Some(ident) => Some(Ident::new(&ident.value, Span::call_site())),
        None => None,
    }
}

fn struct_fields<'a>(
    schema_columns: &Vec<schema::Column>,
    stmt: &'a sqlparser::ast::Statement,
) -> Vec<TokenStream> {
    let table_name = table_name(&stmt);
    match table_name {
        Some(name) => schema_columns
            .iter()
            .filter(|col| col.table == *name)
            .map(|col| {
                let not_null = match &col.def {
                    Some(def) => def.options.iter().any(|opt| match &opt.option {
                        sqlparser::ast::ColumnOption::NotNull => true,
                        sqlparser::ast::ColumnOption::Unique { is_primary, .. } => *is_primary,
                        _ => false,
                    }),
                    None => false,
                };
                let field_type = match &col.def {
                    Some(ColumnDef { data_type, .. }) => match data_type {
                        DataType::Blob(_) => quote! { Vec<u8> },
                        DataType::Integer(_) => quote! { i64 },
                        DataType::Real | DataType::Double => quote! { f64 },
                        DataType::Text => quote! { String },
                        _ => todo!("struct_fields insert statement data_type"),
                    },
                    None => todo!("struct_fields what"),
                };
                let name = Ident::new(&col.name, Span::call_site());

                match not_null {
                    true => quote! { #name: #field_type },
                    false => quote! { #name: Option<#field_type> },
                }
            })
            .collect(),
        None => todo!("struct fields without a table?"),
    }
}

fn match_tokens(
    span: Span,
    schema_columns: &Vec<schema::Column>,
    stmt: &sqlparser::ast::Statement,
) -> Result<Vec<TokenStream>> {
    let Some(table_name) = table_name(stmt) else {
        return Err(Error::new(span, "Table does not exist"));
    };

    let tokens = schema_columns
        .iter()
        .filter(|col| &col.table == table_name)
        .map(|col| {
            let def = match &col.def {
                Some(def) => def,
                None => todo!("match_tokens column without a definition"),
            };

            let lit_str = &def.name.to_string();
            let ident = ident_from(Some(&def.name));

            quote! {
                #lit_str => row.#ident = value.try_into()?
            }
        })
        .collect();

    Ok(tokens)
}

fn return_ty(stmt: &sqlparser::ast::Statement) -> TokenStream {
    match stmt {
        Statement::CreateTable { .. } => quote! {
            ()
        },
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
        _ => todo!("return_ty other statements"),
    }
}

fn return_ty_from_returning_or_parts(
    returning: &Option<Vec<SelectItem>>,
    parts: &Vec<sqlparser::ast::Ident>,
) -> TokenStream {
    match returning {
        Some(cols) => match &cols[..] {
            [SelectItem::QualifiedWildcard(ObjectName(parts), _)] => {
                match ident_from(parts.last()) {
                    Some(ident) => quote! { #ident },
                    None => quote! { () },
                }
            }
            [SelectItem::Wildcard(_)] => match ident_from(parts.last()) {
                Some(ident) => quote! { #ident },
                None => quote! { () },
            },
            _ => todo!("return_ty insert statement"),
        },
        None => todo!("return_ty insert without returning"),
    }
}

fn fn_args(columns: &Vec<&schema::Column>) -> Vec<TokenStream> {
    columns
        .iter()
        .map(|schema::Column { name, def, .. }| {
            let name = Ident::new(name, Span::call_site());
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

fn stmt_columns<'a>(
    span: Span,
    schema_columns: &'a Vec<schema::Column>,
    stmt: &sqlparser::ast::Statement,
) -> Result<Vec<&'a schema::Column>> {
    match table_name(&stmt) {
        Some(table_name) => {
            match schema_columns.iter().any(|col| &col.table == table_name) {
                true => {}
                false => {
                    return Err(Error::new(
                        span,
                        format!("Table {} does not exist", table_name.to_string()),
                    ))
                }
            };
        }
        None => {}
    };
    match stmt {
        Statement::CreateTable { .. } => Ok(vec![]),
        Statement::Insert {
            table_name,
            columns,
            ..
        } => columns
            .iter()
            .map(|col| {
                let schema_col = schema_columns
                    .iter()
                    .find(|sc| &sc.name == &col.value && &sc.table == table_name);
                match schema_col {
                    Some(schema_col) => Ok(schema_col),
                    None => Err(Error::new(
                        span,
                        format!("Table {} does not have column {}", table_name, col),
                    )),
                }
            })
            .collect::<Result<Vec<_>>>(),
        Statement::Update {
            table:
                TableWithJoins {
                    relation:
                        TableFactor::Table {
                            name: table_name, ..
                        },
                    ..
                },
            assignments,
            selection,
            ..
        } => {
            let mut cols = assignments
                .iter()
                .filter_map(|Assignment { id, value }| match value {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Placeholder(_)) => Some(
                        id.iter()
                            .map(|id| id.value.clone())
                            .collect::<Vec<_>>()
                            .join("."),
                    ),
                    _ => todo!("schema_columns not a placeholder for update statement"),
                })
                .collect::<Vec<_>>();

            let cols2 = match selection {
                Some(expr) => extract_columns_from_binary_op(expr),
                None => vec![],
            };
            cols.extend(cols2);

            let out = cols
                .iter()
                .map(|col| {
                    let schema_col = schema_columns
                        .iter()
                        .find(|sc| &sc.name == col && &sc.table == table_name);
                    match schema_col {
                        Some(schema_col) => Ok(schema_col),
                        None => Err(Error::new(
                            span,
                            format!("Table {} does not have column {}", table_name, col),
                        )),
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(out)
        }
        Statement::Delete {
            from, selection, ..
        } => {
            let table = match from.first() {
                Some(TableWithJoins {
                    relation:
                        TableFactor::Table {
                            name: table_name, ..
                        },
                    ..
                }) => table_name,
                _ => {
                    return Err(Error::new(
                        span,
                        "delete statement requires a table in from clause",
                    ))
                }
            };

            let cols = match selection {
                Some(expr) => extract_columns_from_binary_op(expr),
                None => vec![],
            };

            cols.iter()
                .map(|col| {
                    let schema_col = schema_columns
                        .iter()
                        .find(|sc| &sc.name == col && &sc.table == table);
                    match schema_col {
                        Some(schema_col) => Ok(schema_col),
                        None => Err(Error::new(
                            span,
                            format!("Table {} does not have column {}", table, col),
                        )),
                    }
                })
                .collect::<Result<Vec<_>>>()
        }
        _ => todo!("columns from statement for other statements"),
    }
}

fn extract_columns_from_binary_op(expr: &sqlparser::ast::Expr) -> Vec<String> {
    let mut columns = Vec::new();
    let mut expr_stack = vec![expr];
    while let Some(current_expr) = expr_stack.pop() {
        match current_expr {
            ast::Expr::BinaryOp { left, op: _, right } => match (left.as_ref(), right.as_ref()) {
                (ast::Expr::Identifier(ident), ast::Expr::Value(ast::Value::Placeholder(val)))
                    if val == "?" =>
                {
                    columns.push(ident.value.clone())
                }
                (
                    ast::Expr::CompoundIdentifier(parts),
                    ast::Expr::Value(ast::Value::Placeholder(val)),
                ) if val == "?" => {
                    let column_name = parts
                        .iter()
                        .map(|p| p.value.clone())
                        .collect::<Vec<_>>()
                        .join(".");
                    columns.push(column_name);
                }
                (ast::Expr::BinaryOp { left, right, .. }, _) => {
                    expr_stack.push(left.as_ref());
                    expr_stack.push(right.as_ref());
                }
                _ => todo!("rest of the ops"),
            },
            _ => todo!("rest of the ops"),
        }
    }

    columns
}

fn params(columns: &Vec<&schema::Column>) -> TokenStream {
    let tokens: Vec<TokenStream> = columns
        .iter()
        .map(|schema::Column { name, def, .. }| {
            let name = Ident::new(name, Span::call_site());
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

mod schema {
    use crate::SqlExpr;
    use sqlparser::ast::{AlterTableOperation, ColumnDef, Ident, ObjectName, Statement};

    #[derive(Debug)]
    pub struct Column {
        pub table: ObjectName,
        pub name: String,
        pub def: Option<ColumnDef>,
    }

    pub fn column_from_def(table: ObjectName, name: Ident, def: Option<ColumnDef>) -> Column {
        Column {
            table,
            name: name.value.clone(),
            def,
        }
    }

    // pub fn statements<'a>(exprs: &'a Vec<SqlExpr>) -> Vec<&'a String> {
    //     exprs
    //         .iter()
    //         .flat_map(|ex| {
    //             ex.statements.iter().filter_map(|st| match st {
    //                 Statement::CreateTable { .. } => Some(&ex.sql),
    //                 Statement::AlterTable { .. } => Some(&ex.sql),
    //                 Statement::Drop { .. } => Some(&ex.sql),
    //                 _ => None,
    //             })
    //         })
    //         .collect()
    // }

    // get the columns from the statements
    // adding or deleting columns as create, alter or drop is
    // encountered
    pub fn columns(exprs: &Vec<SqlExpr>) -> Vec<Column> {
        let mut columns: Vec<Column> = vec![];
        let statements = exprs.into_iter().flat_map(|ex| &ex.statements);

        for statement in statements {
            match statement {
                Statement::CreateTable {
                    name,
                    columns: cols,
                    ..
                } => {
                    let mut cols = cols
                        .into_iter()
                        .map(|cd| column_from_def(name.clone(), cd.name.clone(), Some(cd.clone())))
                        .collect();
                    columns.append(&mut cols);
                }
                Statement::AlterTable {
                    name, operations, ..
                } => {
                    for op in operations {
                        match op {
                            AlterTableOperation::AddColumn { column_def, .. } => {
                                columns.push(column_from_def(
                                    name.clone(),
                                    column_def.name.clone(),
                                    Some(column_def.clone()),
                                ))
                            }
                            AlterTableOperation::DropColumn { column_name, .. } => {
                                columns = columns
                                    .into_iter()
                                    .filter(|c| &c.name != &column_name.value)
                                    .collect();
                            }
                            AlterTableOperation::RenameColumn {
                                old_column_name,
                                new_column_name,
                            } => {
                                let ix = columns
                                    .iter()
                                    .enumerate()
                                    .find(|(_ix, col)| &col.name == &old_column_name.value)
                                    .map(|(ix, _)| ix);
                                match ix {
                                    Some(ix) => match columns.get_mut(ix) {
                                        Some(col) => {
                                            col.name = new_column_name.value.clone();
                                        }
                                        None => {}
                                    },
                                    None => {}
                                }
                            }
                            AlterTableOperation::RenameTable { table_name } => {
                                columns = columns
                                    .into_iter()
                                    .map(|mut c| {
                                        if &c.table == name {
                                            c.table = table_name.clone();
                                            c
                                        } else {
                                            c
                                        }
                                    })
                                    .collect()
                            }
                            AlterTableOperation::ChangeColumn {
                                old_name,
                                new_name,
                                data_type,
                                options: _options,
                            } => {
                                let ix = columns
                                    .iter()
                                    .enumerate()
                                    .find(|(_ix, col)| &col.name == &old_name.value)
                                    .map(|(ix, _)| ix);
                                match ix {
                                    Some(ix) => match columns.get_mut(ix) {
                                        Some(col) => {
                                            col.name = new_name.value.clone();
                                            col.def = Some(ColumnDef {
                                                name: new_name.clone(),
                                                data_type: data_type.clone(),
                                                collation: None,
                                                options: vec![],
                                            })
                                        }
                                        None => {}
                                    },
                                    None => {}
                                }
                            }
                            sqlparser::ast::AlterTableOperation::AlterColumn { .. } => {
                                todo!("alter column")
                            }
                            _ => {}
                        }
                    }
                }
                Statement::Drop {
                    object_type, names, ..
                } => match object_type {
                    sqlparser::ast::ObjectType::Table => {
                        columns = columns
                            .into_iter()
                            .filter(|c| !names.contains(&c.table))
                            .collect();
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        columns
    }
}

// fn to_input(exprs: Vec<SqlExpr>) -> Input {
//     let defs = exprs
//         .iter()
//         .filter_map(to_statement_expr)
//         .collect::<Vec<_>>();
//     let columns = exprs.iter().map(columns).collect();

//     Input { defs, columns }
// }

// fn to_output(input: Input) -> Output {
//     let stmts = input
//         .defs
//         .into_iter()
//         .filter_map(|def| to_stmt(&input.columns, def))
//         .collect();
//     Output { stmts }
// }

// fn to_tokens(output: Output) -> TokenStream {
//     let create_tables: Vec<CreateTable> = output
//         .stmts
//         .iter()
//         .filter_map(|st| match st {
//             Stmt::CreateTable {
//                 table_name, cast, ..
//             } => Some(CreateTable {
//                 table_name: table_name.clone(),
//                 ident: cast.clone(),
//             }),
//             _ => None,
//         })
//         .collect();
//     let impls: Vec<TokenStream> = output
//         .stmts
//         .iter()
//         .map(|stmts| impl_tokens(&create_tables, stmts))
//         .collect();
//     let tokens: Vec<TokenStream> = output.stmts.into_iter().map(struct_tokens).collect();

//     quote! {
//         #(#tokens)*
//         #(#impls)*
//     }
// }

// fn impl_tokens(create_tables: &Vec<CreateTable>, output: &Stmt) -> TokenStream {
//     match output {
//         Stmt::ExecuteBatch { ident, sql } => quote! {
//             pub async fn #ident(db: static_sqlite::tokio::Sqlite) -> static_sqlite::Result<()> {
//                 let sql = #sql.to_string();
//                 db.call(|conn| conn.execute(#sql, vec![])).await;

//                 Ok(())
//             }

//         },
//         Stmt::Execute {
//             ident,
//             sql,
//             in_cols,
//         } => {
//             let fn_args: Vec<TokenStream> = in_cols.iter().map(fn_tokens).collect();
//             let param_fields: Vec<TokenStream> = in_cols.iter().map(param_tokens).collect();

//             quote! {
//                 pub async fn #ident(db: static_sqlite::tokio::Sqlite, #(#fn_args,)*) -> static_sqlite::Result<usize> {
//                     let sql = #sql.to_string();
//                     db.call(|conn| conn.execute(#sql, vec![#(#param_fields,)*])).await

//                     Ok(())
//                 }
//             }
//         }
//         Stmt::AggQuery {
//             ident,
//             sql,
//             in_cols,
//         } => {
//             let fn_args: Vec<TokenStream> = in_cols.iter().map(fn_tokens).collect();
//             let param_fields: Vec<TokenStream> = in_cols.iter().map(param_tokens).collect();

//             quote! {
//                  async fn #ident(db: static_sqlite::tokio::Sqlite, #(#fn_args,)*) -> static_sqlite::Result<i64> {
//                     let sql = #sql.to_string();
//                     let rows = db.call(|conn| conn.rows(#sql, vec![#(#param_fields,)*])).await?;
//                     let result: i64 = rows.nth(0).expect("count(*) expected").1.try_into()?;

//                     Ok(result)
//                 }
//             }
//         }
//         Stmt::Query {
//             ident,
//             sql,
//             in_cols,
//             from,
//             ret,
//             ..
//         } => {
//             let struct_ident = match from {
//                 FromClause::Table(string) => {
//                     if let Some(ct) = create_tables.iter().find(|ct| &ct.table_name == string) {
//                         ct.ident.clone()
//                     } else {
//                         panic!("this table doesn't exist");
//                     }
//                 }
//                 FromClause::Join(_) => struct_ident(&ident),
//                 FromClause::None => {
//                     todo!()
//                 }
//             };
//             let fn_args: Vec<TokenStream> = in_cols.iter().map(fn_tokens).collect();
//             let param_fields: Vec<TokenStream> = in_cols.iter().map(param_tokens).collect();
//             let (return_statement, return_type) = match ret {
//                 QueryReturn::Row => (
//                     quote! { Ok(rows.last().unwrap().clone()) },
//                     quote! { #struct_ident },
//                 ),
//                 QueryReturn::OptionRow => (
//                     quote! { Ok(rows.last().cloned()) },
//                     quote! { Option<#struct_ident> },
//                 ),
//                 QueryReturn::Rows => (quote! { Ok(rows) }, quote! { Vec<#struct_ident> }),
//             };
//             quote! {
//                 pub async fn #ident(db: &static_sqlite::tokio::Sqlite, #(#fn_args,)*) -> static_sqlite::Result<#return_type> {
//                     let sql = #sql.to_string();
//                     let rows: Vec<#struct_ident> = db.call(move |conn| conn.query(#sql, &[#(#param_fields,)*])).await?;
//                     #return_statement
//                 }
//             }
//         }
//         Stmt::CreateTable { sql, .. } => {
//             quote! {}
//         }
//         Stmt::CreateIndex { ident, sql } => {
//             quote! {
//                 pub fn #ident(db: &static_sqlite::sync::Sqlite) -> static_sqlite::Result<()> {
//                     let _ = db.execute(#sql, vec![]);

//                     Ok(())
//                 }
//             }
//         }
//         Stmt::AlterTable => quote! {},
//     }
// }

// fn struct_tokens(output: Stmt) -> TokenStream {
//     match output {
//         Stmt::ExecuteBatch { .. } | Stmt::Execute { .. } | Stmt::AggQuery { .. } => {
//             let token_stream = quote! {};
//             token_stream
//         }
//         Stmt::Query {
//             ident,
//             in_cols,
//             out_cols,
//             ..
//         } => {
//             quote! {}
//         }
//         Stmt::CreateTable { cast, cols, .. } => {
//             let struct_ident = cast.clone();
//             let struct_fields: Vec<TokenStream> = cols.iter().map(column_tokens).collect();
//             // let instance_fields: Vec<TokenStream> = cols.iter().map(row_tokens).collect();
//             let match_fields: Vec<TokenStream> = cols.iter().map(match_tokens).collect();
//             let name_struct_ident = Ident::new(
//                 &format!("{}Names", &struct_ident.to_string()),
//                 Span::call_site(),
//             );
//             let name_struct_fields: Vec<TokenStream> =
//                 cols.iter().map(name_struct_tokens).collect();
//             let name_struct_self_fields: Vec<TokenStream> =
//                 cols.iter().map(name_struct_self_tokens).collect();

//             let tokens = quote! {
//                 #[derive(Default, Debug, Clone, PartialEq)]
//                 pub struct #struct_ident {
//                     #(#struct_fields,)*
//                 }

//                 impl #struct_ident {
//                     pub fn names() -> #name_struct_ident {
//                         #name_struct_ident { #(#name_struct_self_fields,)* }
//                     }
//                 }

//                 pub struct #name_struct_ident { #(#name_struct_fields,)* }

//                 impl static_sqlite::FromRow for #struct_ident {
//                     fn from_row(columns: Vec<(String, static_sqlite::Value)>) -> static_sqlite::Result<Self> {
//                         let mut row = #struct_ident::default();
//                         for (column, value) in columns {
//                             match column.as_str() {
//                                 #(#match_fields,)*
//                                 _ => {}
//                             }
//                         }

//                         Ok(row)
//                     }
//                 }
//             };

//             tokens
//         }
//         Stmt::CreateIndex { .. } => {
//             quote! {}
//         }
//         Stmt::AlterTable => {
//             quote! {}
//         }
//     }
// }

// #[derive(Debug)]
// enum QueryReturn {
//     Row,
//     OptionRow,
//     Rows,
// }

// fn to_stmt(db_columns: &HashSet<Column>, sql_expr: SqlExpr) -> Option<Stmt> {
//     let SqlExpr {
//         ident,
//         sql,
//         statements,
//     } = sql_expr;
//     // last one is the only one that returns anything?
//     match statements.last() {
//         Some(stmt) => match stmt {
//             Statement::CreateTable { name, columns, .. } => {
//                 create_table_stmt(db_columns, name.to_string(), ident, sql, columns)
//             }
//             Statement::AlterTable {
//                 name, operations, ..
//             } => None,
//             Statement::CreateIndex { .. } => create_index_stmt(ident, sql),
//             Statement::Insert {
//                 table_name,
//                 columns,
//                 returning,
//                 source,
//                 on,
//                 ..
//             } => insert_stmt(
//                 db_columns,
//                 ident,
//                 sql,
//                 table_name.to_string(),
//                 columns,
//                 returning,
//                 source,
//                 on,
//             ),
//             Statement::Update {
//                 table,
//                 assignments,
//                 from,
//                 selection,
//                 returning,
//             } => {
//                 let fn_ident = match ident {
//                     Some(x) => x,
//                     None => panic!("update requires an fn ident"),
//                 };
//                 update_stmt(
//                     db_columns,
//                     fn_ident,
//                     sql,
//                     table,
//                     assignments,
//                     from,
//                     selection,
//                     returning,
//                 )
//             }
//             Statement::Delete {
//                 from,
//                 selection,
//                 returning,
//                 ..
//             } => {
//                 let fn_ident = match ident {
//                     Some(x) => x,
//                     None => panic!("delete requires an fn ident"),
//                 };
//                 delete_stmt(db_columns, fn_ident, sql, from, selection, returning)
//             }
//             Statement::Query(d) => {
//                 let Query { body, limit, .. } = &**d;

//                 let fn_ident = match ident {
//                     Some(x) => x,
//                     None => panic!("queries require an fn ident"),
//                 };
//                 query_stmt(db_columns, fn_ident, sql, body, limit.as_ref())
//             }
//             _ => {
//                 let fn_ident = match ident {
//                     Some(x) => x,
//                     None => panic!("insert requires an fn ident"),
//                 };
//                 Some(Stmt::ExecuteBatch {
//                     ident: fn_ident,
//                     sql,
//                 })
//             }
//         },
//         _ => None,
//     }
// }

// fn create_table_stmt(
//     db_columns: &Vec<schema::Column>,
//     table_name: String,
//     cast: Ident,
//     sql: String,
//     _columns: &Vec<sqlparser::ast::ColumnDef>,
// ) -> Option<Stmt> {
//     // let cols = columns
//     //     .iter()
//     //     .map(|c| column(Some(&table_name), c))
//     //     .collect::<Vec<_>>();
//     let cols = db_columns
//         .iter()
//         .map(|c| column(Some(&table_name), c))
//         .collect::<Vec<_>>();

//     Some(Stmt::CreateTable {
//         table_name,
//         sql,
//         cast,
//         cols,
//     })
// }

// fn create_index_stmt(ident: Ident, sql: String) -> Option<Stmt> {
//     Some(Stmt::CreateIndex { ident, sql })
// }

// fn query_stmt(
//     db_cols: &HashSet<Column>,
//     ident: Ident,
//     sql: String,
//     body: &SetExpr,
//     limit: Option<&sqlparser::ast::Expr>,
// ) -> Option<Stmt> {
//     let select = match body {
//         SetExpr::Select(select) => select,
//         SetExpr::Insert(Statement::Insert {
//             table_name,
//             columns,
//             returning,
//             source,
//             on,
//             ..
//         }) => {
//             return insert_stmt(
//                 db_cols,
//                 ident,
//                 sql,
//                 table_name.to_string(),
//                 columns,
//                 returning,
//                 source,
//                 on,
//             );
//         }
//         _ => {
//             return None;
//         }
//     };
//     let Select {
//         projection,
//         selection,
//         from,
//         ..
//     } = &**select;
//     // make sure the table in from matches something in db_cols
//     let schema_tables = db_cols
//         .iter()
//         .map(|col| &col.table_name)
//         .collect::<HashSet<_>>();
//     from.iter()
//         .map(|f| match &f.relation {
//             TableFactor::Table { name, .. } => name.to_string(),
//             _ => todo!("table"),
//         })
//         .for_each(|table| {
//             if !schema_tables.contains(&table) {
//                 panic!("{}: table name does not exist {}", ident, table);
//             }
//         });
//     let from: FromClause = from.into();
//     let in_cols = match selection {
//         Some(expr) => columns_from_expr(&db_cols, expr, None),
//         None => vec![],
//     };
//     let out_cols = projection
//         .iter()
//         .flat_map(|si| columns_from_select_item(&db_cols, si))
//         .collect::<Vec<_>>();
//     let ret = match limit {
//         Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(number, _))) => {
//             match number.as_str() {
//                 "1" => QueryReturn::OptionRow,
//                 _ => QueryReturn::Rows,
//             }
//         }
//         _ => QueryReturn::Rows,
//     };
//     // one column and it's count(*)
//     let ret = match out_cols[..] {
//         [Column {
//             ref column_type, ..
//         }] => match column_type {
//             ColumnType::Aggregate => {
//                 return Some(Stmt::AggQuery {
//                     ident,
//                     sql,
//                     in_cols,
//                 })
//             }
//             ColumnType::Column => ret,
//         },
//         _ => ret,
//     };

//     Some(Stmt::Query {
//         ident,
//         sql,
//         in_cols,
//         out_cols,
//         from,
//         ret,
//     })
// }

// fn update_stmt(
//     db_cols: &HashSet<Column>,
//     ident: Ident,
//     sql: String,
//     table: &TableWithJoins,
//     assignments: &[Assignment],
//     from: &Option<TableWithJoins>,
//     selection: &Option<sqlparser::ast::Expr>,
//     returning: &Option<Vec<SelectItem>>,
// ) -> Option<Stmt> {
//     let table_names = table_names(table);
//     let table_name = table_names.get(0);
//     let table_name = match table_name {
//         Some(t) => t,
//         None => panic!("update needs a table name in {}", &ident),
//     };

//     let table_columns = db_cols
//         .iter()
//         .filter(|c| &c.table_name == table_name)
//         .map(|c| c.clone())
//         .collect::<HashSet<_>>();
//     let out_cols = match returning {
//         Some(si) => si
//             .iter()
//             .flat_map(|si| columns_from_select_item(&table_columns, si))
//             .collect::<Vec<_>>(),
//         None => vec![],
//     };
//     let mut in_cols = assignments
//         .iter()
//         .filter_map(|a| match a.value {
//             sqlparser::ast::Expr::Value(sqlparser::ast::Value::Placeholder(_)) => Some(&a.id),
//             _ => None,
//         })
//         .flat_map(|c| columns_from_idents(&table_columns, c))
//         .collect::<Vec<_>>();
//     in_cols.extend(match selection {
//         Some(expr) => columns_from_expr(&table_columns, expr, None),
//         None => vec![],
//     });
//     let from: FromClause = match from {
//         Some(fr) => fr.into(),
//         None => FromClause::None,
//     };

//     match returning {
//         Some(_) => Some(Stmt::Query {
//             ident,
//             sql,
//             in_cols,
//             out_cols,
//             from,
//             ret: QueryReturn::Row,
//         }),
//         None => Some(Stmt::Execute {
//             ident,
//             sql,
//             in_cols,
//         }),
//     }
// }

// fn to_statement_expr(
//     SqlExpr {
//         ident, sql, cast, ..
//     }: &SqlExpr,
// ) -> Option<SqlExpr> {
//     let statements = match Parser::parse_sql(&SQLiteDialect {}, &sql) {
//         Ok(ast) => ast,
//         Err(err) => {
//             // TODO: better error handling
//             panic!("{}", err);
//         }
//     };

//     Some(SqlExpr {
//         ident: ident.clone(),
//         sql: sql.clone(),
//         statements,
//         cast: cast.clone(),
//     })
// }

// fn columns(SqlExpr { statements, .. }: &SqlExpr) -> HashSet<Column> {
//     statements
//         .iter()
//         .filter_map(|statement| match statement {
//             Statement::CreateTable { name, columns, .. } => {
//                 let name = name.to_string();
//                 let columns = columns
//                     .iter()
//                     .map(|c| column(Some(&name), c))
//                     .collect::<Vec<_>>();

//                 Some(columns)
//             }
//             Statement::AlterTable {
//                 name, operations, ..
//             } => {
//                 let name = name.to_string();
//                 let columns = operations
//                     .iter()
//                     .filter_map(|op| match op {
//                         AlterTableOperation::AddColumn { column_def, .. } => {
//                             Some(column(Some(&name), column_def))
//                         }
//                         _ => None,
//                     })
//                     .collect::<Vec<_>>();

//                 Some(columns)
//             }
//             _ => None,
//         })
//         .flat_map(|c| c)
//         .collect()
// }

// fn table_names(table: &TableWithJoins) -> Vec<String> {
//     let mut results = table_names_from(&table.relation);
//     results.extend(
//         table
//             .joins
//             .iter()
//             .flat_map(|j| table_names_from(&j.relation)),
//     );

//     results
// }

// fn table_names_from(relation: &TableFactor) -> Vec<String> {
//     match relation {
//         sqlparser::ast::TableFactor::Table { name, .. } => vec![name.to_string()],
//         sqlparser::ast::TableFactor::NestedJoin {
//             table_with_joins, ..
//         } => table_names(&table_with_joins),
//         _ => vec![],
//     }
// }

// fn struct_ident(ident: &Ident) -> Ident {
//     syn::Ident::new(&snake_to_pascal(ident.to_string()), ident.span())
// }

// fn insert_stmt(
//     db_cols: &HashSet<Column>,
//     ident: Ident,
//     sql: String,
//     table_name: String,
//     columns: &Vec<sqlparser::ast::Ident>,
//     returning: &Option<Vec<SelectItem>>,
//     source: &Option<Box<Query>>,
//     on: &Option<sqlparser::ast::OnInsert>,
// ) -> Option<Stmt> {
//     // nice little compile time validation
//     // check insert into count matches placeholder count
//     let placeholder_count = match source.as_deref() {
//         Some(Query { body, .. }) => match &**body {
//             SetExpr::Values(value) => Some(
//                 value
//                     .rows
//                     .iter()
//                     .flatten()
//                     .filter(|expr| match expr {
//                         sqlparser::ast::Expr::Value(sqlparser::ast::Value::Placeholder(_)) => true,
//                         _ => false,
//                     })
//                     .count(),
//             ),
//             _ => None,
//         },
//         None => None,
//     };
//     let input_col_names = columns.iter().map(|c| c.to_string()).collect::<Vec<_>>();
//     match placeholder_count {
//         Some(pc) => {
//             if pc != input_col_names.len() {
//                 panic!("{} placeholder count doesn't match insert into", ident);
//             }
//         }
//         None => {}
//     }

//     let table_columns = db_cols
//         .iter()
//         .filter(|c| c.table_name == table_name)
//         .map(|c| c.clone())
//         .collect::<HashSet<_>>();

//     // check insert into matches table columns
//     let table_column_names = table_columns
//         .iter()
//         .map(|c| c.name.clone())
//         .collect::<Vec<_>>();
//     for n in input_col_names {
//         if !table_column_names.contains(&n) {
//             panic!("column {} does not exist in table {}", n, table_name);
//         }
//     }

//     let in_cols: Vec<Column> = columns_from_idents(&table_columns, columns);

//     match returning {
//         Some(_) => {
//             let out_cols: Vec<Column> = match returning {
//                 Some(si) => si
//                     .iter()
//                     .flat_map(|si| columns_from_select_item(&table_columns, si))
//                     .collect(),
//                 None => vec![],
//             };

//             let ret = match on {
//                 Some(OnInsert::OnConflict(OnConflict {
//                     action: OnConflictAction::DoNothing,
//                     ..
//                 })) => QueryReturn::OptionRow,
//                 Some(_) | None => QueryReturn::Row,
//             };

//             let from = FromClause::Table(table_name);
//             Some(Stmt::Query {
//                 ident,
//                 sql,
//                 in_cols,
//                 out_cols,
//                 ret,
//                 from,
//             })
//         }
//         None => Some(Stmt::Execute {
//             ident,
//             sql,
//             in_cols,
//         }),
//     }
// }

// fn delete_stmt(
//     db_cols: &HashSet<Column>,
//     ident: Ident,
//     sql: String,
//     from: &Vec<TableWithJoins>,
//     selection: &Option<sqlparser::ast::Expr>,
//     returning: &Option<Vec<SelectItem>>,
// ) -> Option<Stmt> {
//     let table_names = from.iter().flat_map(|f| table_names(f)).collect::<Vec<_>>();
//     let table_name = match table_names.first() {
//         Some(t) => t.to_string(),
//         None => panic!("delete expects table name {}", &ident),
//     };
//     let table_columns = db_cols
//         .iter()
//         .filter(|c| c.table_name == table_name)
//         .map(|c| c.clone())
//         .collect::<HashSet<_>>();
//     let in_cols = match selection {
//         Some(expr) => columns_from_expr(&table_columns, expr, None),
//         None => vec![],
//     };
//     let out_cols = match returning {
//         Some(si) => si
//             .iter()
//             .flat_map(|si| columns_from_select_item(&table_columns, si))
//             .collect::<Vec<_>>(),
//         None => vec![],
//     };

//     match returning {
//         Some(_) => {
//             let from: FromClause = from.into();
//             Some(Stmt::Query {
//                 ident,
//                 sql,
//                 in_cols,
//                 out_cols,
//                 ret: QueryReturn::OptionRow,
//                 from,
//             })
//         }
//         _ => Some(Stmt::Execute {
//             ident,
//             sql,
//             in_cols,
//         }),
//     }
// }

// fn snake_to_pascal(input: String) -> String {
//     input
//         .split("_")
//         .filter(|x| !x.is_empty())
//         .map(|x| {
//             let mut chars = x.chars();
//             format!("{}{}", chars.nth(0).unwrap().to_uppercase(), chars.as_str())
//         })
//         .collect::<String>()
// }

// #[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
// enum ColumnType {
//     Aggregate,
//     #[default]
//     Column,
// }

// #[derive(Clone, Default, Debug, PartialEq, Eq, Hash)]
// struct Column {
//     name: String,
//     full_name: String,
//     table_name: String,
//     column_type: ColumnType,
//     data_type: DataType,
// }

// fn columns_from_idents(
//     table_columns: &HashSet<Column>,
//     column_names: &Vec<sqlparser::ast::Ident>,
// ) -> Vec<Column> {
//     column_names
//         .iter()
//         .filter_map(|ident| {
//             table_columns
//                 .iter()
//                 .find(|c| c.name == ident.value)
//                 .cloned()
//         })
//         .collect::<Vec<_>>()
// }

// fn columns_from_select_item(
//     table_columns: &HashSet<Column>,
//     select_item: &sqlparser::ast::SelectItem,
// ) -> HashSet<Column> {
//     match select_item {
//         sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
//             columns_from_expr(&table_columns, expr, None)
//                 .into_iter()
//                 .collect::<HashSet<_>>()
//         }
//         sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
//             columns_from_expr(&table_columns, expr, Some(alias))
//                 .into_iter()
//                 .collect::<HashSet<_>>()
//         }
//         sqlparser::ast::SelectItem::QualifiedWildcard(obj_name, _) => table_columns
//             .iter()
//             .filter(|c| c.table_name == obj_name.to_string())
//             .map(|c| c.clone())
//             .collect::<HashSet<_>>(),
//         sqlparser::ast::SelectItem::Wildcard(_) => table_columns.clone(),
//     }
// }

// fn in_columns_from_query(
//     table_columns: &HashSet<Column>,
//     query: &sqlparser::ast::Query,
// ) -> Vec<Column> {
//     let sqlparser::ast::Query { body, .. } = query;
//     match &**body {
//         SetExpr::Select(select) => {
//             let Select { selection, .. } = &**select;
//             match selection {
//                 Some(expr) => columns_from_expr(table_columns, expr, None),
//                 None => todo!(),
//             }
//         }
//         _ => todo!(),
//     }
// }

// fn columns_from_expr(
//     table_columns: &HashSet<Column>,
//     expr: &sqlparser::ast::Expr,
//     alias: Option<&sqlparser::ast::Ident>,
// ) -> Vec<Column> {
//     match expr {
//         sqlparser::ast::Expr::Identifier(ident) => {
//             match table_columns.iter().find(|c| c.name == ident.to_string()) {
//                 Some(c) => vec![c.clone()],
//                 None => panic!("column {} does not exist", ident.to_string()),
//             }
//         }
//         sqlparser::ast::Expr::CompoundIdentifier(idents) => {
//             let name = idents
//                 .into_iter()
//                 .map(|ident| ident.value.clone())
//                 .collect::<Vec<_>>()
//                 .join(".");
//             match table_columns.iter().find(|c| c.full_name == name) {
//                 Some(c) => vec![c.clone()],
//                 None => vec![],
//             }
//         }
//         sqlparser::ast::Expr::Wildcard => {
//             panic!("unqualified * not supported yet");
//             // table_columns.iter().map(|c| c.clone()).collect::<Vec<_>>()
//         }
//         sqlparser::ast::Expr::QualifiedWildcard(obj_name) => table_columns
//             .iter()
//             .filter(|c| c.table_name == obj_name.to_string())
//             .map(|c| c.clone())
//             .collect::<Vec<_>>(),
//         sqlparser::ast::Expr::BinaryOp { left, right, .. } => match (&**left, &**right) {
//             (
//                 sqlparser::ast::Expr::Identifier(_),
//                 sqlparser::ast::Expr::Value(sqlparser::ast::Value::Placeholder(token)),
//             )
//             | (
//                 sqlparser::ast::Expr::CompoundIdentifier(_),
//                 sqlparser::ast::Expr::Value(sqlparser::ast::Value::Placeholder(token)),
//             ) => match token.as_str() {
//                 "?" => columns_from_expr(&table_columns, left, None),
//                 _ => unimplemented!("? placeholders only please"),
//             },
//             (sqlparser::ast::Expr::BinaryOp { .. }, sqlparser::ast::Expr::BinaryOp { .. }) => vec![
//                 columns_from_expr(&table_columns, left, None),
//                 columns_from_expr(&table_columns, right, None),
//             ]
//             .into_iter()
//             .flatten()
//             .collect(),
//             (sqlparser::ast::Expr::BinaryOp { .. }, _) => {
//                 columns_from_expr(&table_columns, left, None)
//             }
//             (_, sqlparser::ast::Expr::BinaryOp { .. }) => {
//                 columns_from_expr(&table_columns, right, None)
//             }
//             _ => vec![],
//         },
//         sqlparser::ast::Expr::Value(_) => vec![],
//         sqlparser::ast::Expr::Function(sqlparser::ast::Function { name, args, .. }) => {
//             match name.to_string().as_str() {
//                 "count" => {
//                     let name = args
//                         .iter()
//                         .map(|fa| match fa {
//                             sqlparser::ast::FunctionArg::Unnamed(fa_expr) => match fa_expr {
//                                 sqlparser::ast::FunctionArgExpr::Expr(expr) => {
//                                     columns_from_expr(&table_columns, expr, None)
//                                         .iter()
//                                         .map(|c| c.full_name.clone())
//                                         .collect::<Vec<_>>()
//                                         .join("")
//                                 }
//                                 sqlparser::ast::FunctionArgExpr::QualifiedWildcard(table_name) => {
//                                     format!("{}({}.*)", name, table_name)
//                                 }
//                                 sqlparser::ast::FunctionArgExpr::Wildcard => name.to_string(),
//                             },
//                             _ => todo!("count"),
//                         })
//                         .collect::<Vec<_>>()
//                         .join("");
//                     let name = match alias {
//                         Some(name) => name.to_string(),
//                         None => name,
//                     };
//                     vec![Column {
//                         name: name.clone(),
//                         full_name: name,
//                         table_name: "".into(),
//                         data_type: DataType::Integer,
//                         column_type: ColumnType::Aggregate,
//                     }]
//                 }
//                 "strftime" => vec![],
//                 "unixepoch" => vec![],
//                 "substr" => {
//                     let name = match alias {
//                         Some(name) => name.to_string(),
//                         None => "substr".into(),
//                     };
//                     vec![Column {
//                         name: name.clone(),
//                         full_name: name,
//                         table_name: "".into(),
//                         data_type: DataType::Text,
//                         column_type: ColumnType::Column,
//                     }]
//                 }
//                 _ => todo!("what"),
//             }
//         }
//         sqlparser::ast::Expr::InSubquery { subquery, .. } => {
//             in_columns_from_query(table_columns, subquery)
//         }
//         _ => todo!("columns_from_expr"),
//     }
// }

// #[derive(Clone, Default, Debug, PartialEq, Eq, Hash)]
// enum DataType {
//     Integer,
//     Real,
//     Text,
//     #[default]
//     Blob,
//     Any,
//     Null(Box<DataType>),
// }

// fn full_column_name(table_name: Option<&String>, column_name: String) -> String {
//     match table_name {
//         Some(table_name) => format!("{}.{}", table_name, column_name),
//         None => column_name,
//     }
// }

// fn column(table_name: Option<&String>, value: &Column) -> Column {
//     let name = value.name.to_string();
//     let full_name = full_column_name(table_name, name.clone());
//     let inner_data_type = match &value.data_type {
//         sqlparser::ast::DataType::Blob(_) => DataType::Blob,
//         sqlparser::ast::DataType::Integer(_) => DataType::Integer,
//         sqlparser::ast::DataType::Int(_) => DataType::Integer,
//         sqlparser::ast::DataType::Real => DataType::Real,
//         sqlparser::ast::DataType::Text => DataType::Text,
//         sqlparser::ast::DataType::Varchar(_) => DataType::Text,
//         _ => DataType::Any,
//     };
//     let data_type = match not_null(&inner_data_type, &value.options) {
//         true => inner_data_type,
//         false => DataType::Null(inner_data_type.into()),
//     };
//     let table_name = table_name.unwrap_or(&"".into()).clone();

//     Column {
//         name,
//         full_name,
//         table_name,
//         data_type,
//         ..Default::default()
//     }
// }

// fn not_null(data_type: &DataType, value: &Vec<sqlparser::ast::ColumnOptionDef>) -> bool {
//     value.iter().any(|opt| match opt.option {
//         sqlparser::ast::ColumnOption::NotNull => true,
//         sqlparser::ast::ColumnOption::Unique { is_primary, .. } => {
//             is_primary && data_type == &DataType::Integer
//         }
//         _ => false,
//     })
// }

// fn data_type_tokens(data_type: &DataType) -> TokenStream {
//     match data_type {
//         DataType::Integer => quote!(i64),
//         DataType::Real => quote!(f64),
//         DataType::Text => quote!(String),
//         DataType::Blob => quote!(Vec<u8>),
//         DataType::Any => quote!(Vec<u8>),
//         DataType::Null(null) => {
//             let tokens = data_type_tokens(null);
//             quote!(Option<#tokens>)
//         }
//     }
// }

// fn column_tokens(column: &Column) -> TokenStream {
//     let data_type = data_type_tokens(&column.data_type);
//     let name = syn::Ident::new(&column.name, proc_macro2::Span::call_site());

//     quote!(#name: #data_type)
// }

// fn name_struct_tokens(column: &Column) -> TokenStream {
//     let name = syn::Ident::new(&column.name, proc_macro2::Span::call_site());

//     quote!(#name: &'static str)
// }

// fn name_struct_self_tokens(column: &Column) -> TokenStream {
//     let name = syn::Ident::new(&column.name, proc_macro2::Span::call_site());
//     let value = LitStr::new(&name.to_string(), Span::call_site());

//     quote!(#name: #value)
// }

// fn param_tokens(column: &Column) -> TokenStream {
//     let name = syn::Ident::new(&column.name, proc_macro2::Span::call_site());
//     match column.data_type {
//         DataType::Text => quote!(#name.to_string().into()),
//         DataType::Null(ref dt) => match &**dt {
//             DataType::Text => quote! {
//                 match #name {
//                     Some(val) => val.to_string().into(),
//                     None => static_sqlite::Value::Null
//                 }
//             },
//             _ => quote!(#name.into()),
//         },
//         _ => quote!(#name.into()),
//     }
// }

// fn match_tokens(column: &Column) -> TokenStream {
//     let lit_str = &column.name;
//     let ident = syn::Ident::new(&lit_str, proc_macro2::Span::call_site());

//     quote!(#lit_str => row.#ident = value.try_into()?)
// }

// fn fn_tokens(column: &Column) -> TokenStream {
//     let lit_str = &column.name;
//     let ident = syn::Ident::new(&lit_str, proc_macro2::Span::call_site());
//     let fn_type = fn_type(&column.data_type);

//     quote!(#ident: #fn_type)
// }

// fn fn_type(data_type: &DataType) -> TokenStream {
//     match data_type {
//         DataType::Integer => quote!(i64),
//         DataType::Real => quote!(f64),
//         DataType::Text => quote!(impl ToString + Send + Sync + 'static),
//         DataType::Blob | DataType::Any => quote!(Vec<u8>),
//         DataType::Null(dt) => {
//             let dt = fn_type(&*dt);
//             quote!(Option<#dt>)
//         }
//     }
// }

#[derive(Clone, Debug)]
struct SqlExpr {
    ident: Ident,
    sql: String,
    statements: Vec<Statement>,
}

// #[derive(Debug)]
// struct Input {
//     defs: Vec<SqlExpr>,
//     columns: Vec<Column>,
// }

// #[derive(Debug)]
// struct Output {
//     stmts: Vec<Stmt>,
// }

// #[derive(Debug)]
// enum Stmt {
//     ExecuteBatch {
//         ident: Ident,
//         sql: String,
//     },
//     Execute {
//         ident: Ident,
//         sql: String,
//         in_cols: Vec<Column>,
//     },
//     AggQuery {
//         ident: Ident,
//         sql: String,
//         in_cols: Vec<Column>,
//     },
//     Query {
//         ident: Ident,
//         sql: String,
//         in_cols: Vec<Column>,
//         out_cols: Vec<Column>,
//         from: FromClause,
//         ret: QueryReturn,
//     },
//     CreateIndex {
//         ident: Ident,
//         sql: String,
//     },
//     CreateTable {
//         table_name: String,
//         sql: String,
//         cast: Ident,
//         cols: Vec<Column>,
//     },
//     AlterTable,
// }

// #[derive(Debug)]
// enum FromClause {
//     Table(String),
//     #[allow(unused)]
//     Join(Vec<String>),
//     None,
// }

// impl From<&Vec<TableWithJoins>> for FromClause {
//     fn from(value: &Vec<TableWithJoins>) -> Self {
//         if value.len() == 1 {
//             let twj = value.iter().nth(0).unwrap();
//             Self::from(twj)
//         } else {
//             FromClause::Join(value.iter().flat_map(|twj| table_names(twj)).collect())
//         }
//     }
// }

// impl From<&TableWithJoins> for FromClause {
//     fn from(value: &TableWithJoins) -> Self {
//         let name = match &value.relation {
//             TableFactor::Table { name, .. } => name.to_string(),
//             _ => todo!("still working on more complex select queries"),
//         };
//         FromClause::Table(name)
//     }
// }

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
