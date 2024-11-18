use proc_macro2::Span;
use sqlparser::ast::{
    AlterTableOperation, ColumnDef, Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName,
    ObjectType, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use std::collections::HashMap;
use syn::{Error, Result};

use crate::SqlExpr;

#[derive(Debug, Eq, Hash, PartialEq, Clone, Copy)]
pub struct Table<'a>(pub &'a ObjectName);

#[derive(Debug, Eq, Hash, PartialEq, Clone, Copy)]
pub struct Column<'a> {
    pub name: &'a sqlparser::ast::Ident,
    pub def: Option<&'a ColumnDef>,
    pub placeholder: Option<&'a str>,
}

#[derive(Debug)]
pub struct Schema<'a>(pub HashMap<Table<'a>, Vec<Column<'a>>>);

pub fn db_schema<'a>(migrate_expr: &'a SqlExpr) -> Result<Schema<'a>> {
    let mut set: HashMap<Table<'_>, Vec<Column<'_>>> = HashMap::new();

    let SqlExpr {
        ident, statements, ..
    } = migrate_expr;
    let span = ident.span();
    let _result = statements.iter().try_for_each(|stmt| match stmt {
        Statement::CreateTable { name, columns, .. } => {
            let columns = columns
                .iter()
                .map(|col| Column {
                    name: &col.name,
                    def: Some(col),
                    placeholder: None,
                })
                .collect();
            set.insert(Table(name), columns);
            Ok(())
        }
        Statement::AlterTable {
            name, operations, ..
        } => {
            for op in operations {
                match op {
                    AlterTableOperation::AddColumn { column_def, .. } => {
                        match set.get_mut(&Table(name)) {
                            Some(columns) => columns.push(Column {
                                name: &column_def.name,
                                def: Some(column_def),
                                placeholder: None,
                            }),
                            None => todo!(),
                        };
                    }
                    AlterTableOperation::DropColumn { column_name, .. } => {
                        match set.get_mut(&Table(name)) {
                            Some(columns) => {
                                columns.retain(|col| col.name != column_name);
                            }
                            None => {}
                        }
                    }
                    AlterTableOperation::RenameColumn {
                        old_column_name,
                        new_column_name,
                    } => match set.get_mut(&Table(name)) {
                        Some(columns) => {
                            match columns.iter().position(|col| col.name == old_column_name) {
                                Some(ix) => {
                                    let mut col = columns.remove(ix);
                                    col.name = new_column_name;
                                    columns.push(col);
                                }
                                None => {}
                            };
                        }
                        None => {}
                    },
                    AlterTableOperation::RenameTable { table_name } => {
                        match set.remove(&Table(name)) {
                            Some(columns) => {
                                set.insert(Table(table_name), columns);
                            }
                            None => {}
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        Statement::Drop {
            object_type, names, ..
        } => {
            match object_type {
                ObjectType::Table => match names.first() {
                    Some(name) => {
                        set.remove(&Table(name));
                    }
                    None => {
                        return Err(Error::new(
                            span,
                            format!("drop statement {} requires a name", stmt.to_string()),
                        ))
                    }
                },
                _ => todo!(),
            }
            Ok(())
        }
        _ => Ok(()),
    })?;

    Ok(Schema(set))
}

pub fn query_schema<'a>(span: Span, statements: &'a Vec<Statement>) -> Result<Schema<'a>> {
    let mut set: HashMap<Table<'_>, Vec<Column<'_>>> = HashMap::new();

    statements.iter().try_for_each(|stmt| match stmt {
        Statement::Query(query) => {
            set_query_columns(query, span, &mut set)?;
            Ok(())
        }
        Statement::Insert {
            table_name,
            columns,
            returning,
            ..
        } => {
            let table = Table(table_name);
            let mut columns: Vec<Column<'a>> = columns
                .iter()
                .map(|name| Column {
                    name,
                    def: None,
                    placeholder: Some("?"),
                })
                .collect();
            let returning_columns = returning_columns(table, returning);
            columns.extend(returning_columns);
            set.insert(table, columns);
            Ok(())
        }
        Statement::Update {
            table,
            assignments,
            from: _from,
            selection,
            returning,
        } => {
            let table = match &table.relation {
                TableFactor::Table { name, .. } => Table(name),
                _ => todo!(),
            };
            let mut columns: Vec<_> = assignments
                .iter()
                .filter_map(|assign| {
                    let name = match assign.id.as_slice() {
                        [_schema, _table, column] => Some(column),
                        [_table, column] => Some(column),
                        [column] => Some(column),
                        _ => None,
                    };
                    match name {
                        Some(name) => match &assign.value {
                            Expr::Value(Value::Placeholder(val)) => Some(Column {
                                name,
                                def: None,
                                placeholder: Some(val.as_str()),
                            }),
                            _ => Some(Column {
                                name,
                                def: None,
                                placeholder: None,
                            }),
                        },
                        None => None,
                    }
                })
                .collect();
            let selection_columns = selection_columns(selection);
            columns.extend(selection_columns);
            let returning_columns = returning_columns(table, returning);
            columns.extend(returning_columns);
            set.insert(table, columns);
            Ok(())
        }
        Statement::Delete {
            from,
            selection,
            returning,
            ..
        } => {
            let table = match from.first() {
                None => {
                    return Err(syn::Error::new(
                        span,
                        "Delete statement requires at least one table",
                    ))
                }
                Some(table) => match &table.relation {
                    TableFactor::Table { name, .. } => Table(name),
                    _ => todo!(),
                },
            };
            let mut columns = selection_columns(selection);
            let ret_columns = returning_columns(table, returning);
            columns.extend(ret_columns);
            set.insert(table, columns);
            Ok(())
        }
        _ => todo!(),
    })?;

    Ok(Schema(set))
}

fn set_query_columns<'a>(
    query: &'a Query,
    span: Span,
    set: &mut HashMap<Table<'a>, Vec<Column<'a>>>,
) -> Result<()> {
    let Query {
        with: _with,
        body,
        order_by,
        ..
    } = query;
    match body.as_ref() {
        SetExpr::Select(select) => {
            let tables = from_tables(&select.from);
            let table = match tables.first() {
                Some(table) => *table,
                None => return Err(Error::new(span, "Only one table in from supported for now")),
            };
            let mut columns = selection_columns(&select.selection);
            let projection_columns = select_items_columns(table, select.projection.as_slice());
            columns.extend(projection_columns);
            let order_by_columns: Vec<Column<'_>> = order_by
                .iter()
                .flat_map(|ob| expr_columns(&ob.expr))
                .collect();
            columns.extend(order_by_columns);
            set.insert(table, columns.into_iter().collect());
        }
        SetExpr::Query(query) => set_query_columns(query, span, set)?,
        _ => todo!("fn set_query_columns"),
    }
    Ok(())
}

fn from_tables(from: &Vec<TableWithJoins>) -> Vec<Table<'_>> {
    from.iter()
        .flat_map(|table| {
            let mut tables = match &table.relation {
                TableFactor::Table { name, .. } => vec![Table(name)],
                _ => todo!("fn from_tables"),
            };
            let join_tables: Vec<_> = table
                .joins
                .iter()
                .map(|join| relation_table(&join.relation))
                .collect();
            tables.extend(join_tables);
            tables
        })
        .collect()
}

fn relation_table(relation: &TableFactor) -> Table<'_> {
    match relation {
        TableFactor::Table { name, .. } => Table(name),
        _ => todo!("fn relation_table: other TableFactors"),
    }
}

fn returning_columns<'a>(
    table: Table<'a>,
    returning: &'a Option<Vec<SelectItem>>,
) -> Vec<Column<'a>> {
    match returning {
        Some(select_items) => select_items_columns(table, select_items),
        None => vec![],
    }
}

fn select_items_columns<'a>(_table: Table<'a>, select_items: &'a [SelectItem]) -> Vec<Column<'a>> {
    select_items
        .iter()
        .filter_map(|si| match si {
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(name) => Some(Column {
                    name,
                    def: None,
                    placeholder: None,
                }),
                Expr::CompoundIdentifier(name) => compound_ident_column(name),
                _ => todo!("fn select_items_columns selectitem match"),
            },
            SelectItem::ExprWithAlias { expr, .. } => match expr {
                Expr::Identifier(name) => Some(Column {
                    name,
                    def: None,
                    placeholder: None,
                }),
                Expr::CompoundIdentifier(name) => compound_ident_column(name),
                expr => todo!("fn select_items_columns ExprWithAlias {expr}"),
            },
            _ => None,
        })
        .collect()
}

fn compound_ident_column<'a>(name: &'a Vec<Ident>) -> Option<Column<'a>> {
    let name = match name.as_slice() {
        [_schema, _table, name] => Some(name),
        [_table, name] => Some(name),
        [name] => Some(name),
        _ => None,
    };

    match name {
        Some(name) => Some(Column {
            name,
            def: None,
            placeholder: None,
        }),
        None => None,
    }
}

fn selection_columns<'a>(selection: &'a Option<Expr>) -> Vec<Column<'a>> {
    match selection {
        Some(expr) => expr_columns(expr),
        None => vec![],
    }
}

fn expr_columns<'a>(expr: &'a Expr) -> Vec<Column<'a>> {
    match expr {
        Expr::BinaryOp { left, op: _, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Identifier(name), Expr::Value(Value::Placeholder(val))) if val == "?" => {
                vec![Column {
                    name,
                    def: None,
                    placeholder: Some(val.as_str()),
                }]
            }
            (Expr::CompoundIdentifier(parts), Expr::Value(Value::Placeholder(val)))
                if val == "?" =>
            {
                match parts.as_slice() {
                    [_schema_name, _table_name, name] => {
                        vec![Column {
                            name,
                            def: None,
                            placeholder: Some(val.as_str()),
                        }]
                    }
                    [_table_name, name] => {
                        vec![Column {
                            name,
                            def: None,
                            placeholder: Some(val.as_str()),
                        }]
                    }
                    _ => unreachable!("one part compound identifier?!"),
                }
            }
            (Expr::BinaryOp { left, right, .. }, _) => {
                let mut cols = expr_columns(left);
                cols.extend(expr_columns(right));
                cols
            }
            _ => todo!("fn expr_columns: rest of the binary ops"),
        },
        Expr::CompoundIdentifier(parts) => {
            let name = match parts.as_slice() {
                [_schema, _table, name] => Some(name),
                [_table, name] => Some(name),
                [name] => Some(name),
                _ => None,
            };

            match name {
                Some(name) => vec![Column {
                    name,
                    def: None,
                    placeholder: None,
                }],
                None => vec![],
            }
        }
        Expr::Identifier(name) => vec![Column {
            name,
            def: None,
            placeholder: None,
        }],
        Expr::Nested(expr) => expr_columns(expr),
        Expr::Function(func) => func
            .args
            .iter()
            .flat_map(|arg| match arg {
                FunctionArg::Named { name: _name, arg } => match arg {
                    FunctionArgExpr::Expr(expr) => expr_columns(expr),
                    FunctionArgExpr::QualifiedWildcard(_object_name) => todo!(),
                    FunctionArgExpr::Wildcard => todo!(),
                },
                FunctionArg::Unnamed(function_arg_expr) => match function_arg_expr {
                    FunctionArgExpr::Expr(expr) => expr_columns(expr),
                    FunctionArgExpr::QualifiedWildcard(_object_name) => todo!(),
                    FunctionArgExpr::Wildcard => todo!(),
                },
            })
            .collect::<Vec<_>>(),
        Expr::Case {
            conditions,
            results,
            else_result,
            ..
        } => {
            let mut cols = conditions
                .iter()
                .flat_map(|expr| expr_columns(expr))
                .collect::<Vec<_>>();
            let results = results.iter().flat_map(|expr| expr_columns(expr));
            cols.extend(results);
            if let Some(else_expr) = else_result {
                let columns = expr_columns(else_expr.as_ref());
                cols.extend(columns);
            }
            cols
        }
        _ => todo!("expr_columns rest of the ops"),
    }
}

pub fn query_table_names(query: &Box<Query>) -> Vec<&ObjectName> {
    match query.body.as_ref() {
        SetExpr::Select(select) => select
            .from
            .iter()
            .map(|table| match &table.relation {
                TableFactor::Table { name, .. } => name,
                _ => todo!(),
            })
            .collect::<Vec<_>>(),
        SetExpr::Query(query) => query_table_names(query),
        _ => todo!("query_table_names"),
    }
}

pub fn placeholder_len(stmt: &Statement) -> usize {
    match stmt {
        Statement::Insert { source, .. } => match source {
            Some(query) => {
                let Query { body, .. } = query.as_ref();
                match body.as_ref() {
                    SetExpr::Values(values) => values
                        .rows
                        .iter()
                        .flat_map(|expr| expr)
                        .collect::<Vec<_>>()
                        .len(),
                    SetExpr::Select(select) => {
                        let Select { selection, .. } = select.as_ref();
                        match selection.as_ref() {
                            Some(expr) => expr_columns(expr)
                                .iter()
                                .filter(|col| col.placeholder.is_some())
                                .collect::<Vec<_>>()
                                .len(),
                            None => 0,
                        }
                    }
                    _ => todo!("fn placeholders"),
                }
            }
            None => 0,
        },
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            let len = assignments.len();
            match selection {
                Some(expr) => expr_columns(expr).len() + len,
                None => len,
            }
        }
        Statement::Delete { selection, .. } => match selection {
            Some(expr) => expr_columns(expr).len(),
            None => todo!(),
        },
        Statement::Query(query) => {
            let Query { body, .. } = query.as_ref();
            match body.as_ref() {
                SetExpr::Values(values) => values
                    .rows
                    .iter()
                    .flat_map(|expr| expr)
                    .collect::<Vec<_>>()
                    .len(),
                SetExpr::Select(select) => {
                    let Select { selection, .. } = select.as_ref();
                    dbg!(&selection);
                    match selection.as_ref() {
                        Some(expr) => expr_columns(expr)
                            .iter()
                            .filter(|col| col.placeholder.is_some())
                            .collect::<Vec<_>>()
                            .len(),
                        None => 0,
                    }
                }
                _ => todo!("fn placeholders"),
            }
        }
        _ => 0,
    }
}
