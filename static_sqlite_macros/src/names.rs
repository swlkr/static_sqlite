use crate::{
    errors::{Error, Result},
    SqlExpr,
};
use sqlparser::ast::{
    visit_expressions, visit_relations, visit_statements, BinaryOperator, ColumnOption,
    CreateTable, Expr, ObjectName, Statement, TableFactor, Visit, Visitor,
};
use std::{collections::HashMap, ops::ControlFlow};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Name {
    Table(Vec<String>),
    Col(Vec<String>),
}

#[allow(unused)]
pub fn names(statements: &Vec<Statement>) -> Vec<Name> {
    let mut visited = vec![];
    visit_relations(statements, |ObjectName(parts)| {
        visited.push(Name::Table(
            parts.iter().map(|ident| ident.value.clone()).collect(),
        ));
        ControlFlow::<()>::Continue(())
    });
    visit_expressions(statements, |expr| {
        let names = name_from_expr(expr);
        visited.extend(names);
        ControlFlow::<()>::Continue(())
    });
    visit_statements(statements, |statement| {
        match statement {
            Statement::CreateTable(CreateTable { name, columns, .. }) => {
                visited.extend(
                    columns
                        .iter()
                        .map(|def| {
                            Name::Col(vec![
                                name.0.last().unwrap().value.clone(),
                                def.name.value.clone(),
                            ])
                        })
                        .collect::<Vec<Name>>(),
                );
                visited.extend(
                    columns
                        .iter()
                        .flat_map(|def| {
                            def.options.iter().flat_map(|opt| match &opt.option {
                                ColumnOption::ForeignKey {
                                    foreign_table,
                                    referred_columns,
                                    ..
                                } => {
                                    let mut v = vec![];
                                    let table = foreign_table
                                        .0
                                        .iter()
                                        .map(|ident| ident.value.clone())
                                        .collect();
                                    v.push(Name::Table(table));

                                    for col in referred_columns {
                                        v.push(Name::Col(vec![col.value.clone()]))
                                    }
                                    v
                                }
                                _ => Vec::with_capacity(0),
                            })
                        })
                        .collect::<Vec<Name>>(),
                );
            }
            _statement => {}
        }
        ControlFlow::<()>::Continue(())
    });
    visited
}

#[allow(unused)]
#[derive(Default)]
struct AliasVisitor(HashMap<Name, Name>);

impl Visitor for AliasVisitor {
    type Break = ();

    fn post_visit_table_factor(
        &mut self,
        table_factor: &sqlparser::ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => match alias {
                Some(table_alias) => {
                    self.0.insert(
                        Name::Table(name.0.iter().map(|ident| ident.value.clone()).collect()),
                        Name::Table(vec![table_alias.name.value.clone()]),
                    );
                    self.0.insert(
                        Name::Table(vec![table_alias.name.value.clone()]),
                        Name::Table(name.0.iter().map(|ident| ident.value.clone()).collect()),
                    );
                }

                None => {}
            },
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    }
}

#[allow(unused)]
pub fn aliases(statements: &Vec<Statement>) -> HashMap<Name, Name> {
    let mut visitor = AliasVisitor::default();
    statements.iter().for_each(|statement| {
        statement.visit(&mut visitor);
    });
    visitor.0
}

#[allow(unused)]
fn name_from_expr(expr: &Expr) -> Vec<Name> {
    match expr {
        Expr::Identifier(ident) => vec![Name::Col(vec![ident.value.clone()])],
        Expr::CompoundIdentifier(vec) => {
            vec![Name::Col(
                vec.iter().map(|ident| ident.value.clone()).collect(),
            )]
        }
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard(ObjectName(parts)) => vec![Name::Table(
            parts.iter().map(|ident| ident.value.clone()).collect(),
        )],
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Gt => todo!(),
            BinaryOperator::Lt => todo!(),
            BinaryOperator::GtEq => todo!(),
            BinaryOperator::LtEq => todo!(),
            BinaryOperator::Spaceship => todo!(),
            BinaryOperator::Eq => {
                let mut cols = name_from_expr(left.as_ref());
                cols.extend(name_from_expr(right.as_ref()));
                cols
            }
            BinaryOperator::NotEq => todo!(),
            BinaryOperator::And => todo!(),
            BinaryOperator::Or => todo!(),
            BinaryOperator::Xor => todo!(),
            _ => vec![],
        },
        expr => {
            println!("{expr}");
            vec![]
        }
    }
}

fn table(s1: impl core::fmt::Display) -> Name {
    Name::Table(vec![s1.to_string()])
}

fn col(s1: impl core::fmt::Display, s2: impl core::fmt::Display) -> Name {
    Name::Col(vec![s1.to_string(), s2.to_string()])
}

fn table_name(object_name: &ObjectName) -> Name {
    Name::Table(object_name.0.iter().map(|x| x.value.clone()).collect())
}

fn col_name(ident: &sqlparser::ast::Ident) -> Name {
    Name::Col(vec![ident.value.clone()])
}

pub fn validate_migrate_expr(sql_expr: &SqlExpr) -> syn::Result<()> {
    // first run get table name and column names from create table
    let tables: Vec<Name> = sql_expr
        .statements
        .iter()
        .flat_map(|stmt| match stmt {
            Statement::CreateTable(CreateTable { name, columns, .. }) => {
                let mut v = vec![];
                v.push(table_name(name));
                v.extend(columns.iter().map(|col| col_name(&col.name)));
                v
            }
            _ => vec![],
        })
        .collect();
    // second run look at foreign keys
    let refs: Vec<Name> = sql_expr
        .statements
        .iter()
        .flat_map(|stmt| match stmt {
            Statement::CreateTable(CreateTable { columns, .. }) => columns
                .iter()
                .flat_map(|col| {
                    col.options
                        .iter()
                        .flat_map(|opt| match &opt.option {
                            ColumnOption::ForeignKey {
                                foreign_table,
                                referred_columns,
                                ..
                            } => {
                                let mut v = vec![];
                                v.push(table_name(&foreign_table));
                                v.extend(
                                    referred_columns
                                        .iter()
                                        .map(|col| Name::Col(vec![col.value.clone()])),
                                );
                                v
                            }
                            _ => vec![],
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            _ => {
                vec![]
            }
        })
        .collect();

    match validate_query(&tables, &HashMap::new(), &refs) {
        Ok(_) => Ok(()),
        Err(err) => Err(syn::Error::new(sql_expr.ident.span(), format!("{:?}", err))),
    }
}

pub fn validate_query(
    db_schema: &Vec<Name>,
    aliases: &HashMap<Name, Name>,
    query_schema: &Vec<Name>,
) -> Result<()> {
    query_schema.iter().try_for_each(|name| match name {
        Name::Table(vec) => match db_schema.contains(name) {
            true => Ok(()),
            false => Err(Error::MissingTable(vec.join("."))),
        },
        Name::Col(vec) => match db_schema.contains(name) {
            true => Ok(()),
            false => {
                let (table_name, column_name) = match vec.as_slice() {
                    [_schema, table_name, column_name] => (table_name, column_name),
                    [table_name, column_name] => (table_name, column_name),
                    [column_name] => (&String::new(), column_name),
                    val => todo!("{:?}", val),
                };

                match aliases.get(&table(table_name.clone())) {
                    Some(Name::Table(parts)) => {
                        let aliased_table_name = match parts.as_slice() {
                            [_schema, table] => table,
                            [table] => table,
                            _ => todo!(),
                        };
                        let column = col(aliased_table_name, column_name);
                        match db_schema.contains(&column) {
                            true => Ok(()),
                            false => Err(Error::MissingColumn(format!(
                                "{aliased_table_name}.{column_name}"
                            ))),
                        }
                    }
                    Some(Name::Col(_)) => todo!(),
                    None => Err(Error::MissingColumn(vec.join("."))),
                }
            }
        },
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};

    fn parse_sql(sql: &'static str) -> Vec<Statement> {
        Parser::new(&SQLiteDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
    }

    #[test]
    fn names_works() {
        let statements =
            parse_sql("select u.id, t.id from User u join Todo as t on t.user_id == u.id");
        let names = names(&statements);
        let left: Vec<Name> = vec![
            table("User"),
            table("Todo"),
            col("u", "id"),
            col("t", "id"),
            col("t", "user_id"),
            col("u", "id"),
            col("t", "user_id"),
            col("u", "id"),
        ];

        assert_eq!(left, names);
    }

    #[test]
    fn names_from_schema_works() {
        let statements =
            parse_sql("create table User (id integer primary key, email text unique not null)");
        let names = names(&statements);
        let left: Vec<Name> = vec![table("User"), col("User", "id"), col("User", "email")];

        assert_eq!(left, names);
    }

    #[test]
    fn aliases_works() {
        let statements =
            parse_sql("select u.id, t.id from User u join Todo as t on t.user_id == u.id");
        let names = aliases(&statements);
        let left: HashMap<Name, Name> = HashMap::from([
            (table("User"), table("u")),
            (table("u"), table("User")),
            (table("Todo"), table("t")),
            (table("t"), table("Todo")),
        ]);

        assert_eq!(left, names);
    }

    #[test]
    fn validate_works() {
        let statements =
            parse_sql("select u.id, t.id from User u join Todo as t on t.user_id == u.id");
        let create_statements = parse_sql(
            r#"create table User (id integer primary key, email text unique not null);
            create table Todo (id integer primary key, user_id integer not null references User(id))"#,
        );
        let db_schema = names(&create_statements);
        let query_schema = names(&statements);
        let aliases = aliases(&statements);
        let result = validate_query(&db_schema, &aliases, &query_schema);

        assert_eq!(Ok::<(), Error>(()), result);

        let db_schema: Vec<Name> = vec![table("User"), col("User", "id")];
        let query_schema: Vec<Name> = vec![table("User"), col("User", "id2")];
        let result = validate_query(&db_schema, &HashMap::default(), &query_schema);

        assert_eq!(Err(Error::MissingColumn("User.id2".into())), result);
    }
}
