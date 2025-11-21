use crate::sql::statement::{
    Create, Delete, Drop, Insert, OrderBy, OrderDirection, Select, TableRef, Update,
};

use super::{
    tokens::{Keyword, Token},
    Parser, ParserResult, Sql,
};

impl<'sql> Sql<'sql> for Select {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self> {
        let columns = parser.parse_separated_tokens(
            |parser| {
                let expr = parser.parse_expr(None)?;
                match parser.consume_optional(Token::Keyword(Keyword::As)) {
                    false => Ok(expr),
                    _ => Ok(super::Expression::Alias {
                        alias: parser.parse_ident()?,
                        expr: Box::new(expr),
                    }),
                }
            },
            false,
        )?;

        parser.expect_keyword(Keyword::From)?;
        let from = parser.parse_ident()?;
        let from_alias = parser.parse_alias()?;
        let from = TableRef::new(from, from_alias);

        let mut joins = Vec::new();
        while let Some(clause) = parser.parse_join()? {
            joins.push(clause)
        }

        let r#where = match parser.consume_optional(Token::Keyword(Keyword::Where)) {
            true => Some(parser.parse_expr(None)?),
            false => None,
        };

        let group_by = parser.parse_by_separated_keyword(Keyword::Group, |p| p.parse_expr(None))?;
        let order_by = parser.parse_by_separated_keyword(Keyword::Order, |p| {
            let expr = p.parse_expr(None)?;
            let direction = match p.consume_one(&[Keyword::Asc, Keyword::Desc]) {
                Keyword::Desc => OrderDirection::Desc,
                _ => OrderDirection::Asc,
            };

            Ok(OrderBy { expr, direction })
        })?;
        let limit = parser.parse_limit()?;

        Ok(Select {
            columns,
            from,
            joins,
            r#where,
            order_by,
            group_by,
            limit,
        })
    }
}

impl<'sql> Sql<'sql> for Create {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self> {
        let keyword = parser.expect_one(&[
            Keyword::Database,
            Keyword::Table,
            Keyword::Unique,
            Keyword::Index,
            Keyword::Sequence,
        ])?;

        Ok(match keyword {
            Keyword::Database => Create::Database(parser.parse_ident()?),
            Keyword::Table => Create::Table {
                name: parser.parse_ident()?,
                columns: parser.parse_separated_tokens(Parser::parse_col, true)?,
            },
            Keyword::Unique | Keyword::Index => {
                let unique = keyword.eq(&Keyword::Unique);
                if unique {
                    parser.expect_keyword(Keyword::Index)?;
                }

                let name = parser.parse_ident()?;
                parser.expect_keyword(Keyword::On)?;
                let table = parser.parse_ident()?;
                parser.expect_token(Token::LeftParen)?;
                let column = parser.parse_ident()?;
                parser.expect_token(Token::RightParen)?;

                Create::Index {
                    name,
                    column,
                    table,
                    unique,
                }
            }
            Keyword::Sequence => {
                let name = parser.parse_ident()?;
                parser.expect_keyword(Keyword::As)?;
                let r#type = parser.parse_type()?;

                parser.expect_keyword(Keyword::Owned)?;
                parser.expect_keyword(Keyword::By)?;
                let table = parser.parse_ident()?;

                Create::Sequence {
                    table,
                    name,
                    r#type,
                }
            }
            _ => panic!("Unsupported keyword"),
        })
    }
}

impl<'sql> Sql<'sql> for Insert {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self> {
        parser.expect_keyword(Keyword::Into)?;
        let into = parser.parse_ident()?;
        let columns: Vec<String> = match parser.peek_token() {
            Some(_) => parser.parse_separated_tokens(Parser::parse_ident, true),
            None => Ok(vec![]),
        }?;

        parser.expect_keyword(Keyword::Values)?;

        let mut rows = Vec::new();
        loop {
            // each row is ( expr, expr, ... )
            parser.expect_token(Token::LeftParen)?;
            let row = parser.parse_separated_tokens(|p| p.parse_expr(None), false)?;
            parser.expect_token(Token::RightParen)?;
            rows.push(row);

            // comma? if so, more rows; otherwise break
            match parser.consume_optional(Token::Comma) {
                true => continue,
                false => break,
            }
        }

        Ok(Self {
            into,
            columns,
            values: rows,
        })
    }
}

impl<'sql> Sql<'sql> for Update {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self> {
        let table = parser.parse_ident()?;
        parser.expect_keyword(Keyword::Set)?;

        let columns = parser.parse_separated_tokens(Parser::parse_assign, false)?;
        let r#where = match parser.consume_optional(Token::Keyword(Keyword::Where)) {
            true => Some(parser.parse_expr(None)?),
            false => None,
        };

        Ok(Update {
            columns,
            r#where,
            table,
        })
    }
}

impl<'sql> Sql<'sql> for Drop {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self> {
        let keyword = parser.expect_one(&[Keyword::Database, Keyword::Table])?;
        let identifier = parser.parse_ident()?;

        Ok(match keyword {
            Keyword::Database => Drop::Database(identifier),
            Keyword::Table => Drop::Table(identifier),
            _ => unreachable!("Unsupported statement for DROP"),
        })
    }
}

impl<'sql> Sql<'sql> for Delete {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self> {
        parser.expect_keyword(Keyword::From)?;
        let (from, r#where) = parser.parse_from_and_where()?;

        Ok(Self { from, r#where })
    }
}
