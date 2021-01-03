pub use sqlparser::dialect::Dialect;
pub use sqlparser::parser::*;

#[derive(Debug, Default)]
pub struct PreparedStatementDialect;

impl Dialect for PreparedStatementDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '$' || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ('0'..='9').contains(&ch) || ch == '$' || ch == '_'
    }
}
