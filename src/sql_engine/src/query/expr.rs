use sqlparser::ast::*;
pub enum EvalError {

}

pub fn resolve_static_expr(expr: &Expr) -> Result<Expr, EvalError> {
    use Expr::*;
    match expr {
        Identifier(_) => expr.clone(),
        Wildcard => expr.clone(),
        QualifiedWildcard(_) => expr.clone(),
        CompoundIdentifier(_) => expr.clone(),
        IsNull(Box<Expr>),
        IsNotNull(Box<Expr>),
        InList {
            expr: Box<Expr>,
            list: Vec<Expr>,
            negated: bool,
        },
        InSubquery {
            expr: Box<Expr>,
            subquery: Box<Query>,
            negated: bool,
        },
        Between {
            expr: Box<Expr>,
            negated: bool,
            low: Box<Expr>,
            high: Box<Expr>,
        },
        BinaryOp {
            left: Box<Expr>,
            op: BinaryOperator,
            right: Box<Expr>,
        },
        UnaryOp {
            op: UnaryOperator,
            expr: Box<Expr>,
        },
        Cast {
            expr: Box<Expr>,
            data_type: DataType,
        },
        Extract {
            field: DateTimeField,
            expr: Box<Expr>,
        },
        Collate {
            expr: Box<Expr>,
            collation: ObjectName,
        },
        Nested(Box<Expr>),
        Value(Value),
        TypedString {
            data_type: DataType,
            value: String,
        },
        Function(Function),
        Case {
            operand: Option<Box<Expr>>,
            conditions: Vec<Expr>,
            results: Vec<Expr>,
            else_result: Option<Box<Expr>>,
        },
        Exists(Box<Query>),
        Subquery(Box<Query>),
        ListAgg(ListAgg),
    }
}