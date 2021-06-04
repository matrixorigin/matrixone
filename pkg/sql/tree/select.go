package tree

type SelectStatement interface {
	Statement
}

//the SelectStatement with an ORDER and/or LIMIT.
type Select struct {
	statementImpl
	Select  SelectStatement
	OrderBy OrderBy
	Limit   *Limit
}

func NewSelect(s SelectStatement,o OrderBy,l *Limit)*Select{
	return &Select{
		Select:        s,
		OrderBy:       o,
		Limit:         l,
	}
}

// OrderBy represents an ORDER BY clause.
type OrderBy []*Order

//the ordering expression.
type Order struct {
	Expr      Expr
	Direction Direction
	//without order
	NullOrder bool
}

func NewOrder(e Expr, d Direction, o bool) *Order {
	return &Order{
		Expr:      e,
		Direction: d,
		NullOrder: o,
	}
}

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	Ascending Direction = iota
	Descending
)

var directionName = []string{
	"ASC",
	"DESC",
}

//the LIMIT clause.
type Limit struct {
	Offset, Count Expr
}

func NewLimit(o, c Expr) *Limit {
	return &Limit{
		Offset: o,
		Count:  c,
	}
}

// the parenthesized SELECT/UNION/VALUES statement.
type ParenSelect struct {
	SelectStatement
	Select *Select
}

type SelectClause struct {
	SelectStatement
	From     *From
	Distinct bool
	Where    *Where
	Exprs    SelectExprs
	GroupBy  GroupBy
	Having   *Where
}

//WHERE or HAVING clause.
type Where struct {
	Expr Expr
}

func NewWhere(e Expr) *Where {
	return &Where{Expr: e}
}

//SELECT expressions.
type SelectExprs []SelectExpr

//a SELECT expression.
type SelectExpr struct {
	Expr Expr
	As   UnrestrictedIdentifier
}

//a GROUP BY clause.
type GroupBy []Expr

const (
	JOIN_TYPE_FULL  = "FULL"
	JOIN_TYPE_LEFT  = "LEFT"
	JOIN_TYPE_RIGHT = "RIGHT"
	JOIN_TYPE_CROSS = "CROSS"
	JOIN_TYPE_INNER = "INNER"
)

//the table expression
type TableExpr interface {
	NodePrinter
}

type tableExprImpl struct {
}

func (tei *tableExprImpl) Print(ctx *PrintCtx) {}

var _ TableExpr = &Subquery{}

type JoinTableExpr struct {
	TableExpr
	JoinType string
	Left     TableExpr
	Right    TableExpr
	Cond     JoinCond
}

func NewJoinTableExpr(jt string, l, r TableExpr, jc JoinCond) *JoinTableExpr {
	return &JoinTableExpr{
		JoinType: jt,
		Left:     l,
		Right:    r,
		Cond:     jc,
	}
}

//the join condition.
type JoinCond interface {
	NodePrinter
}

// the NATURAL join condition
type NaturalJoinCond struct {
	JoinCond
}

func NewNaturalJoinCond() *NaturalJoinCond {
	return &NaturalJoinCond{}
}

//the ON condition for join
type OnJoinCond struct {
	JoinCond
	Expr Expr
}

func NewOnJoinCond(e Expr) *OnJoinCond {
	return &OnJoinCond{Expr: e}
}

//the USING condition
type UsingJoinCond struct {
	JoinCond
	Cols IdentifierList
}

func NewUsingJoinCond(c IdentifierList) *UsingJoinCond {
	return &UsingJoinCond{Cols: c}
}

//the parenthesized TableExpr.
type ParenTableExpr struct {
	TableExpr
	Expr TableExpr
}

func NewParenTableExpr(e TableExpr) *ParenTableExpr {
	return &ParenTableExpr{Expr: e}
}

//The alias, optionally with a column list:
// "AS name" or "AS name(col1, col2)".
type AliasClause struct {
	NodePrinter
	Alias Identifier
}

//the table expression coupled with an optional alias.
type AliasedTableExpr struct {
	TableExpr
	Expr TableExpr
	As   AliasClause
}

func NewAliasedTableExpr(e TableExpr, a AliasClause) *AliasedTableExpr {
	return &AliasedTableExpr{
		Expr: e,
		As:   a,
	}
}

//the statements as a data source includes the select statement.
type StatementSource struct {
	TableExpr
	Statement Statement
}

func NewStatementSource(s Statement) *StatementSource {
	return &StatementSource{
		Statement: s,
	}
}

//the list of table expressions.
type TableExprs []TableExpr

//the FROM clause.
type From struct {
	Tables TableExprs
}

func NewFrom(t TableExprs) *From {
	return &From{Tables: t}
}
