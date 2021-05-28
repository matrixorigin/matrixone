package tree

import "fmt"

//AST for the expression
type Expr interface {
	fmt.Stringer
	NodePrinter

	//Visitor Design Pattern
	//Accept the visitor to access the node.
	Accept(Visitor) Expr
}

type exprImpl struct {
}

func (ei *exprImpl) String() string {
	return ""
}

func (ei *exprImpl) Print(ctx *PrintCtx) {
}

func (ei *exprImpl) Accept(_ Visitor) Expr {
	return ei
}

//Binary Operator
type BinaryOp int

const (
	PLUS BinaryOp = iota
	MINUS
	MULTI
	DIV // /
	INTEGER_DIV //
	BIT_OR // |
	BIT_AND // &
	BIT_XOR // ^
	LEFT_SHIFT // <<
	RIGHT_SHIFT // >>
	MOD // %
)

var binaryOpName = []string{
	"+",
	"-",
	"*",
	"/",
}

//binary expression
type BinaryExpr struct {
	exprImpl

	//operator
	Op BinaryOp

	//left expression
	Left Expr

	//right expression
	Right Expr
}

func NewBinaryExpr(op BinaryOp,left Expr,right Expr)*BinaryExpr{
	return &BinaryExpr{
		Op:    op,
		Left:  left,
		Right: right,
	}
}

//unary expression
type UnaryOp int

const(
	//-
	UNARY_MINUS UnaryOp = iota
	//+
	UNARY_PLUS
	//~
	UNARY_TILDE
	//!
	UNARY_MARK
)

var unaryOpName = []string{
	"-",
	"+",
	"~",
	"!",
}

//unary expression
type UnaryExpr struct {
	exprImpl

	//operator
	Op UnaryOp

	//expression
	Expr     Expr
}

func NewUnaryExpr(op UnaryOp,expr Expr)*UnaryExpr{
	return &UnaryExpr{
		Op:    op,
		Expr:  expr,
	}
}

//comparion operation
type ComparisonOp int

const (
	EQUAL ComparisonOp = iota // =
	LESS_THAN // <
	LESS_THAN_EQUAL // <=
	GREAT_THAN // >
	GREAT_THAN_EQUAL // >=
	NOT_EQUAL // <>, !=
	IN // IN
	NOT_IN // NOT IN
	LIKE // LIKE
	NOT_LIKE // NOT LIKE
	REG_MATCH // REG_MATCH
	NOT_REG_MATCH // NOT REG_MATCH

	//reference: https://dev.mysql.com/doc/refman/8.0/en/all-subqueries.html
	//subquery with ANY,SOME,ALL
	//operand comparison_operator [ANY | SOME | ALL] (subquery)
	ANY
	SOME
	ALL
)

var comparionName =[]string{
	"=",
	"<",
	"<=",
	">",
	">=",
	"!=",
	"IN",
	"NOT IN",
	"LIKE",
	"NOT LIKE",

}

type ComparisonExpr struct {
	exprImpl
	Op ComparisonOp

	//ANY SOME ALL with subquery
	SubOp ComparisonOp
	Left Expr
	Right Expr
}

func NewComparisonExpr(op ComparisonOp,l,r Expr)*ComparisonExpr{
	return &ComparisonExpr{
		Op:       op,
		SubOp:    ComparisonOp(0),
		Left:     l,
		Right:    r,
	}
}

func NewComparisonExprWithSubop(op,subop ComparisonOp,l,r Expr)*ComparisonExpr{
	return &ComparisonExpr{
		Op:       op,
		SubOp:    subop,
		Left:     l,
		Right:    r,
	}
}

//and expression
type AndExpr struct {
	exprImpl
	Left,Right Expr
}

func NewAndExpr(l,r Expr)*AndExpr{
	return &AndExpr{
		Left:l,
		Right: r,
	}
}

//xor expression
type XorExpr struct {
	exprImpl
	Left,Right Expr
}

func NewXorExpr(l,r Expr)*XorExpr{
	return &XorExpr{
		Left:l,
		Right: r,
	}
}

//or expression
type OrExpr struct {
	exprImpl
	Left,Right Expr
}

func NewOrExpr(l,r Expr)*OrExpr{
	return &OrExpr{
		Left:l,
		Right: r,
	}
}

//not expression
type NotExpr struct {
	exprImpl
	Expr Expr
}

func NewNotExpr(e Expr)*NotExpr{
	return &NotExpr{
		Expr:e,
	}
}

//is null expression
type IsNullExpr struct {
	exprImpl
	Expr Expr
}

func NewIsNullExpr(e Expr)*IsNullExpr{
	return &IsNullExpr{
		Expr:     e,
	}
}

//is not null expression
type IsNotNullExpr struct {
	exprImpl
	Expr Expr
}

func NewIsNotNullExpr(e Expr)*IsNotNullExpr{
	return &IsNotNullExpr{
		Expr:     e,
	}
}

//subquery interface
type SubqueryExpr interface {
	Expr
}

//subquery
type Subquery struct {
	SubqueryExpr

	Select SelectStatement
	Exists bool
}

func NewSubquery(s SelectStatement,e bool)*Subquery{
	return &Subquery{
		Select:       s,
		Exists:       e,
	}
}

//a list of expression.
type Exprs []Expr

//ast fir the list of expression
type ExprList struct {
	exprImpl
	Exprs Exprs
}

//the parenthesized expression.
type ParenExpr struct {
	exprImpl
	Expr Expr
}

func NewParenExpr(e Expr)*ParenExpr{
	return &ParenExpr{
		Expr:     e,
	}
}