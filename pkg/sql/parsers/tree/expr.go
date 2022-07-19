// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"fmt"
)

//AST for the expression
type Expr interface {
	fmt.Stringer
	NodeFormatter
}

type exprImpl struct {
	Expr
}

func (node *exprImpl) String() string {
	return ""
}

//Binary Operator
type BinaryOp int

const (
	PLUS BinaryOp = iota
	MINUS
	MULTI
	DIV         // /
	INTEGER_DIV //
	BIT_OR      // |
	BIT_AND     // &
	BIT_XOR     // ^
	LEFT_SHIFT  // <<
	RIGHT_SHIFT // >>
	MOD         // %
)

func (op BinaryOp) ToString() string {
	switch op {
	case PLUS:
		return "+"
	case MINUS:
		return "-"
	case MULTI:
		return "*"
	case DIV:
		return "/"
	case INTEGER_DIV:
		return "div"
	case BIT_OR:
		return "|"
	case BIT_AND:
		return "&"
	case BIT_XOR:
		return "^"
	case LEFT_SHIFT:
		return "<<"
	case RIGHT_SHIFT:
		return ">>"
	case MOD:
		return "%"
	default:
		return "Unknown BinaryExprOperator"
	}
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

func (node *BinaryExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	ctx.WriteByte(' ')
	ctx.WriteString(node.Op.ToString())
	ctx.WriteByte(' ')
	ctx.PrintExpr(node, node.Right, false)
}

func NewBinaryExpr(op BinaryOp, left Expr, right Expr) *BinaryExpr {
	return &BinaryExpr{
		Op:    op,
		Left:  left,
		Right: right,
	}
}

//unary expression
type UnaryOp int

const (
	//-
	UNARY_MINUS UnaryOp = iota
	//+
	UNARY_PLUS
	//~
	UNARY_TILDE
	//!
	UNARY_MARK
)

func (op UnaryOp) ToString() string {
	switch op {
	case UNARY_MINUS:
		return "-"
	case UNARY_PLUS:
		return "+"
	case UNARY_TILDE:
		return "~"
	case UNARY_MARK:
		return "!"
	default:
		return "Unknown UnaryExprOperator"
	}
}

//unary expression
type UnaryExpr struct {
	exprImpl

	//operator
	Op UnaryOp

	//expression
	Expr Expr
}

func (e *UnaryExpr) Format(ctx *FmtCtx) {
	if _, unary := e.Expr.(*UnaryExpr); unary {
		ctx.WriteString(e.Op.ToString())
		ctx.WriteByte(' ')
		ctx.PrintExpr(e, e.Expr, true)
		return
	}
	ctx.WriteString(e.Op.ToString())
	ctx.PrintExpr(e, e.Expr, true)
}

func (e *UnaryExpr) String() string {
	return unaryOpName[e.Op] + e.Expr.String()
}

func NewUnaryExpr(op UnaryOp, expr Expr) *UnaryExpr {
	return &UnaryExpr{
		Op:   op,
		Expr: expr,
	}
}

var unaryOpName = []string{
	"-",
	"+",
	"~",
	"!",
}

//comparion operation
type ComparisonOp int

const (
	EQUAL            ComparisonOp = iota // =
	LESS_THAN                            // <
	LESS_THAN_EQUAL                      // <=
	GREAT_THAN                           // >
	GREAT_THAN_EQUAL                     // >=
	NOT_EQUAL                            // <>, !=
	IN                                   // IN
	NOT_IN                               // NOT IN
	LIKE                                 // LIKE
	NOT_LIKE                             // NOT LIKE
	REG_MATCH                            // REG_MATCH
	NOT_REG_MATCH                        // NOT REG_MATCH
	IS_DISTINCT_FROM
	IS_NOT_DISTINCT_FROM
	NULL_SAFE_EQUAL // <=>
	//reference: https://dev.mysql.com/doc/refman/8.0/en/all-subqueries.html
	//subquery with ANY,SOME,ALL
	//operand comparison_operator [ANY | SOME | ALL] (subquery)
	ANY
	SOME
	ALL
)

func (op ComparisonOp) ToString() string {
	switch op {
	case EQUAL:
		return "="
	case LESS_THAN:
		return "<"
	case LESS_THAN_EQUAL:
		return "<="
	case GREAT_THAN:
		return ">"
	case GREAT_THAN_EQUAL:
		return ">="
	case NOT_EQUAL:
		return "!="
	case IN:
		return "in"
	case NOT_IN:
		return "not in"
	case LIKE:
		return "like"
	case NOT_LIKE:
		return "not like"
	case REG_MATCH:
		return "reg_match"
	case NOT_REG_MATCH:
		return "not reg_match"
	case IS_DISTINCT_FROM:
		return "is distinct from"
	case IS_NOT_DISTINCT_FROM:
		return "is not distinct from"
	case NULL_SAFE_EQUAL:
		return "<=>"
	case ANY:
		return "any"
	case SOME:
		return "some"
	case ALL:
		return "all"
	default:
		return "Unknown ComparisonExprOperator"
	}
}

type ComparisonExpr struct {
	exprImpl
	Op ComparisonOp

	//ANY SOME ALL with subquery
	SubOp  ComparisonOp
	Left   Expr
	Right  Expr
	Escape Expr
}

func (node *ComparisonExpr) Format(ctx *FmtCtx) {
	if node.Left != nil {
		ctx.PrintExpr(node, node.Left, true)
		ctx.WriteByte(' ')
	}
	ctx.WriteString(node.Op.ToString())
	ctx.WriteByte(' ')

	if node.SubOp != ComparisonOp(0) {
		ctx.WriteString(node.SubOp.ToString())
		ctx.WriteByte(' ')
	}

	ctx.PrintExpr(node, node.Right, false)
	if node.Escape != nil {
		ctx.WriteString(" escape ")
		ctx.PrintExpr(node, node.Escape, true)
	}
}

func NewComparisonExpr(op ComparisonOp, l, r Expr) *ComparisonExpr {
	return &ComparisonExpr{
		Op:    op,
		SubOp: ComparisonOp(0),
		Left:  l,
		Right: r,
	}
}

func NewSubqueryComparisonExpr(op ComparisonOp, subOp ComparisonOp, l, r Expr) *ComparisonExpr {
	return &ComparisonExpr{
		Op:    op,
		SubOp: subOp,
		Left:  l,
		Right: r,
	}
}

func NewComparisonExprWithSubop(op, subop ComparisonOp, l, r Expr) *ComparisonExpr {
	return &ComparisonExpr{
		Op:    op,
		SubOp: subop,
		Left:  l,
		Right: r,
	}
}

func NewComparisonExprWithEscape(op ComparisonOp, l, r, e Expr) *ComparisonExpr {
	return &ComparisonExpr{
		Op:     op,
		Left:   l,
		Right:  r,
		Escape: e,
	}
}

//and expression
type AndExpr struct {
	exprImpl
	Left, Right Expr
}

func (node *AndExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	ctx.WriteString(" and ")
	ctx.PrintExpr(node, node.Right, false)
}

func NewAndExpr(l, r Expr) *AndExpr {
	return &AndExpr{
		Left:  l,
		Right: r,
	}
}

//xor expression
type XorExpr struct {
	exprImpl
	Left, Right Expr
}

func (node *XorExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	ctx.WriteString(" xor ")
	ctx.PrintExpr(node, node.Right, false)
}

func NewXorExpr(l, r Expr) *XorExpr {
	return &XorExpr{
		Left:  l,
		Right: r,
	}
}

//or expression
type OrExpr struct {
	exprImpl
	Left, Right Expr
}

func (node *OrExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	ctx.WriteString(" or ")
	ctx.PrintExpr(node, node.Right, false)
}

func NewOrExpr(l, r Expr) *OrExpr {
	return &OrExpr{
		Left:  l,
		Right: r,
	}
}

//not expression
type NotExpr struct {
	exprImpl
	Expr Expr
}

func (node *NotExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("not ")
	ctx.PrintExpr(node, node.Expr, true)
}

func NewNotExpr(e Expr) *NotExpr {
	return &NotExpr{
		Expr: e,
	}
}

//is null expression
type IsNullExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsNullExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is null")
}

func NewIsNullExpr(e Expr) *IsNullExpr {
	return &IsNullExpr{
		Expr: e,
	}
}

//is not null expression
type IsNotNullExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsNotNullExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is not null")
}

func NewIsNotNullExpr(e Expr) *IsNotNullExpr {
	return &IsNotNullExpr{
		Expr: e,
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

func (node *Subquery) Format(ctx *FmtCtx) {
	if node.Exists {
		ctx.WriteString("exists ")
	}
	node.Select.Format(ctx)
}

func NewSubquery(s SelectStatement, e bool) *Subquery {
	return &Subquery{
		Select: s,
		Exists: e,
	}
}

//a list of expression.
type Exprs []Expr

func (node Exprs) Format(ctx *FmtCtx) {
	prefix := ""
	for _, n := range node {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = ", "
	}
}

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

func (node *ParenExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	node.Expr.Format(ctx)
	ctx.WriteByte(')')
}

func NewParenExpr(e Expr) *ParenExpr {
	return &ParenExpr{
		Expr: e,
	}
}

type FuncType int

func (node *FuncType) ToString() string {
	switch *node {
	case FUNC_TYPE_DISTINCT:
		return "distinct"
	case FUNC_TYPE_ALL:
		return "all"
	default:
		return "Unknown FuncType"
	}
}

const (
	FUNC_TYPE_DEFAULT FuncType = iota
	FUNC_TYPE_DISTINCT
	FUNC_TYPE_ALL
)

// AggType specifies the type of aggregation.
type AggType int

const (
	_ AggType = iota
	AGG_TYPE_GENERAL
)

//the common interface to UnresolvedName and QualifiedFunctionName.
type FunctionReference interface {
	fmt.Stringer
	NodeFormatter
}

var _ FunctionReference = &UnresolvedName{}

//function reference
type ResolvableFunctionReference struct {
	FunctionReference
}

func (node *ResolvableFunctionReference) Format(ctx *FmtCtx) {
	node.FunctionReference.(*UnresolvedName).Format(ctx)
}

func FuncName2ResolvableFunctionReference(funcName *UnresolvedName) ResolvableFunctionReference {
	return ResolvableFunctionReference{FunctionReference: funcName}
}

//function call expression
type FuncExpr struct {
	exprImpl
	Func  ResolvableFunctionReference
	Type  FuncType
	Exprs Exprs

	//specify the type of aggregation.
	AggType AggType

	//aggregations which specify an order.
	OrderBy OrderBy
}

func (node *FuncExpr) Format(ctx *FmtCtx) {
	node.Func.Format(ctx)

	ctx.WriteString("(")
	if node.Type != FUNC_TYPE_DEFAULT {
		ctx.WriteString(node.Type.ToString())
		ctx.WriteByte(' ')
	}
	node.Exprs.Format(ctx)
	ctx.WriteByte(')')
}

func NewFuncExpr(ft FuncType, name *UnresolvedName, e Exprs, order OrderBy) *FuncExpr {
	return &FuncExpr{
		Func:    FuncName2ResolvableFunctionReference(name),
		Type:    ft,
		Exprs:   e,
		AggType: AGG_TYPE_GENERAL,
		OrderBy: order,
	}
}

//type reference
type ResolvableTypeReference interface {
}

var _ ResolvableTypeReference = &UnresolvedObjectName{}
var _ ResolvableTypeReference = &T{}

//the Cast expression
type CastExpr struct {
	exprImpl
	Expr Expr
	Type ResolvableTypeReference
}

func (node *CastExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("cast(")
	node.Expr.Format(ctx)
	ctx.WriteString(" as ")
	node.Type.(*T).InternalType.Format(ctx)
	ctx.WriteByte(')')
}

func NewCastExpr(e Expr, t ResolvableTypeReference) *CastExpr {
	return &CastExpr{
		Expr: e,
		Type: t,
	}
}

//the parenthesized list of expressions.
type Tuple struct {
	exprImpl
	Exprs Exprs
}

func (node *Tuple) Format(ctx *FmtCtx) {
	if node.Exprs != nil {
		ctx.WriteByte('(')
		node.Exprs.Format(ctx)
		ctx.WriteByte(')')
	}
}

func NewTuple(e Exprs) *Tuple {
	return &Tuple{Exprs: e}
}

//the BETWEEN or a NOT BETWEEN expression
type RangeCond struct {
	exprImpl
	Not      bool
	Left     Expr
	From, To Expr
}

func (node *RangeCond) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	if node.Not {
		ctx.WriteString(" not")
	}
	ctx.WriteString(" between ")
	ctx.PrintExpr(node, node.From, true)
	ctx.WriteString(" and ")
	ctx.PrintExpr(node, node.To, false)
}

func NewRangeCond(n bool, l, f, t Expr) *RangeCond {
	return &RangeCond{
		Not:  n,
		Left: l,
		From: f,
		To:   t,
	}
}

//Case-When expression.
type CaseExpr struct {
	exprImpl
	Expr  Expr
	Whens []*When
	Else  Expr
}

func (node *CaseExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("case")
	if node.Expr != nil {
		ctx.WriteByte(' ')
		node.Expr.Format(ctx)
	}
	ctx.WriteByte(' ')
	prefix := ""
	for _, w := range node.Whens {
		ctx.WriteString(prefix)
		w.Format(ctx)
		prefix = " "
	}
	if node.Else != nil {
		ctx.WriteString(" else ")
		node.Else.Format(ctx)
	}
	ctx.WriteString(" end")
}

func NewCaseExpr(e Expr, w []*When, el Expr) *CaseExpr {
	return &CaseExpr{
		Expr:  e,
		Whens: w,
		Else:  el,
	}
}

//When sub-expression.
type When struct {
	Cond Expr
	Val  Expr
}

func (node *When) Format(ctx *FmtCtx) {
	ctx.WriteString("when ")
	node.Cond.Format(ctx)
	ctx.WriteString(" then ")
	node.Val.Format(ctx)
}

func NewWhen(c, v Expr) *When {
	return &When{
		Cond: c,
		Val:  v,
	}
}

// IntervalType is the type for time and timestamp units.
type IntervalType int

func (node *IntervalType) ToString() string {
	switch *node {
	case INTERVAL_TYPE_SECOND:
		return "second"
	default:
		return "Unknown IntervalType"
	}
}

const (
	//an invalid time or timestamp unit
	INTERVAL_TYPE_INVALID IntervalType = iota
	//the time or timestamp unit MICROSECOND.
	INTERVAL_TYPE_MICROSECOND
	//the time or timestamp unit SECOND.
	INTERVAL_TYPE_SECOND
	//the time or timestamp unit MINUTE.
	INTERVAL_TYPE_MINUTE
	//the time or timestamp unit HOUR.
	INTERVAL_TYPE_HOUR
	//the time or timestamp unit DAY.
	INTERVAL_TYPE_DAY
	//the time or timestamp unit WEEK.
	INTERVAL_TYPE_WEEK
	//the time or timestamp unit MONTH.
	INTERVAL_TYPE_MONTH
	//the time or timestamp unit QUARTER.
	INTERVAL_TYPE_QUARTER
	//the time or timestamp unit YEAR.
	INTERVAL_TYPE_YEAR
	//the time unit SECOND_MICROSECOND.
	INTERVAL_TYPE_SECOND_MICROSECOND
	//the time unit MINUTE_MICROSECOND.
	INTERVAL_TYPE_MINUTE_MICROSECOND
	//the time unit MINUTE_SECOND.
	INTERVAL_TYPE_MINUTE_SECOND
	//the time unit HOUR_MICROSECOND.
	INTERVAL_TYPE_HOUR_MICROSECOND
	//the time unit HOUR_SECOND.
	INTERVAL_TYPE_HOUR_SECOND
	//the time unit HOUR_MINUTE.
	INTERVAL_TYPE_HOUR_MINUTE
	//the time unit DAY_MICROSECOND.
	INTERVAL_TYPE_DAY_MICROSECOND
	//the time unit DAY_SECOND.
	INTERVAL_TYPE_DAY_SECOND
	//the time unit DAY_MINUTE.
	INTERVAL_TYPE_DAYMINUTE
	//the time unit DAY_HOUR.
	INTERVAL_TYPE_DAYHOUR
	//the time unit YEAR_MONTH.
	INTERVAL_TYPE_YEARMONTH
)

//INTERVAL / time unit
type IntervalExpr struct {
	exprImpl
	Expr Expr
	Type IntervalType
}

func (node *IntervalExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("interval")
	if node.Expr != nil {
		ctx.WriteByte(' ')
		node.Expr.Format(ctx)
	}
	if node.Type != INTERVAL_TYPE_INVALID {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Type.ToString())
	}
}

func NewIntervalExpr(t IntervalType) *IntervalExpr {
	return &IntervalExpr{
		Type: t,
	}
}

//the DEFAULT expression.
type DefaultVal struct {
	exprImpl
	Expr Expr
}

func (node *DefaultVal) Format(ctx *FmtCtx) {
	ctx.WriteString("default")
	if node.Expr != nil {
		node.Expr.Format(ctx)
	}
}

func NewDefaultVal(e Expr) *DefaultVal {
	return &DefaultVal{
		Expr: e,
	}
}

type MaxValue struct {
	exprImpl
}

func NewMaxValue() *MaxValue {
	return &MaxValue{}
}

type TypeExpr interface {
	Expr
}

/*
Variable Expression Used in Set Statement,
Load Data statement, Show statement,etc.
Variable types:
User-Defined Variable
Local-Variable: DECLARE statement
System Variable: Global System Variable, Session System Variable
*/

type VarExpr struct {
	exprImpl
	Name   string
	System bool
	Global bool
	Expr   Expr
}

// incomplete
func (node *VarExpr) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteByte('@')
		if node.System {
			ctx.WriteByte('@')
		}
		ctx.WriteString(node.Name)
	}
}

func NewVarExpr(n string, s bool, g bool, e Expr) *VarExpr {
	return &VarExpr{
		Name:   n,
		System: s,
		Global: g,
		Expr:   e,
	}
}

// select a from t1 where a > ?
type ParamExpr struct {
	exprImpl
	Offset int
}

func (node *ParamExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('?')
}

func NewParamExpr(offset int) *ParamExpr {
	return &ParamExpr{
		Offset: offset,
	}
}
