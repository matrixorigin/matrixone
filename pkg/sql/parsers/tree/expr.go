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
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// AST for the expression
type Expr interface {
	fmt.Stringer
	NodeFormatter
	NodeChecker
}

type exprImpl struct {
	Expr
}

func (node *exprImpl) String() string {
	return ""
}

// Binary Operator
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

// binary expression
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

// Accept implements NodeChecker interface
func (node *BinaryExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}

	node = newNode.(*BinaryExpr)
	tmpNode, ok := node.Left.Accept(v)
	if !ok {
		return node, false
	}
	node.Left = tmpNode

	tmpNode, ok = node.Right.Accept(v)
	if !ok {
		return node, false
	}
	node.Right = tmpNode
	return v.Exit(node)
}

func NewBinaryExpr(op BinaryOp, left Expr, right Expr) *BinaryExpr {
	return &BinaryExpr{
		Op:    op,
		Left:  left,
		Right: right,
	}
}

// unary expression
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

// unary expression
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

func (e *UnaryExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(e)
	if skipChildren {
		return v.Exit(newNode)
	}
	e = newNode.(*UnaryExpr)
	node, ok := e.Expr.Accept(v)
	if !ok {
		return e, false
	}
	e.Expr = node
	return v.Exit(e)
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

// comparion operation
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
	ILIKE
	NOT_ILIKE
	REG_MATCH     // REG_MATCH
	NOT_REG_MATCH // NOT REG_MATCH
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
	case ILIKE:
		return "ilike"
	case NOT_ILIKE:
		return "not ilike"
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

// Accept implements NodeChecker interface
func (node *ComparisonExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}

	node = newNode.(*ComparisonExpr)
	tmpNode, ok := node.Left.Accept(v)
	if !ok {
		return node, false
	}
	node.Left = tmpNode

	tmpNode, ok = node.Right.Accept(v)
	if !ok {
		return node, false
	}
	node.Right = tmpNode
	return v.Exit(node)
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

// and expression
type AndExpr struct {
	exprImpl
	Left, Right Expr
}

func (node *AndExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	ctx.WriteString(" and ")
	ctx.PrintExpr(node, node.Right, false)
}

func (node *AndExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}

	node = newNode.(*AndExpr)
	tmpNode, ok := node.Left.Accept(v)
	if !ok {
		return node, false
	}
	node.Left = tmpNode

	tmpNode, ok = node.Right.Accept(v)
	if !ok {
		return node, false
	}
	node.Right = tmpNode

	return v.Exit(node)
}

func NewAndExpr(l, r Expr) *AndExpr {
	return &AndExpr{
		Left:  l,
		Right: r,
	}
}

// xor expression
type XorExpr struct {
	exprImpl
	Left, Right Expr
}

func (node *XorExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	ctx.WriteString(" xor ")
	ctx.PrintExpr(node, node.Right, false)
}

func (node *XorExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}

	node = newNode.(*XorExpr)
	tmpNode, ok := node.Left.Accept(v)
	if !ok {
		return node, false
	}
	node.Left = tmpNode

	tmpNode, ok = node.Right.Accept(v)
	if !ok {
		return node, false
	}
	node.Right = tmpNode

	return v.Exit(node)
}

func NewXorExpr(l, r Expr) *XorExpr {
	return &XorExpr{
		Left:  l,
		Right: r,
	}
}

// or expression
type OrExpr struct {
	exprImpl
	Left, Right Expr
}

func (node *OrExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Left, true)
	ctx.WriteString(" or ")
	ctx.PrintExpr(node, node.Right, false)
}

func (node *OrExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}

	node = newNode.(*OrExpr)
	tmpNode, ok := node.Left.Accept(v)
	if !ok {
		return node, false
	}
	node.Left = tmpNode

	tmpNode, ok = node.Right.Accept(v)
	if !ok {
		return node, false
	}
	node.Right = tmpNode

	return v.Exit(node)
}

func NewOrExpr(l, r Expr) *OrExpr {
	return &OrExpr{
		Left:  l,
		Right: r,
	}
}

// not expression
type NotExpr struct {
	exprImpl
	Expr Expr
}

func (node *NotExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("not ")
	ctx.PrintExpr(node, node.Expr, true)
}

func (node *NotExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}

	node = newNode.(*NotExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewNotExpr(e Expr) *NotExpr {
	return &NotExpr{
		Expr: e,
	}
}

// is null expression
type IsNullExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsNullExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is null")
}

// Accept implements NodeChecker interface
func (node *IsNullExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsNullExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewIsNullExpr(e Expr) *IsNullExpr {
	return &IsNullExpr{
		Expr: e,
	}
}

// is not null expression
type IsNotNullExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsNotNullExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is not null")
}

// Accept implements NodeChecker interface
func (node *IsNotNullExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsNotNullExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewIsNotNullExpr(e Expr) *IsNotNullExpr {
	return &IsNotNullExpr{
		Expr: e,
	}
}

// is unknown expression
type IsUnknownExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsUnknownExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is unknown")
}

// Accept implements NodeChecker interface
func (node *IsUnknownExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsUnknownExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewIsUnknownExpr(e Expr) *IsUnknownExpr {
	return &IsUnknownExpr{
		Expr: e,
	}
}

// is not unknown expression
type IsNotUnknownExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsNotUnknownExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is not unknown")
}

// Accept implements NodeChecker interface
func (node *IsNotUnknownExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsNotUnknownExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewIsNotUnknownExpr(e Expr) *IsNotUnknownExpr {
	return &IsNotUnknownExpr{
		Expr: e,
	}
}

// is true expression
type IsTrueExpr struct {
	exprImpl
	Expr Expr
}

// Accept implements NodeChecker interface
func (node *IsTrueExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsTrueExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func (node *IsTrueExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is true")
}

func NewIsTrueExpr(e Expr) *IsTrueExpr {
	return &IsTrueExpr{
		Expr: e,
	}
}

// is not true expression
type IsNotTrueExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsNotTrueExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is not true")
}

// Accept implements NodeChecker interface
func (node *IsNotTrueExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsNotTrueExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewIsNotTrueExpr(e Expr) *IsNotTrueExpr {
	return &IsNotTrueExpr{
		Expr: e,
	}
}

// is false expression
type IsFalseExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsFalseExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is false")
}

// Accept implements NodeChecker interface
func (node *IsFalseExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsFalseExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewIsFalseExpr(e Expr) *IsFalseExpr {
	return &IsFalseExpr{
		Expr: e,
	}
}

// is not false expression
type IsNotFalseExpr struct {
	exprImpl
	Expr Expr
}

func (node *IsNotFalseExpr) Format(ctx *FmtCtx) {
	ctx.PrintExpr(node, node.Expr, true)
	ctx.WriteString(" is not false")
}

// Accept implements NodeChecker interface
func (node *IsNotFalseExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*IsNotFalseExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewIsNotFalseExpr(e Expr) *IsNotFalseExpr {
	return &IsNotFalseExpr{
		Expr: e,
	}
}

// subquery interface
type SubqueryExpr interface {
	Expr
}

// subquery
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

func (node *Subquery) Accept(v Visitor) (Expr, bool) {
	panic("unimplement Subquery Accept")
}

func (node *Subquery) String() string {
	return "subquery"
}

func NewSubquery(s SelectStatement, e bool) *Subquery {
	return &Subquery{
		Select: s,
		Exists: e,
	}
}

// a list of expression.
type Exprs []Expr

func (node Exprs) Format(ctx *FmtCtx) {
	prefix := ""
	for _, n := range node {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = ", "
	}
}

// ast fir the list of expression
type ExprList struct {
	exprImpl
	Exprs Exprs
}

func (n *ExprList) Accept(v Visitor) (Expr, bool) {
	panic("unimplement ExprList Accept")
}

// the parenthesized expression.
type ParenExpr struct {
	exprImpl
	Expr Expr
}

func (node *ParenExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	node.Expr.Format(ctx)
	ctx.WriteByte(')')
}

func (node *ParenExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*ParenExpr)
	if node.Expr != nil {
		tmpNode, ok := node.Expr.Accept(v)
		if !ok {
			return node, false
		}
		node.Expr = tmpNode
	}
	return v.Exit(node)
}

func NewParentExpr(e Expr) *ParenExpr {
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
	case FUNC_TYPE_TABLE:
		return "table function"
	default:
		return "Unknown FuncType"
	}
}

const (
	FUNC_TYPE_DEFAULT FuncType = iota
	FUNC_TYPE_DISTINCT
	FUNC_TYPE_ALL
	FUNC_TYPE_TABLE
)

// AggType specifies the type of aggregation.
type AggType int

const (
	_ AggType = iota
	AGG_TYPE_GENERAL
)

// the common interface to UnresolvedName and QualifiedFunctionName.
type FunctionReference interface {
	fmt.Stringer
	NodeFormatter
}

var _ FunctionReference = &UnresolvedName{}

// function reference
type ResolvableFunctionReference struct {
	FunctionReference
}

func (node *ResolvableFunctionReference) Format(ctx *FmtCtx) {
	node.FunctionReference.(*UnresolvedName).Format(ctx)
}

func FuncName2ResolvableFunctionReference(funcName *UnresolvedName) ResolvableFunctionReference {
	return ResolvableFunctionReference{FunctionReference: funcName}
}

// function call expression
type FuncExpr struct {
	exprImpl
	Func     ResolvableFunctionReference
	FuncName *CStr
	Type     FuncType
	Exprs    Exprs

	//specify the type of aggregation.
	AggType AggType

	WindowSpec *WindowSpec

	OrderBy OrderBy
}

func (node *FuncExpr) Format(ctx *FmtCtx) {
	if node.FuncName != nil {
		ctx.WriteString(node.FuncName.Compare())
	} else {
		node.Func.Format(ctx)
	}

	ctx.WriteString("(")
	if node.Type != FUNC_TYPE_DEFAULT && node.Type != FUNC_TYPE_TABLE {
		ctx.WriteString(node.Type.ToString())
		ctx.WriteByte(' ')
	}
	if node.Func.FunctionReference.(*UnresolvedName).Parts[0] == "trim" {
		trimExprsFormat(ctx, node.Exprs)
	} else {
		node.Exprs.Format(ctx)
	}

	if node.OrderBy != nil {
		node.OrderBy.Format(ctx)
	}

	ctx.WriteByte(')')

	if node.WindowSpec != nil {
		ctx.WriteString(" ")
		node.WindowSpec.Format(ctx)
	}
}

// Accept implements NodeChecker interface
func (node *FuncExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*FuncExpr)
	for i, val := range node.Exprs {
		tmpNode, ok := val.Accept(v)
		if !ok {
			return node, false
		}
		node.Exprs[i] = tmpNode
	}
	return v.Exit(node)
}

func trimExprsFormat(ctx *FmtCtx, exprs Exprs) {
	tp := exprs[0].(*NumVal).String()
	switch tp {
	case "0":
		exprs[3].Format(ctx)
	case "1":
		exprs[2].Format(ctx)
		ctx.WriteString(" from ")
		exprs[3].Format(ctx)
	case "2":
		exprs[1].Format(ctx)
		ctx.WriteString(" from ")
		exprs[3].Format(ctx)
	case "3":
		exprs[1].Format(ctx)
		ctx.WriteString(" ")
		exprs[2].Format(ctx)
		ctx.WriteString(" from ")
		exprs[3].Format(ctx)
	default:
		panic("unknown trim type")
	}
}

type WindowSpec struct {
	PartitionBy Exprs
	OrderBy     OrderBy
	HasFrame    bool
	Frame       *FrameClause
}

func (node *WindowSpec) Format(ctx *FmtCtx) {
	ctx.WriteString("over (")
	flag := false
	if len(node.PartitionBy) > 0 {
		ctx.WriteString("partition by ")
		node.PartitionBy.Format(ctx)
		flag = true
	}

	if len(node.OrderBy) > 0 {
		if flag {
			ctx.WriteString(" ")
		}
		node.OrderBy.Format(ctx)
		flag = true
	}

	if node.Frame != nil && node.HasFrame {
		if flag {
			ctx.WriteString(" ")
		}
		node.Frame.Format(ctx)
	}

	ctx.WriteByte(')')
}

type FrameType int

const (
	Rows FrameType = iota
	Range
	Groups
)

type FrameClause struct {
	Type   FrameType
	HasEnd bool
	Start  *FrameBound
	End    *FrameBound
}

func (node *FrameClause) Format(ctx *FmtCtx) {
	switch node.Type {
	case Rows:
		ctx.WriteString("rows")
	case Range:
		ctx.WriteString("range")
	case Groups:
		ctx.WriteString("groups")
	}
	ctx.WriteString(" ")
	if !node.HasEnd {
		node.Start.Format(ctx)
		return
	}
	ctx.WriteString("between ")
	node.Start.Format(ctx)
	ctx.WriteString(" and ")
	node.End.Format(ctx)
}

type BoundType int

const (
	Following BoundType = iota
	Preceding
	CurrentRow
)

type FrameBound struct {
	Type      BoundType
	UnBounded bool
	Expr      Expr
}

func (node *FrameBound) Format(ctx *FmtCtx) {
	if node.UnBounded {
		ctx.WriteString("unbounded")
	}
	if node.Type == CurrentRow {
		ctx.WriteString("current row")
	} else {
		if node.Expr != nil {
			node.Expr.Format(ctx)
		}
		if node.Type == Preceding {
			ctx.WriteString(" preceding")
		} else {
			ctx.WriteString(" following")
		}
	}
}

// type reference
type ResolvableTypeReference interface {
}

var _ ResolvableTypeReference = &UnresolvedObjectName{}
var _ ResolvableTypeReference = &T{}

type SerialExtractExpr struct {
	exprImpl
	SerialExpr Expr
	IndexExpr  Expr
	ResultType ResolvableTypeReference
}

func (node *SerialExtractExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("serial_extract(")
	node.SerialExpr.Format(ctx)
	ctx.WriteString(", ")
	node.IndexExpr.Format(ctx)
	ctx.WriteString(" as ")
	node.ResultType.(*T).InternalType.Format(ctx)
	ctx.WriteByte(')')
}

// Accept implements NodeChecker interface
func (node *SerialExtractExpr) Accept(v Visitor) (Expr, bool) {
	//TODO: need validation from @iamlinjunhong

	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*SerialExtractExpr)

	tmpNode, ok := node.SerialExpr.Accept(v)
	if !ok {
		return node, false
	}
	node.SerialExpr = tmpNode

	tmpNode, ok = node.IndexExpr.Accept(v)
	if !ok {
		return node, false
	}
	node.IndexExpr = tmpNode

	return v.Exit(node)
}

func NewSerialExtractExpr(serialExpr Expr, indexExpr Expr, typ ResolvableTypeReference) *SerialExtractExpr {
	return &SerialExtractExpr{
		SerialExpr: serialExpr,
		IndexExpr:  indexExpr,
		ResultType: typ,
	}
}

// the Cast expression
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

// Accept implements NodeChecker interface
func (node *CastExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*CastExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewCastExpr(e Expr, t ResolvableTypeReference) *CastExpr {
	return &CastExpr{
		Expr: e,
		Type: t,
	}
}

type BitCastExpr struct {
	exprImpl
	Expr Expr
	Type ResolvableTypeReference
}

func (node *BitCastExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("bit_cast(")
	node.Expr.Format(ctx)
	ctx.WriteString(" as ")
	node.Type.(*T).InternalType.Format(ctx)
	ctx.WriteByte(')')
}

// Accept implements NodeChecker interface
func (node *BitCastExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*BitCastExpr)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewBitCastExpr(e Expr, t ResolvableTypeReference) *BitCastExpr {
	return &BitCastExpr{
		Expr: e,
		Type: t,
	}
}

// the parenthesized list of expressions.
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

// Accept implements NodeChecker interface
func (node *Tuple) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*Tuple)
	for i, val := range node.Exprs {
		tmpNode, ok := val.Accept(v)
		if !ok {
			return node, false
		}
		node.Exprs[i] = tmpNode
	}
	return v.Exit(node)
}

func NewTuple(e Exprs) *Tuple {
	return &Tuple{Exprs: e}
}

// the BETWEEN or a NOT BETWEEN expression
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

// Accept implements NodeChecker interface
func (node *RangeCond) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}

	node = newNode.(*RangeCond)
	tmpNode, ok := node.Left.Accept(v)
	if !ok {
		return node, false
	}
	node.Left = tmpNode

	tmpNode, ok = node.From.Accept(v)
	if !ok {
		return node, false
	}
	node.From = tmpNode

	tmpNode, ok = node.To.Accept(v)
	if !ok {
		return node, false
	}
	node.To = tmpNode

	return v.Exit(node)
}

func NewRangeCond(n bool, l, f, t Expr) *RangeCond {
	return &RangeCond{
		Not:  n,
		Left: l,
		From: f,
		To:   t,
	}
}

// Case-When expression.
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

// Accept implements NodeChecker interface
func (node *CaseExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*CaseExpr)

	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode

	for _, when := range node.Whens {
		tmpNode, ok = when.Cond.Accept(v)
		if !ok {
			return node, false
		}
		when.Cond = tmpNode

		tmpNode, ok = when.Val.Accept(v)
		if !ok {
			return node, false
		}
		when.Val = tmpNode
	}

	tmpNode, ok = node.Else.Accept(v)
	if !ok {
		return node, false
	}
	node.Else = tmpNode

	return v.Exit(node)
}

func NewCaseExpr(e Expr, w []*When, el Expr) *CaseExpr {
	return &CaseExpr{
		Expr:  e,
		Whens: w,
		Else:  el,
	}
}

// When sub-expression.
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

// INTERVAL / time unit
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

// Accept implements NodeChecker Accept interface.
func (node *IntervalExpr) Accept(v Visitor) (Expr, bool) {
	// TODO:
	panic("unimplement interval expr Accept")
}

func NewIntervalExpr(t IntervalType) *IntervalExpr {
	return &IntervalExpr{
		Type: t,
	}
}

// the DEFAULT expression.
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

// Accept implements NodeChecker interface
func (node *DefaultVal) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*DefaultVal)
	tmpNode, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = tmpNode
	return v.Exit(node)
}

func NewDefaultVal(e Expr) *DefaultVal {
	return &DefaultVal{
		Expr: e,
	}
}

type UpdateVal struct {
	exprImpl
}

func (node *UpdateVal) Format(ctx *FmtCtx) {}

// Accept implements NodeChecker interface
func (node *UpdateVal) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	return v.Exit(node)
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

// Accept implements NodeChecker Accept interface.
func (node *VarExpr) Accept(v Visitor) (Expr, bool) {
	panic("unimplement VarExpr Accept")
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

// Accept implements NodeChecker Accept interface.
func (node *ParamExpr) Accept(v Visitor) (Expr, bool) {
	panic("unimplement ParamExpr Accept")
}

func NewParamExpr(offset int) *ParamExpr {
	return &ParamExpr{
		Offset: offset,
	}
}

type MaxValue struct {
	exprImpl
}

func (node *MaxValue) Format(ctx *FmtCtx) {
	ctx.WriteString("MAXVALUE")
}

func NewMaxValue() *MaxValue {
	return &MaxValue{}
}

// Accept implements NodeChecker interface
func (node *MaxValue) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	return v.Exit(node)
}

// SampleExpr for sample(exprList, N rows / R percent)
type SampleExpr struct {
	// rows or percent.
	typ sampleType
	// sample level.
	level sampleLevel

	// N or K
	n int
	k float64

	// sample by '*'
	isStar bool
	// sample by columns.
	columns Exprs
}

func (s SampleExpr) String() string {
	return "sample"
}

func (s SampleExpr) Format(ctx *FmtCtx) {
	if s.typ == SampleRows {
		ctx.WriteString(fmt.Sprintf("sample %d rows", s.n))
	} else {
		ctx.WriteString(fmt.Sprintf("sample %.1f percent", s.k))
	}
}

func (s SampleExpr) Accept(v Visitor) (node Expr, ok bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	return v.Exit(node)
}

func (s SampleExpr) Valid() error {
	if s.typ == SampleRows {
		if s.n < 1 || s.n > 11_000 {
			return moerr.NewSyntaxErrorNoCtx("sample(expr list, N rows) requires N between 1 and 11000.")
		}
		return nil
	} else {
		if s.k < 0 || s.k > 100 {
			return moerr.NewSyntaxErrorNoCtx("sample(expr list, K percent) requires K between 0.00 and 100.00")
		}
		return nil
	}
}

func (s SampleExpr) GetColumns() (columns Exprs, isStar bool) {
	return s.columns, s.isStar
}

func (s SampleExpr) GetSampleDetail() (isSampleRows bool, usingRow bool, n int32, k float64) {
	return s.typ == SampleRows, s.level == SampleUsingRow, int32(s.n), s.k
}

type sampleType int
type sampleLevel int

const (
	SampleRows    sampleType = 0
	SamplePercent sampleType = 1

	SampleUsingBlock sampleLevel = 0
	SampleUsingRow   sampleLevel = 1
)

func NewSampleRowsFuncExpression(number int, isStar bool, columns Exprs, sampleUnit string) (*SampleExpr, error) {
	e := &SampleExpr{
		typ:     SampleRows,
		n:       number,
		k:       0,
		isStar:  isStar,
		columns: columns,
	}
	if len(sampleUnit) == 5 && strings.ToLower(sampleUnit) == "block" {
		e.level = SampleUsingBlock
		return e, nil
	}
	if len(sampleUnit) == 3 && strings.ToLower(sampleUnit) == "row" {
		e.level = SampleUsingRow
		return e, nil
	}
	return e, moerr.NewInternalErrorNoCtx("sample(expr, N rows, unit) only support unit 'block' or 'row'")
}

func NewSamplePercentFuncExpression1(percent int64, isStar bool, columns Exprs) (*SampleExpr, error) {
	return &SampleExpr{
		typ:     SamplePercent,
		n:       0,
		k:       float64(percent),
		isStar:  isStar,
		columns: columns,
	}, nil
}

func NewSamplePercentFuncExpression2(percent float64, isStar bool, columns Exprs) (*SampleExpr, error) {
	if nan := math.IsNaN(percent); nan {
		return nil, moerr.NewSyntaxErrorNoCtx("sample(expr, K percent) requires K between 0.00 and 100.00")
	}
	k, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", percent), 64)

	return &SampleExpr{
		typ:     SamplePercent,
		n:       0,
		k:       k,
		isStar:  isStar,
		columns: columns,
	}, nil
}
