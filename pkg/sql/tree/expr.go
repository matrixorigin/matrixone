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
	DIV         // /
	INTEGER_DIV //
	BIT_OR      // |
	BIT_AND     // &
	BIT_XOR     // ^
	LEFT_SHIFT  // <<
	RIGHT_SHIFT // >>
	MOD         // %
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
	Expr Expr
}

func NewUnaryExpr(op UnaryOp, expr Expr) *UnaryExpr {
	return &UnaryExpr{
		Op:   op,
		Expr: expr,
	}
}

func (e *UnaryExpr) String() string {
	return unaryOpName[e.Op] + e.Expr.String()
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

	//reference: https://dev.mysql.com/doc/refman/8.0/en/all-subqueries.html
	//subquery with ANY,SOME,ALL
	//operand comparison_operator [ANY | SOME | ALL] (subquery)
	ANY
	SOME
	ALL
)

var comparionName = []string{
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
	Left  Expr
	Right Expr
}

func NewComparisonExpr(op ComparisonOp, l, r Expr) *ComparisonExpr {
	return &ComparisonExpr{
		Op:    op,
		SubOp: ComparisonOp(0),
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

//and expression
type AndExpr struct {
	exprImpl
	Left, Right Expr
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

func NewSubquery(s SelectStatement, e bool) *Subquery {
	return &Subquery{
		Select: s,
		Exists: e,
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

func NewParenExpr(e Expr) *ParenExpr {
	return &ParenExpr{
		Expr: e,
	}
}

type funcType int

const (
	_ funcType = iota
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
	NodePrinter
}

var _ FunctionReference = &UnresolvedName{}

//function reference
type ResolvableFunctionReference struct {
	FunctionReference
}

func FuncName2ResolvableFunctionReference(funcName *UnresolvedName) ResolvableFunctionReference {
	return ResolvableFunctionReference{FunctionReference: funcName}
}

//function call expression
type FuncExpr struct {
	exprImpl
	Func  ResolvableFunctionReference
	Type  funcType
	Exprs Exprs

	//specify the type of aggregation.
	AggType AggType

	//aggregations which specify an order.
	OrderBy OrderBy
}

func NewFuncExpr(ft funcType, name *UnresolvedName, e Exprs, order OrderBy) *FuncExpr {
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

func NewWhen(c, v Expr) *When {
	return &When{
		Cond: c,
		Val:  v,
	}
}

// IntervalType is the type for time and timestamp units.
type IntervalType int

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
	Type IntervalType
}

func NewIntervalExpr(t IntervalType) *IntervalExpr {
	return &IntervalExpr{
		Type: t,
	}
}

//the DEFAULT expression.
type DefaultVal struct {
	exprImpl
}

func NewDefaultVal() *DefaultVal {
	return &DefaultVal{}
}

type MaxValue struct {
	exprImpl
}

func NewMaxValue() *MaxValue {
	return &MaxValue{}
}

/*
Variable Expression Used in Set Statement,
Load DataSource statement, Show statement,etc.
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

func NewVarExpr(n string, s bool, g bool, e Expr) *VarExpr {
	return &VarExpr{
		Name:   n,
		System: s,
		Global: g,
		Expr:   e,
	}
}
