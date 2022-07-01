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
	"strings"
)

type SelectStatement interface {
	Statement
}

// Select represents a SelectStatement with an ORDER and/or LIMIT.
type Select struct {
	statementImpl
	Select  SelectStatement
	OrderBy OrderBy
	Limit   *Limit
	With    *With
	Ep      *ExportParam
}

func (node *Select) Format(ctx *FmtCtx) {
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	node.Select.Format(ctx)
	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		node.OrderBy.Format(ctx)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		node.Limit.Format(ctx)
	}
	if node.Ep != nil {
		ctx.WriteByte(' ')
		node.Ep.Format(ctx)
	}
}

func NewSelect(s SelectStatement, o OrderBy, l *Limit) *Select {
	return &Select{
		Select:  s,
		OrderBy: o,
		Limit:   l,
	}
}

// OrderBy represents an ORDER BY clause.
type OrderBy []*Order

func (node *OrderBy) Format(ctx *FmtCtx) {
	prefix := "order by "
	for _, n := range *node {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = ", "
	}
}

//the ordering expression.
type Order struct {
	Expr      Expr
	Direction Direction
	//without order
	NullOrder bool
}

func (node *Order) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
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
	DefaultDirection Direction = iota
	Ascending
	Descending
)

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "asc",
	Descending:       "desc",
}

func (d Direction) String() string {
	if d < 0 || d > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", d)
	}
	return directionName[d]
}

//the LIMIT clause.
type Limit struct {
	Offset, Count Expr
}

func (node *Limit) Format(ctx *FmtCtx) {
	needSpace := false
	if node != nil && node.Count != nil {
		ctx.WriteString("limit ")
		node.Count.Format(ctx)
		needSpace = true
	}
	if node != nil && node.Offset != nil {
		if needSpace {
			ctx.WriteByte(' ')
		}
		ctx.WriteString("offset ")
		node.Offset.Format(ctx)
	}
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

func (node *ParenSelect) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	node.Select.Format(ctx)
	ctx.WriteByte(')')
}

// SelectClause represents a SELECT statement.
type SelectClause struct {
	SelectStatement
	Distinct bool
	Exprs    SelectExprs
	From     *From
	Where    *Where
	GroupBy  GroupBy
	Having   *Where
	Option   string
}

func (node *SelectClause) Format(ctx *FmtCtx) {
	ctx.WriteString("select ")
	if node.Distinct {
		ctx.WriteString("distinct ")
	}
	if node.Option != "" {
		ctx.WriteString(node.Option)
		ctx.WriteByte(' ')
	}
	node.Exprs.Format(ctx)
	if len(node.From.Tables) > 0 {
		canFrom := true
		als, ok := node.From.Tables[0].(*AliasedTableExpr)
		if ok {
			tbl, ok := als.Expr.(*TableName)
			if ok {
				if string(tbl.ObjectName) == "" {
					canFrom = false
				}
			}
		}
		if canFrom {
			ctx.WriteByte(' ')
			node.From.Format(ctx)
		}
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
	if len(node.GroupBy) > 0 {
		ctx.WriteByte(' ')
		node.GroupBy.Format(ctx)
	}
	if node.Having != nil {
		ctx.WriteByte(' ')
		node.Having.Format(ctx)
	}
}

//WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

func (node *Where) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Type)
	ctx.WriteByte(' ')
	node.Expr.Format(ctx)
}

const (
	AstWhere  = "where"
	AstHaving = "having"
)

func NewWhere(e Expr) *Where {
	return &Where{Expr: e}
}

//SELECT expressions.
type SelectExprs []SelectExpr

func (node *SelectExprs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		n.Format(ctx)
	}
}

//a SELECT expression.
type SelectExpr struct {
	exprImpl
	Expr Expr
	As   UnrestrictedIdentifier
}

func (node *SelectExpr) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
	if node.As != "" {
		ctx.WriteString(" as ")
		ctx.WriteString(string(node.As))
	}
}

//a GROUP BY clause.
type GroupBy []Expr

func (node *GroupBy) Format(ctx *FmtCtx) {
	prefix := "group by "
	for _, n := range *node {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = ", "
	}
}

const (
	JOIN_TYPE_FULL          = "FULL"
	JOIN_TYPE_LEFT          = "LEFT"
	JOIN_TYPE_RIGHT         = "RIGHT"
	JOIN_TYPE_CROSS         = "CROSS"
	JOIN_TYPE_INNER         = "INNER"
	JOIN_TYPE_STRAIGHT      = "STRAIGHT_JOIN"
	JOIN_TYPE_NATURAL       = "NATURAL"
	JOIN_TYPE_NATURAL_LEFT  = "NATURAL LEFT"
	JOIN_TYPE_NATURAL_RIGHT = "NATURAL RIGHT"
)

//the table expression
type TableExpr interface {
	NodeFormatter
}

var _ TableExpr = &Subquery{}

type JoinTableExpr struct {
	TableExpr
	JoinType string
	Left     TableExpr
	Right    TableExpr
	Cond     JoinCond
}

func (node *JoinTableExpr) Format(ctx *FmtCtx) {
	if node.Left != nil {
		node.Left.Format(ctx)
	}
	if node.JoinType != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(strings.ToLower(node.JoinType))
	}
	if node.JoinType != JOIN_TYPE_STRAIGHT {
		ctx.WriteByte(' ')
		ctx.WriteString("join")
	}
	if node.Right != nil {
		ctx.WriteByte(' ')
		node.Right.Format(ctx)
	}
	if node.Cond != nil {
		ctx.WriteByte(' ')
		node.Cond.Format(ctx)
	}
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
	NodeFormatter
}

// the NATURAL join condition
type NaturalJoinCond struct {
	JoinCond
}

func (node *NaturalJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("natural")
}

func NewNaturalJoinCond() *NaturalJoinCond {
	return &NaturalJoinCond{}
}

//the ON condition for join
type OnJoinCond struct {
	JoinCond
	Expr Expr
}

func (node *OnJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("on ")
	node.Expr.Format(ctx)
}

func NewOnJoinCond(e Expr) *OnJoinCond {
	return &OnJoinCond{Expr: e}
}

//the USING condition
type UsingJoinCond struct {
	JoinCond
	Cols IdentifierList
}

func (node *UsingJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("using (")
	node.Cols.Format(ctx)
	ctx.WriteByte(')')
}

func NewUsingJoinCond(c IdentifierList) *UsingJoinCond {
	return &UsingJoinCond{Cols: c}
}

//the parenthesized TableExpr.
type ParenTableExpr struct {
	TableExpr
	Expr TableExpr
}

func (node *ParenTableExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	node.Expr.Format(ctx)
	ctx.WriteByte(')')
}

func NewParenTableExpr(e TableExpr) *ParenTableExpr {
	return &ParenTableExpr{Expr: e}
}

//The alias, optionally with a column list:
// "AS name" or "AS name(col1, col2)".
type AliasClause struct {
	NodeFormatter
	Alias Identifier
	Cols  IdentifierList
}

func (node *AliasClause) Format(ctx *FmtCtx) {
	if node.Alias != "" {
		ctx.WriteString(string(node.Alias))
	}
	if node.Cols != nil {
		ctx.WriteByte('(')
		node.Cols.Format(ctx)
		ctx.WriteByte(')')
	}
}

//the table expression coupled with an optional alias.
type AliasedTableExpr struct {
	TableExpr
	Expr TableExpr
	As   AliasClause
}

func (node *AliasedTableExpr) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
	if node.As.Alias != "" {
		ctx.WriteString(" as ")
		node.As.Format(ctx)
	}
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

func (node *TableExprs) Format(ctx *FmtCtx) {
	prefix := ""
	for _, n := range *node {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = ", "
	}
}

//the FROM clause.
type From struct {
	Tables TableExprs
}

func (node *From) Format(ctx *FmtCtx) {
	ctx.WriteString("from ")
	node.Tables.Format(ctx)
}

func NewFrom(t TableExprs) *From {
	return &From{Tables: t}
}
