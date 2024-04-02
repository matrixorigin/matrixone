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
	Select         SelectStatement
	TimeWindow     *TimeWindow
	OrderBy        OrderBy
	Limit          *Limit
	With           *With
	Ep             *ExportParam
	SelectLockInfo *SelectLockInfo
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
	if node.TimeWindow != nil {
		ctx.WriteByte(' ')
		node.TimeWindow.Format(ctx)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		node.Limit.Format(ctx)
	}
	if node.Ep != nil {
		ctx.WriteByte(' ')
		node.Ep.Format(ctx)
	}
	if node.SelectLockInfo != nil {
		ctx.WriteByte(' ')
		node.SelectLockInfo.Format(ctx)
	}
}

func (node *Select) GetStatementType() string { return "Select" }
func (node *Select) GetQueryType() string     { return QueryTypeDQL }

func NewSelect(s SelectStatement, o OrderBy, l *Limit) *Select {
	return &Select{
		Select:  s,
		OrderBy: o,
		Limit:   l,
	}
}

type TimeWindow struct {
	Interval *Interval
	Sliding  *Sliding
	Fill     *Fill
}

func (node *TimeWindow) Format(ctx *FmtCtx) {
	node.Interval.Format(ctx)
	if node.Sliding != nil {
		ctx.WriteByte(' ')
		node.Sliding.Format(ctx)
	}
	if node.Fill != nil {
		ctx.WriteByte(' ')
		node.Fill.Format(ctx)
	}
}

type Interval struct {
	Col  *UnresolvedName
	Val  Expr
	Unit string
}

func (node *Interval) Format(ctx *FmtCtx) {
	ctx.WriteString("interval(")
	node.Col.Format(ctx)
	ctx.WriteString(", ")
	node.Val.Format(ctx)
	ctx.WriteString(", ")
	ctx.WriteString(node.Unit)
	ctx.WriteByte(')')
}

type Sliding struct {
	Val  Expr
	Unit string
}

func (node *Sliding) Format(ctx *FmtCtx) {
	ctx.WriteString("sliding(")
	node.Val.Format(ctx)
	ctx.WriteString(", ")
	ctx.WriteString(node.Unit)
	ctx.WriteByte(')')
}

type FillMode int

const (
	FillNone FillMode = iota
	FillPrev
	FillNext
	FillValue
	FillNull
	FillLinear
)

func (f FillMode) String() string {
	switch f {
	case FillNone:
		return "none"
	case FillPrev:
		return "prev"
	case FillNext:
		return "next"
	case FillValue:
		return "value"
	case FillNull:
		return "null"
	case FillLinear:
		return "linear"
	default:
		return ""
	}
}

type Fill struct {
	Mode FillMode
	Val  Expr
}

func (node *Fill) Format(ctx *FmtCtx) {
	ctx.WriteString("fill(")
	ctx.WriteString(node.Mode.String())

	if node.Mode == FillValue {
		ctx.WriteString(", ")
		node.Val.Format(ctx)
	}
	ctx.WriteByte(')')
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

// the ordering expression.
type Order struct {
	Expr          Expr
	Direction     Direction
	NullsPosition NullsPosition
	//without order
	NullOrder bool
}

func (node *Order) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
	if node.NullsPosition != DefaultNullsPosition {
		ctx.WriteByte(' ')
		ctx.WriteString(node.NullsPosition.String())
	}
}

func NewOrder(e Expr, d Direction, np NullsPosition, o bool) *Order {
	return &Order{
		Expr:          e,
		Direction:     d,
		NullsPosition: np,
		NullOrder:     o,
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

type NullsPosition int8

const (
	DefaultNullsPosition NullsPosition = iota
	NullsFirst
	NullsLast
)

var nullsPositionName = [...]string{
	DefaultNullsPosition: "",
	NullsFirst:           "nulls first",
	NullsLast:            "nulls last",
}

func (np NullsPosition) String() string {
	if np < 0 || np >= NullsPosition(len(nullsPositionName)) {
		return fmt.Sprintf("NullsPosition(%d)", np)
	}
	return nullsPositionName[np]
}

// the LIMIT clause.
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

func (node *SelectClause) GetStatementType() string { return "Select" }
func (node *SelectClause) GetQueryType() string     { return QueryTypeDQL }

// WHERE or HAVING clause.
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

// SELECT expressions.
type SelectExprs []SelectExpr

func (node *SelectExprs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		n.Format(ctx)
	}
}

// a SELECT expression.
type SelectExpr struct {
	exprImpl
	Expr Expr
	As   *CStr
}

func (node *SelectExpr) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
	if node.As != nil && !node.As.Empty() {
		ctx.WriteString(" as ")
		ctx.WriteString(node.As.Origin())
	}
}

// a GROUP BY clause.
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

// the table expression
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
	if node.JoinType != "" && node.Right != nil {
		ctx.WriteByte(' ')
		ctx.WriteString(strings.ToLower(node.JoinType))
	}
	if node.JoinType != JOIN_TYPE_STRAIGHT && node.Right != nil {
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

// the join condition.
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

// the ON condition for join
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

// the USING condition
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

// the parenthesized TableExpr.
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

// The alias, optionally with a column list:
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

// the table expression coupled with an optional alias.
type AliasedTableExpr struct {
	TableExpr
	Expr       TableExpr
	As         AliasClause
	IndexHints []*IndexHint
}

func (node *AliasedTableExpr) Format(ctx *FmtCtx) {
	node.Expr.Format(ctx)
	if node.As.Alias != "" {
		ctx.WriteString(" as ")
		node.As.Format(ctx)
	}
	if node.IndexHints != nil {
		prefix := " "
		for _, hint := range node.IndexHints {
			ctx.WriteString(prefix)
			hint.Format(ctx)
			prefix = " "
		}
	}
}

func NewAliasedTableExpr(e TableExpr, a AliasClause) *AliasedTableExpr {
	return &AliasedTableExpr{
		Expr: e,
		As:   a,
	}
}

// the statements as a data source includes the select statement.
type StatementSource struct {
	TableExpr
	Statement Statement
}

func NewStatementSource(s Statement) *StatementSource {
	return &StatementSource{
		Statement: s,
	}
}

// the list of table expressions.
type TableExprs []TableExpr

func (node *TableExprs) Format(ctx *FmtCtx) {
	prefix := ""
	for _, n := range *node {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = ", "
	}
}

// the FROM clause.
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

type IndexHintType int

const (
	HintUse IndexHintType = iota + 1
	HintIgnore
	HintForce
)

type IndexHintScope int

// Index hint scopes.
const (
	HintForScan IndexHintScope = iota + 1
	HintForJoin
	HintForOrderBy
	HintForGroupBy
)

type IndexHint struct {
	IndexNames []string
	HintType   IndexHintType
	HintScope  IndexHintScope
}

func (node *IndexHint) Format(ctx *FmtCtx) {
	indexHintType := ""
	switch node.HintType {
	case HintUse:
		indexHintType = "use index"
	case HintIgnore:
		indexHintType = "ignore index"
	case HintForce:
		indexHintType = "force index"
	}

	indexHintScope := ""
	switch node.HintScope {
	case HintForScan:
		indexHintScope = ""
	case HintForJoin:
		indexHintScope = " for join"
	case HintForOrderBy:
		indexHintScope = " for order by"
	case HintForGroupBy:
		indexHintScope = " for group by"
	}
	ctx.WriteString(indexHintType)
	ctx.WriteString(indexHintScope)
	ctx.WriteString("(")
	if node.IndexNames != nil {
		for i, value := range node.IndexNames {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.WriteString(value)
		}
	}
	ctx.WriteString(")")
}
