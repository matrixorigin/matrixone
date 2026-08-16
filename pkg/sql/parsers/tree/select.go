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
	"reflect"
	"sort"
	"strings"
)

type SelectStatement interface {
	Statement
}

// SelectInto records either a SELECT INTO OUTFILE clause or MySQL's SELECT
// expression INTO @user_variable form while the parser constructs a Select.
type SelectInto struct {
	Export   *ExportParam
	UserVars []*VarExpr
}

// Select represents a SelectStatement with an ORDER and/or LIMIT.
type Select struct {
	statementImpl
	IsPerform      bool
	Select         SelectStatement
	RewriteOption  *RewriteOption
	TimeWindow     *TimeWindow
	OrderBy        OrderBy
	Limit          *Limit
	RankOption     *RankOption
	With           *With
	Ep             *ExportParam
	IntoVars       []*VarExpr
	DeprecatedInto bool
	SelectLockInfo *SelectLockInfo
}

func (node *Select) Format(ctx *FmtCtx) {
	if node.IsPerform {
		ctx.WriteString("perform ")
	}
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
	if node.RankOption != nil {
		ctx.WriteByte(' ')
		node.RankOption.Format(ctx)
	}
	if node.Ep != nil && SelectIntoExport(node.Select) == nil {
		ctx.WriteByte(' ')
		node.Ep.Format(ctx)
	} else if len(node.IntoVars) > 0 && len(SelectIntoVariables(node.Select)) == 0 {
		ctx.WriteString(" into ")
		for i, variable := range node.IntoVars {
			if i > 0 {
				ctx.WriteString(", ")
			}
			variable.Format(ctx)
		}
	}
	if node.SelectLockInfo != nil {
		ctx.WriteByte(' ')
		node.SelectLockInfo.Format(ctx)
	}
}

func (node *Select) GetStatementType() string {
	if node.IsPerform {
		return "Perform"
	}
	return "Select"
}
func (node *Select) GetQueryType() string { return QueryTypeDQL }

func NewSelect(s SelectStatement, o OrderBy, l *Limit) *Select {
	return &Select{
		Select:  s,
		OrderBy: o,
		Limit:   l,
	}
}

type RewriteOption struct {
	// key: db.table or table.
	// Each key maps to an ordered chain of rewrites applied as stacked views:
	// element 0 is the innermost layer (closest to the base table) and the last
	// element is the outermost layer (what the query's table reference resolves
	// to). A reference to the same table inside one layer's body resolves to the
	// next inner layer; once the chain is exhausted it resolves to the base
	// table.
	Rewrites map[string][]*Rewrite
	// RemapDb maps a source database name to a target database name. It is
	// applied before the table Rewrites: a reference to <src>.t (or an
	// unqualified table when the current database is <src>) is resolved against
	// <dst> instead.
	RemapDb map[string]string
}

type Rewrite struct {
	TableName string
	DbName    string
	Stmt      Statement
}

type TimeWindow struct {
	Interval *Interval
	Sliding  *Sliding
	GapFill  bool
	Fill     *Fill
}

func (node *TimeWindow) Format(ctx *FmtCtx) {
	node.Interval.Format(ctx)
	if node.Sliding != nil {
		ctx.WriteByte(' ')
		node.Sliding.Format(ctx)
	}
	if node.GapFill {
		ctx.WriteString(" gapfill(partition)")
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

type RankOption struct {
	Option map[string]string
}

func (node *RankOption) Format(ctx *FmtCtx) {
	if node == nil {
		return
	}

	ctx.WriteString("by rank")
	if len(node.Option) == 0 {
		return
	}

	ctx.WriteString(" with option ")
	keys := make([]string, 0, len(node.Option))
	for key := range node.Option {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for idx, key := range keys {
		if idx > 0 {
			ctx.WriteString(", ")
		}
		ctx.WriteByte('\'')
		ctx.WriteString(key)
		ctx.WriteString("=")
		ctx.WriteString(node.Option[key])
		ctx.WriteByte('\'')
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

const (
	QuerySpecOptionNone             uint64 = 0
	QuerySpecOptionAll              uint64 = 1 << 1
	QuerySpecOptionDistinct         uint64 = 1 << 2
	QuerySpecOptionDistinctRow      uint64 = 1 << 3
	QuerySpecOptionHighPriority     uint64 = 1 << 4
	QuerySpecOptionStraightJoin     uint64 = 1 << 5
	QuerySpecOptionSqlSmallResult   uint64 = 1 << 6
	QuerySpecOptionSqlBigResult     uint64 = 1 << 7
	QuerySpecOptionSqlBufferResult  uint64 = 1 << 8
	QuerySpecOptionSqlNoCache       uint64 = 1 << 9
	QuerySpecOptionSqlCalcFoundRows uint64 = 1 << 10
)

var (
	QuerySpecOptionNames = map[uint64]string{
		QuerySpecOptionNone:             "none",
		QuerySpecOptionAll:              "all",
		QuerySpecOptionDistinct:         "distinct",
		QuerySpecOptionDistinctRow:      "distinctrow",
		QuerySpecOptionHighPriority:     "high_priority",
		QuerySpecOptionStraightJoin:     "straight_join",
		QuerySpecOptionSqlSmallResult:   "sql_small_result",
		QuerySpecOptionSqlBigResult:     "sql_big_result",
		QuerySpecOptionSqlBufferResult:  "sql_buffer_result",
		QuerySpecOptionSqlNoCache:       "sql_no_cache",
		QuerySpecOptionSqlCalcFoundRows: "sql_calc_found_rows",
	}
)

// SelectClause represents a SELECT statement.
type SelectClause struct {
	SelectStatement
	Distinct bool
	Exprs    SelectExprs
	// IntoVars is populated for MySQL's pre-FROM SELECT ... INTO @var form.
	// The enclosing Select also copies it so frontend execution can handle both
	// the pre-FROM and terminal placements uniformly.
	IntoVars   []*VarExpr
	IntoExport *ExportParam
	From       *From
	Where      *Where
	GroupBy    *GroupByClause
	Having     *Where
	Option     uint64
	// OrderByOriginalExprs is planner-internal metadata for a generated
	// projection whose derived-table columns must retain the original output
	// expression categories for ORDER BY duplicate-name resolution.
	OrderByOriginalExprs []Expr
	// OrderBySourceProbes is shared with generated grouping-set branches. Once
	// their real FROM scope is bound, it tells the outer ORDER BY whether a
	// potentially shadowed name denotes a source column or an output alias.
	OrderBySourceProbes map[string]*GroupingSetOrderSourceProbe
}

func (node *SelectClause) Format(ctx *FmtCtx) {
	ctx.WriteString("select ")
	if node.Distinct {
		ctx.WriteString("distinct ")
	}
	if node.Option != 0 {
		for i := uint64(1); i <= 10; i++ {
			opt := uint64(1 << i)
			//distinct printed already
			if opt == QuerySpecOptionDistinct || opt == QuerySpecOptionDistinctRow {
				continue
			}

			if node.Option&opt != 0 {
				ctx.WriteString(QuerySpecOptionNames[opt])
				ctx.WriteByte(' ')
			}
		}
	}
	node.Exprs.Format(ctx)
	if node.IntoExport != nil {
		ctx.WriteByte(' ')
		node.IntoExport.Format(ctx)
	} else if len(node.IntoVars) > 0 {
		ctx.WriteString(" into ")
		for i, variable := range node.IntoVars {
			if i > 0 {
				ctx.WriteString(", ")
			}
			variable.Format(ctx)
		}
	}
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
	if node.GroupBy != nil {
		ctx.WriteByte(' ')
		node.GroupBy.Format(ctx)
	}
	if node.Having != nil {
		ctx.WriteByte(' ')
		node.Having.Format(ctx)
	}
}

// SelectIntoVariables returns variables attached to the pre-FROM SELECT
// clause. It intentionally does not clear the clause field: tree formatting
// needs the original placement while the enclosing Select uses the same list
// for execution.
func SelectIntoVariables(stmt SelectStatement) []*VarExpr {
	vars, _, _ := SelectIntoVariablesForTopLevel(stmt)
	return vars
}

const MisplacedIntoClauseMessage = "Misplaced INTO clause, INTO is not allowed inside subqueries, and must be placed at end of UNION clauses."
const PerformIntoClauseMessage = "INTO is not allowed with PERFORM statements."

func SelectIntoVariablesForTopLevel(stmt SelectStatement) (vars []*VarExpr, deprecated bool, err string) {
	return selectIntoVariablesForTopLevel(stmt, false)
}

func selectIntoVariablesForTopLevel(stmt SelectStatement, insideUnion bool) (vars []*VarExpr, deprecated bool, err string) {
	switch node := stmt.(type) {
	case *Select:
		if len(node.IntoVars) > 0 {
			return node.IntoVars, insideUnion || node.DeprecatedInto, ""
		}
		vars, deprecated, err = selectIntoVariablesForTopLevel(node.Select, insideUnion)
		if err != "" {
			return nil, false, err
		}
		return vars, deprecated || node.DeprecatedInto, ""
	case *SelectClause:
		return node.IntoVars, insideUnion && len(node.IntoVars) > 0, ""
	case *ParenSelect:
		return selectIntoVariablesForTopLevel(node.Select, insideUnion)
	case *UnionClause:
		if selectTreeHasInto(node.Left) || selectTreeHasExport(node.Left) {
			return nil, false, MisplacedIntoClauseMessage
		}
		vars, deprecated, err = selectIntoVariablesForTopLevel(node.Right, true)
		if err != "" {
			return nil, false, err
		}
		if len(vars) > 0 {
			deprecated = true
		}
		return vars, deprecated, ""
	default:
		return nil, false, ""
	}
}

func SelectIntoActionConflict(stmt SelectStatement, suffix *SelectInto) bool {
	actions := 0
	if vars, _, _ := SelectIntoVariablesForTopLevel(stmt); len(vars) > 0 {
		actions++
	}
	if SelectIntoExport(stmt) != nil {
		actions++
	}
	if suffix != nil {
		if len(suffix.UserVars) > 0 {
			actions++
		}
		if suffix.Export != nil {
			actions++
		}
	}
	return actions > 1
}

func ValidateSelectIntoPlacement(stmt *Select) string {
	if stmt == nil {
		return ""
	}
	if stmt.IsPerform && (len(stmt.IntoVars) > 0 || selectTreeHasInto(stmt.Select)) {
		return PerformIntoClauseMessage
	}
	if withHasInto(stmt.With) ||
		selectStatementHasNestedInto(stmt.Select) ||
		timeWindowHasInto(stmt.TimeWindow) ||
		orderByHasInto(stmt.OrderBy) ||
		limitHasInto(stmt.Limit) {
		return MisplacedIntoClauseMessage
	}
	return ""
}

// ValidateSelectIntoNotAllowed rejects SELECT ... INTO actions when the
// SELECT is embedded in a statement that has no owner for the assignment.
// A top-level SELECT is allowed to own its INTO clause, so callers for
// enclosing statements must use this check instead of
// ValidateSelectIntoPlacement.
func ValidateSelectIntoNotAllowed(stmt *Select) string {
	if stmt == nil {
		return ""
	}
	if selectStatementHasInto(stmt) || withHasInto(stmt.With) ||
		selectStatementHasNestedInto(stmt.Select) || timeWindowHasInto(stmt.TimeWindow) ||
		orderByHasInto(stmt.OrderBy) || limitHasInto(stmt.Limit) {
		return MisplacedIntoClauseMessage
	}
	return ""
}

// ValidateSelectIntoEnclosingStatement applies the no-owner rule to the
// statement forms which can contain a SELECT source.  EXPLAIN uses this
// helper so it cannot accidentally accept an assignment that would only be
// meaningful when the explained SELECT is executed directly.
func ValidateSelectIntoEnclosingStatement(stmt Statement) string {
	switch node := stmt.(type) {
	case *Select:
		return ValidateSelectIntoNotAllowed(node)
	case *Insert:
		if err := ValidateSelectIntoNotAllowed(node.Rows); err != "" {
			return err
		}
		if withHasInto(node.With) {
			return MisplacedIntoClauseMessage
		}
		if updateExprsHaveInto(node.OnDuplicateUpdate) || selectExprsHaveInto(node.Returning) {
			return MisplacedIntoClauseMessage
		}
		return ""
	case *Replace:
		if err := ValidateSelectIntoNotAllowed(node.Rows); err != "" {
			return err
		}
		if selectExprsHaveInto(node.Returning) {
			return MisplacedIntoClauseMessage
		}
		return ""
	case *Delete:
		if withHasInto(node.With) || tableExprsHaveInto(node.Tables) ||
			tableExprsHaveInto(node.TableRefs) || selectExprsHaveInto(node.Returning) ||
			(node.Where != nil && exprHasNestedInto(node.Where.Expr)) ||
			orderByHasInto(node.OrderBy) || limitHasInto(node.Limit) {
			return MisplacedIntoClauseMessage
		}
		return ""
	case *Update:
		if withHasInto(node.With) || tableExprsHaveInto(node.Tables) ||
			(node.From != nil && tableExprsHaveInto(node.From.Tables)) ||
			updateExprsHaveInto(node.Exprs) || selectExprsHaveInto(node.Returning) ||
			(node.Where != nil && exprHasNestedInto(node.Where.Expr)) ||
			orderByHasInto(node.OrderBy) || limitHasInto(node.Limit) {
			return MisplacedIntoClauseMessage
		}
		return ""
	default:
		return ""
	}
}

func updateExprsHaveInto(exprs UpdateExprs) bool {
	for _, expr := range exprs {
		if expr != nil && exprHasNestedInto(expr.Expr) {
			return true
		}
	}
	return false
}

func ValidateValuesIntoPlacement(stmt *ValuesStatement) string {
	if stmt == nil {
		return ""
	}
	for _, row := range stmt.Rows {
		if exprsHaveInto(row) {
			return MisplacedIntoClauseMessage
		}
	}
	if orderByHasInto(stmt.OrderBy) || limitHasInto(stmt.Limit) {
		return MisplacedIntoClauseMessage
	}
	return ""
}

func ValidatePerformSelectIntoPlacement(stmt *Select) string {
	if stmt == nil {
		return ""
	}
	if selectTreeHasUserVariableInto(reflect.ValueOf(stmt)) {
		return PerformIntoClauseMessage
	}
	return ""
}

func selectTreeHasUserVariableInto(value reflect.Value) bool {
	if !value.IsValid() {
		return false
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		if value.CanInterface() {
			switch node := value.Interface().(type) {
			case *Select:
				if len(node.IntoVars) > 0 {
					return true
				}
			case *SelectClause:
				if len(node.IntoVars) > 0 {
					return true
				}
			}
		}
		return selectTreeHasUserVariableInto(value.Elem())
	}

	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if selectTreeHasUserVariableInto(value.Field(i)) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if selectTreeHasUserVariableInto(value.Index(i)) {
				return true
			}
		}
	}
	return false
}

func withHasInto(with *With) bool {
	if with == nil {
		return false
	}
	for _, cte := range with.CTEs {
		if cte != nil && statementHasInto(cte.Stmt) {
			return true
		}
	}
	return false
}

func statementHasInto(stmt Statement) bool {
	switch node := stmt.(type) {
	case *Select:
		return len(node.IntoVars) > 0 || node.Ep != nil ||
			withHasInto(node.With) || selectTreeHasInto(node.Select) ||
			selectTreeHasExport(node.Select) || selectStatementHasNestedInto(node.Select) ||
			timeWindowHasInto(node.TimeWindow) || orderByHasInto(node.OrderBy) ||
			limitHasInto(node.Limit)
	case *ValuesStatement:
		return ValidateValuesIntoPlacement(node) != ""
	default:
		return false
	}
}

func selectStatementHasNestedInto(stmt SelectStatement) bool {
	switch node := stmt.(type) {
	case *Select:
		return withHasInto(node.With) ||
			selectStatementHasNestedInto(node.Select) ||
			timeWindowHasInto(node.TimeWindow) ||
			orderByHasInto(node.OrderBy) ||
			limitHasInto(node.Limit)
	case *SelectClause:
		if selectExprsHaveInto(node.Exprs) {
			return true
		}
		if node.From != nil && tableExprsHaveInto(node.From.Tables) {
			return true
		}
		if node.Where != nil && exprHasNestedInto(node.Where.Expr) {
			return true
		}
		if node.Having != nil && exprHasNestedInto(node.Having.Expr) {
			return true
		}
		if groupByHasInto(node.GroupBy) {
			return true
		}
		return false
	case *ParenSelect:
		return selectStatementHasNestedInto(node.Select)
	case *UnionClause:
		return selectStatementHasNestedInto(node.Left) || selectStatementHasNestedInto(node.Right)
	case *ValuesClause:
		for _, row := range node.Rows {
			if exprsHaveInto(row) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func selectExprsHaveInto(exprs SelectExprs) bool {
	for _, expr := range exprs {
		if exprHasNestedInto(expr.Expr) {
			return true
		}
	}
	return false
}

func tableExprsHaveInto(exprs TableExprs) bool {
	for _, expr := range exprs {
		if tableExprHasInto(expr) {
			return true
		}
	}
	return false
}

func tableExprHasInto(expr TableExpr) bool {
	switch node := expr.(type) {
	case nil:
		return false
	case *Subquery:
		return selectStatementHasInto(node.Select)
	case *AliasedTableExpr:
		return tableExprHasInto(node.Expr)
	case *ParenTableExpr:
		return tableExprHasInto(node.Expr)
	case *JoinTableExpr:
		return tableExprHasInto(node.Left) || tableExprHasInto(node.Right) || joinCondHasInto(node.Cond)
	case *ApplyTableExpr:
		return tableExprHasInto(node.Left) || tableExprHasInto(node.Right)
	case *TableFunction:
		return exprHasNestedInto(node.Func) ||
			(node.SelectStmt != nil && selectStatementHasInto(node.SelectStmt))
	case *TableName:
		if node.AtTsExpr != nil && exprHasNestedInto(node.AtTsExpr.Expr) {
			return true
		}
		if node.IcebergRef != nil {
			return exprHasNestedInto(node.IcebergRef.Snapshot) ||
				exprHasNestedInto(node.IcebergRef.Timestamp)
		}
		return false
	case *StatementSource:
		return statementHasInto(node.Statement)
	case SelectStatement:
		return selectStatementHasInto(node)
	default:
		return false
	}
}

func joinCondHasInto(cond JoinCond) bool {
	switch node := cond.(type) {
	case nil:
		return false
	case *OnJoinCond:
		return exprHasNestedInto(node.Expr)
	default:
		return false
	}
}

func groupByHasInto(groupBy *GroupByClause) bool {
	if groupBy == nil {
		return false
	}
	for _, exprs := range groupBy.GroupByExprsList {
		if exprsHaveInto(exprs) {
			return true
		}
	}
	return exprsHaveInto(groupBy.GroupingSet)
}

func orderByHasInto(orderBy OrderBy) bool {
	for _, order := range orderBy {
		if order != nil && exprHasNestedInto(order.Expr) {
			return true
		}
	}
	return false
}

func limitHasInto(limit *Limit) bool {
	return limit != nil && (exprHasNestedInto(limit.Offset) || exprHasNestedInto(limit.Count))
}

func timeWindowHasInto(timeWindow *TimeWindow) bool {
	if timeWindow == nil {
		return false
	}
	if timeWindow.Interval != nil && exprHasNestedInto(timeWindow.Interval.Val) {
		return true
	}
	if timeWindow.Sliding != nil && exprHasNestedInto(timeWindow.Sliding.Val) {
		return true
	}
	return timeWindow.Fill != nil && exprHasNestedInto(timeWindow.Fill.Val)
}

func exprsHaveInto(exprs Exprs) bool {
	for _, expr := range exprs {
		if exprHasNestedInto(expr) {
			return true
		}
	}
	return false
}

func exprHasNestedInto(expr Expr) bool {
	switch node := expr.(type) {
	case nil:
		return false
	case *Subquery:
		return selectStatementHasInto(node.Select)
	case *ParenExpr:
		return exprHasNestedInto(node.Expr)
	case *BinaryExpr:
		return exprHasNestedInto(node.Left) || exprHasNestedInto(node.Right)
	case *UnaryExpr:
		return exprHasNestedInto(node.Expr)
	case *ComparisonExpr:
		return exprHasNestedInto(node.Left) || exprHasNestedInto(node.Right) || exprHasNestedInto(node.Escape)
	case *AndExpr:
		return exprHasNestedInto(node.Left) || exprHasNestedInto(node.Right)
	case *XorExpr:
		return exprHasNestedInto(node.Left) || exprHasNestedInto(node.Right)
	case *OrExpr:
		return exprHasNestedInto(node.Left) || exprHasNestedInto(node.Right)
	case *NotExpr:
		return exprHasNestedInto(node.Expr)
	case *IsNullExpr:
		return exprHasNestedInto(node.Expr)
	case *IsNotNullExpr:
		return exprHasNestedInto(node.Expr)
	case *IsUnknownExpr:
		return exprHasNestedInto(node.Expr)
	case *IsNotUnknownExpr:
		return exprHasNestedInto(node.Expr)
	case *IsTrueExpr:
		return exprHasNestedInto(node.Expr)
	case *IsNotTrueExpr:
		return exprHasNestedInto(node.Expr)
	case *IsFalseExpr:
		return exprHasNestedInto(node.Expr)
	case *IsNotFalseExpr:
		return exprHasNestedInto(node.Expr)
	case *FuncExpr:
		if exprsHaveInto(node.Exprs) || orderByHasInto(node.OrderBy) {
			return true
		}
		if node.WindowSpec != nil {
			return exprsHaveInto(node.WindowSpec.PartitionBy) ||
				orderByHasInto(node.WindowSpec.OrderBy) ||
				frameHasInto(node.WindowSpec.Frame)
		}
		return false
	case *SerialExtractExpr:
		return exprHasNestedInto(node.SerialExpr) || exprHasNestedInto(node.IndexExpr)
	case *CastExpr:
		return exprHasNestedInto(node.Expr)
	case *BitCastExpr:
		return exprHasNestedInto(node.Expr)
	case *Tuple:
		return exprsHaveInto(node.Exprs)
	case *RangeCond:
		return exprHasNestedInto(node.Left) || exprHasNestedInto(node.From) || exprHasNestedInto(node.To)
	case *CaseExpr:
		if exprHasNestedInto(node.Expr) || exprHasNestedInto(node.Else) {
			return true
		}
		for _, when := range node.Whens {
			if when != nil && (exprHasNestedInto(when.Cond) || exprHasNestedInto(when.Val)) {
				return true
			}
		}
		return false
	case *IntervalExpr:
		return exprHasNestedInto(node.Expr)
	case *DefaultVal:
		return exprHasNestedInto(node.Expr)
	case *UpdateVal:
		return false
	case *SampleExpr:
		return exprsHaveInto(node.columns)
	case *FullTextMatchExpr:
		if exprHasNestedInto(node.Pattern) {
			return true
		}
		for _, keyPart := range node.KeyParts {
			if keyPart != nil && (exprHasNestedInto(keyPart.ColName) || exprHasNestedInto(keyPart.Expr)) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func frameHasInto(frame *FrameClause) bool {
	if frame == nil {
		return false
	}
	return frameBoundHasInto(frame.Start) || frameBoundHasInto(frame.End)
}

func frameBoundHasInto(bound *FrameBound) bool {
	return bound != nil && exprHasNestedInto(bound.Expr)
}

func selectStatementHasInto(stmt SelectStatement) bool {
	return selectTreeHasInto(stmt) || selectTreeHasExport(stmt) || selectStatementHasNestedInto(stmt)
}

func selectTreeHasInto(stmt SelectStatement) bool {
	switch node := stmt.(type) {
	case *Select:
		return len(node.IntoVars) > 0 || selectTreeHasInto(node.Select)
	case *SelectClause:
		return len(node.IntoVars) > 0
	case *ParenSelect:
		return selectTreeHasInto(node.Select)
	case *UnionClause:
		return selectTreeHasInto(node.Left) || selectTreeHasInto(node.Right)
	default:
		return false
	}
}

func SelectIntoExport(stmt SelectStatement) *ExportParam {
	return selectIntoExportForTopLevel(stmt)
}

func selectIntoExportForTopLevel(stmt SelectStatement) *ExportParam {
	switch node := stmt.(type) {
	case *Select:
		if node.Ep != nil {
			return node.Ep
		}
		return selectIntoExportForTopLevel(node.Select)
	case *SelectClause:
		return node.IntoExport
	case *ParenSelect:
		return selectIntoExportForTopLevel(node.Select)
	case *UnionClause:
		if selectTreeHasExport(node.Left) {
			return nil
		}
		return selectIntoExportForTopLevel(node.Right)
	default:
		return nil
	}
}

func selectTreeHasExport(stmt SelectStatement) bool {
	switch node := stmt.(type) {
	case *Select:
		return node.Ep != nil || selectTreeHasExport(node.Select)
	case *SelectClause:
		return node.IntoExport != nil
	case *ParenSelect:
		return selectTreeHasExport(node.Select)
	case *UnionClause:
		return selectTreeHasExport(node.Left) || selectTreeHasExport(node.Right)
	default:
		return false
	}
}

func SelectIntoExportOr(stmt SelectStatement, fallback *ExportParam) *ExportParam {
	if export := SelectIntoExport(stmt); export != nil {
		return export
	}
	return fallback
}

func (node *SelectClause) GetStatementType() string { return "Select" }
func (node *SelectClause) GetQueryType() string     { return QueryTypeDQL }

// WHERE or HAVING clause.
type Where struct {
	Type         string
	RollupHaving bool
	Expr         Expr
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
		ctx.WriteIdentifier(Identifier(node.As.Origin()))
	}
}

// a GROUP BY clause.
type GroupByClause struct {
	GroupByExprsList []Exprs
	GroupingSet      Exprs
	Apart            bool
	// The next four fields are planner-internal metadata for generated
	// grouping-set branches. They keep hidden ORDER BY expressions and bound
	// output identity in the original FROM scope.
	GroupingSetOrderHiddenCount  int
	GroupingSetOrderAliases      map[string][]Expr
	GroupingSetOrderSourceProbes map[string]*GroupingSetOrderSourceProbe
	PreserveOrderSemanticKeys    bool
	Cube                         bool
	GroupingSets                 bool
	Rollup                       bool
}

// GroupingSetOrderSourceProbe defers an otherwise unknowable name-resolution
// choice until a generated branch has bound its real FROM scope.
type GroupingSetOrderSourceProbe struct {
	FallbackName string
	Resolved     bool
	SourceFound  bool
}

func (node *GroupByClause) Format(ctx *FmtCtx) {
	if node.Apart {
		if len(node.GroupingSet) == 0 {
			return
		}
		ctx.WriteString("group by ")
		node.GroupingSet.Format(ctx)
		return
	}
	if node.Cube {
		ctx.WriteString("group by cube(")
		if len(node.GroupByExprsList) > 0 {
			node.GroupByExprsList[0].Format(ctx)
		}
		ctx.WriteByte(')')
		return
	}
	if node.GroupingSets {
		ctx.WriteString("group by grouping sets (")
		for i, list := range node.GroupByExprsList {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.WriteByte('(')
			list.Format(ctx)
			ctx.WriteByte(')')
		}
		ctx.WriteByte(')')
		return
	}

	prefix := "group by "
	for _, list := range node.GroupByExprsList {
		for _, n := range list {
			ctx.WriteString(prefix)
			n.Format(ctx)
			prefix = ", "
		}
	}
	if node.Rollup {
		ctx.WriteString(" with rollup")
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
	JOIN_TYPE_NATURAL_FULL  = "NATURAL FULL"
	JOIN_TYPE_CENTROIDX     = "CENTROIDX"
	JOIN_TYPE_DEDUP         = "DEDUP"
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
	Option   string
}

func (node *JoinTableExpr) Format(ctx *FmtCtx) {
	if node.Left != nil {
		node.Left.Format(ctx)
	}
	if node.JoinType != "" && node.Right != nil {
		ctx.WriteByte(' ')
		if node.JoinType == JOIN_TYPE_CENTROIDX {
			ctx.WriteString(strings.ToLower(node.JoinType))
			ctx.WriteString(" ('")
			ctx.WriteString(strings.ToLower(node.Option))
			ctx.WriteString("')")
		} else {
			ctx.WriteString(strings.ToLower(node.JoinType))
		}
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

const (
	APPLY_TYPE_CROSS = "CROSS APPLY"
	APPLY_TYPE_OUTER = "OUTER APPLY"
)

type ApplyTableExpr struct {
	TableExpr
	ApplyType string
	Left      TableExpr
	Right     TableExpr
}

type ApplyCond interface {
	NodeFormatter
}

func (node *ApplyTableExpr) Format(ctx *FmtCtx) {
	node.Left.Format(ctx)
	ctx.WriteByte(' ')
	ctx.WriteString(strings.ToLower(node.ApplyType))
	ctx.WriteByte(' ')
	node.Right.Format(ctx)
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
		ctx.WriteIdentifier(node.Alias)
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

func NewAliasedTableExpr(e TableExpr, a string, i IdentifierList) *AliasedTableExpr {
	return &AliasedTableExpr{
		Expr: e,
		As: AliasClause{
			Alias: Identifier(a),
			Cols:  i,
		},
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
			ctx.WriteIdentifier(Identifier(value))
		}
	}
	ctx.WriteString(")")
}
