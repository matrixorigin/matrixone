// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"path"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/externalwrite"
	sqldatastream "github.com/matrixorigin/matrixone/pkg/sql/datastream"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/foreignext"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func getViewSecurityTypeFromContext(ctx CompilerContext) string {
	securityType := ""
	val, err := ctx.ResolveVariable("view_security_type", true, false)
	if err == nil {
		if s, ok := val.(string); ok {
			securityType = s
		}
	}
	securityType = strings.TrimSpace(strings.ToUpper(securityType))
	if securityType == "INVOKER" {
		return "INVOKER"
	}
	// Default to DEFINER to match SQL SECURITY behavior.
	return "DEFINER"
}

func parserSQLModeFromContext(ctx CompilerContext) *string {
	sqlMode := ""
	val, err := ctx.ResolveVariable("sql_mode", true, false)
	if err == nil {
		if s, ok := val.(string); ok {
			sqlMode = mysql.SessionSQLModeForParser(s)
		}
	}
	return &sqlMode
}

func canonicalCreateTableSQL(stmt *tree.CreateTable) string {
	fmtCtx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
	stmt.Format(fmtCtx)
	// CreateTable.Format does not include ClusterByOption, but rel_createsql is
	// also consumed when a table is recreated on another node.
	if stmt.ClusterByOption != nil {
		fmtCtx.WriteString(" cluster by (")
		for i, col := range stmt.ClusterByOption.ColumnList {
			if i > 0 {
				fmtCtx.WriteString(", ")
			}
			col.Format(fmtCtx)
		}
		fmtCtx.WriteByte(')')
	}
	return fmtCtx.String()
}

// createTableSQLForCatalog preserves the historical rel_createsql contract for
// a single-statement request, including comments, constraint names, and exact
// formatting consumed by SHOW CREATE TABLE. For a multi-statement COM_QUERY,
// GetRootSql contains the whole request and cannot identify the row's creating
// statement, so persist this statement's canonical AST instead.
func createTableSQLForCatalog(ctx CompilerContext, stmt *tree.CreateTable) string {
	// Partition metadata is parsed again by ALTER TABLE ADD PARTITION. Keep it
	// independent of the creating session's SQL mode, even when the request
	// contains only this statement.
	if stmt.PartitionOption != nil {
		return canonicalCreateTableSQL(stmt)
	}

	rootSQL := ctx.GetRootSql()
	if rootSQL != "" {
		sqlMode := parserSQLModeFromContext(ctx)
		statements, err := parsers.ParseWithSQLMode(
			ctx.GetContext(), dialect.MYSQL, rootSQL, 1, *sqlMode,
		)
		if err == nil {
			defer func() {
				for _, statement := range statements {
					statement.Free()
				}
			}()
			if len(statements) == 1 {
				// CREATE TABLE ... LIKE is expanded before this helper runs. Persist
				// that current schema instead of lossy lineage SQL so new catalogs do
				// not need source-table recovery after another upgrade.
				if create, ok := statements[0].(*tree.CreateTable); ok && create.IsAsLike {
					return canonicalCreateTableSQL(stmt)
				}
				return rootSQL
			}
		}
	}
	return canonicalCreateTableSQL(stmt)
}

// validateViewDefinitionPlugins asks every registered index plugin to vet the optimized
// plan of a view's defining SELECT before it is persisted.
//
// View DDL is the one statement that STORES a query without running it, so anything an
// algorithm cannot execute commits silently and only surfaces when someone selects from
// the view. Today only the fulltext family refuses anything: MATCH() AGAINST() binds to a
// placeholder with no implementation, so a definition no FULLTEXT index can serve is
// unrunnable rather than merely slow. Vector plugins return nil -- their distance
// functions are real kernels, so a plan that misses the index still executes as a
// brute-force scan.
//
// Plugins are consulted in a stable order so that a definition offending more than one
// reports the same error every time.
//
// This must run on the OPTIMIZED plan, which is why it is not part of the validate hook
// passed into the bind: that hook fires before createQuery, where an index rewrite has not
// yet had its chance and every candidate expression still looks unresolved.
// indexRewritesDisabled reports whether the global optimizer_hints variable turns off
// applyIndices. Reads the same variable parseOptimizeHints does, in the same key=value
// form, rather than depending on a QueryBuilder that genViewTableDef does not have.
func indexRewritesDisabled(ctx CompilerContext) bool {
	if ctx == nil {
		return false
	}
	proc := ctx.GetProcess()
	if proc == nil {
		return false
	}
	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables("optimizer_hints")
	if !ok {
		return false
	}
	str, ok := v.(string)
	if !ok || len(str) == 0 {
		return false
	}
	// splitOptimizerHint, not a private copy of the split: this must answer "did applyIndices
	// really get switched off", and only the parser the optimizer itself uses knows that. A
	// copy that trimmed whitespace read ` applyIndices=1` as a hint the optimizer had ignored,
	// so the rewrites ran while validation below was skipped as if they had not.
	for _, kv := range strings.Split(str, ",") {
		if key, value, ok := splitOptimizerHint(kv); ok && key == "applyIndices" && value != 0 {
			return true
		}
	}
	return false
}

func validateViewDefinitionPlugins(ctx CompilerContext, query *plan.Query) error {
	if query == nil {
		return nil
	}
	// Every plugin's answer is inferred from whether its rewrite fired, so the inference is
	// only valid when rewrites were allowed to run at all. The global optimizer_hints knob
	// can switch applyIndices off wholesale (apply_indices.go), leaving every placeholder in
	// place regardless of the catalog -- under `set global optimizer_hints = 'applyIndices=1'`
	// this would refuse EVERY fulltext view, including ones with a perfectly good index, and
	// keep refusing cluster-wide until someone noticed the hint. A diagnostic knob must not
	// silently turn into a DDL policy, so skip validation entirely while it is set: being
	// permissive costs an unrunnable view, being wrong here costs working ones.
	if indexRewritesDisabled(ctx) {
		return nil
	}
	plugins := indexplugin.All()
	sort.Slice(plugins, func(i, j int) bool { return plugins[i].Algo() < plugins[j].Algo() })
	for _, p := range plugins {
		hooks := p.Plan()
		if hooks == nil {
			continue
		}
		if err := hooks.ValidateViewDefinition(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

func genViewTableDef(
	ctx CompilerContext,
	stmt *tree.Select,
	colNames tree.IdentifierList,
	viewDatabase string,
	viewName string,
) (*plan.TableDef, error) {
	var tableDef plan.TableDef
	dependencyCapture := newViewDependencyCaptureContext(ctx)
	ctx = dependencyCapture
	validate := func(query *Query) error {
		for _, node := range query.Nodes {
			if node == nil || node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil {
				continue
			}
			if !node.TableDef.IsTemporary && node.TableDef.TableType != catalog.SystemTemporaryTable {
				continue
			}

			tableName := node.TableDef.OriginalName
			if tableName == "" {
				tableName = node.TableDef.Name
			}
			return moerr.NewViewSelectTmpTable(ctx.GetContext(), tableName)
		}
		return nil
	}

	// check view statement
	var stmtPlan *Plan
	var outputColumnProvenance []OutputColumnProvenance
	var expandedSelectLists map[*tree.SelectClause]tree.SelectExprs
	captureColumnTypes := func(bindCtx *BindContext) {
		outputColumnProvenance = make([]OutputColumnProvenance, len(bindCtx.headings))
		for i := range outputColumnProvenance {
			outputColumnProvenance[i] = bindCtx.outputColumnProvenanceForProject(int32(i))
		}
		expandedSelectLists = bindCtx.expandedSelectLists
	}
	var err error
	switch s := stmt.Select.(type) {
	case *tree.ParenSelect:
		stmtPlan, err = bindAndOptimizeSelectQueryWithValidatorAndCapture(
			plan.Query_SELECT, ctx, s.Select, false, true, validate, captureColumnTypes, true,
			objectkey.Encode(viewDatabase, viewName))
		if err != nil {
			return nil, err
		}
	default:
		stmtPlan, err = bindAndOptimizeSelectQueryWithValidatorAndCapture(
			plan.Query_SELECT, ctx, stmt, false, true, validate, captureColumnTypes, true,
			objectkey.Encode(viewDatabase, viewName))
		if err != nil {
			return nil, err
		}
	}

	query := stmtPlan.GetQuery()
	// Must run on the OPTIMIZED plan, which is why it is not part of the validate hook
	// above: that hook fires before createQuery, where every MATCH is still an unresolved
	// function whether or not an index exists.
	if err = validateViewDefinitionPlugins(ctx, query); err != nil {
		return nil, err
	}
	projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
	if len(colNames) > 0 && len(colNames) != len(projectList) {
		return nil, moerr.NewViewWrongList(ctx.GetContext())
	}
	cols := make([]*plan.ColDef, len(projectList))
	for idx, expr := range projectList {
		name := query.Headings[idx]
		originName := ""
		if len(colNames) > 0 {
			originName = string(colNames[idx])
			name = originName
		}
		typ := &expr.Typ
		if idx < len(outputColumnProvenance) {
			if sourceType := mysqlSpecialTypeFromProvenance(outputColumnProvenance[idx]); sourceType != nil {
				typ = sourceType
			}
		}
		defaultDef := &plan.Default{NullAbility: !expr.Typ.NotNullable}
		if idx < len(outputColumnProvenance) {
			provenance := outputColumnProvenance[idx]
			if provenance.State == ProvenanceSingleSource && provenance.Source != nil &&
				provenance.Source.Metadata.Default != nil {
				defaultDef = DeepCopyDefault(provenance.Source.Metadata.Default)
				defaultDef.NullAbility = !expr.Typ.NotNullable
			}
		}
		cols[idx] = &plan.ColDef{
			Name:       strings.ToLower(name),
			OriginName: originName,
			Alg:        plan.CompressType_Lz4,
			Typ:        *typ,
			Default:    defaultDef,
		}
	}
	tableDef.Cols = cols

	// Check alter and change the viewsql.
	rootSQL := ctx.GetRootSql()
	viewSql := rootSQL
	// remove sql hint
	viewSql = cleanHint(viewSql)
	if len(viewSql) != 0 {
		if viewSql[0] == 'A' {
			viewSql = strings.Replace(viewSql, "ALTER", "CREATE", 1)
		}
		if viewSql[0] == 'a' {
			viewSql = strings.Replace(viewSql, "alter", "create", 1)
		}
	}
	persistedCreateSQL := rootSQL
	if stableViewSQL, rewritten := stableViewSQLWithExpandedStars(ctx, stmt, viewSql, expandedSelectLists); rewritten {
		viewSql = stableViewSQL
		persistedCreateSQL = stableViewSQL
	}

	lowerCaseTableNames := ctx.GetLowerCaseTableNames()
	viewData, err := json.Marshal(ViewData{
		Stmt:                viewSql,
		DefaultDatabase:     ctx.DefaultDatabase(),
		SQLMode:             parserSQLModeFromContext(ctx),
		SecurityType:        getViewSecurityTypeFromContext(ctx),
		LowerCaseTableNames: &lowerCaseTableNames,
		Dependencies:        dependencyCapture.dependencies(),
	})
	if err != nil {
		return nil, err
	}
	tableDef.ViewSql = &plan.ViewDef{
		View: string(viewData),
	}
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemViewRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: persistedCreateSQL,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})

	return &tableDef, nil
}

func stableViewSQLWithExpandedStars(
	ctx CompilerContext,
	stmt *tree.Select,
	viewSql string,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (string, bool) {
	// SAMPLE(*) expands to a sampling operator during binding. The rewriter
	// leaves that query block intact while still stabilizing ordinary stars in
	// unrelated query blocks.
	if viewSql == "" || len(expandedSelectLists) == 0 || !viewSelectHasStar(stmt) {
		return viewSql, false
	}

	stableSelect, ok := viewSelectWithExpandedStars(stmt, expandedSelectLists)
	if !ok {
		return viewSql, false
	}

	parserSQLMode := ""
	if sqlMode := parserSQLModeFromContext(ctx); sqlMode != nil {
		parserSQLMode = *sqlMode
	}
	stmts, err := mysql.ParseWithSQLMode(ctx.GetContext(), viewSql, ctx.GetLowerCaseTableNames(), parserSQLMode)
	if err != nil {
		return viewSql, false
	}
	defer func() {
		for _, statement := range stmts {
			statement.Free()
		}
	}()
	if len(stmts) != 1 {
		return viewSql, false
	}

	switch viewStmt := stmts[0].(type) {
	case *tree.CreateView:
		stableStmt := *viewStmt
		stableStmt.AsSource = stableSelect
		return formatStableViewSQL(&stableStmt), true
	case *tree.AlterView:
		stableStmt := &tree.CreateView{
			Name:     viewStmt.Name,
			ColNames: viewStmt.ColNames,
			AsSource: stableSelect,
		}
		return formatStableViewSQL(stableStmt), true
	default:
		return viewSql, false
	}
}

func formatStableViewSQL(stmt *tree.CreateView) string {
	return tree.StringWithOpts(
		stmt,
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
		tree.WithModeIndependentStringLiterals(),
	)
}

func viewSelectHasStar(stmt *tree.Select) bool {
	if stmt == nil {
		return false
	}
	return selectWithHasStar(stmt.With) ||
		selectStatementHasStar(stmt.Select) ||
		orderByHasStar(stmt.OrderBy) ||
		limitHasStar(stmt.Limit) ||
		timeWindowHasStar(stmt.TimeWindow)
}

func selectStatementHasStar(stmt tree.SelectStatement) bool {
	switch selectStmt := stmt.(type) {
	case *tree.SelectClause:
		return selectClauseHasStar(selectStmt)
	case *tree.Select:
		return viewSelectHasStar(selectStmt)
	case *tree.ParenSelect:
		return selectStatementHasStar(selectStmt.Select)
	case *tree.UnionClause:
		return selectStatementHasStar(selectStmt.Left) || selectStatementHasStar(selectStmt.Right)
	}
	return false
}

func selectWithHasStar(with *tree.With) bool {
	if with == nil {
		return false
	}
	for _, cte := range with.CTEs {
		if cte == nil {
			continue
		}
		selectStmt, ok := cte.Stmt.(tree.SelectStatement)
		if ok && selectStatementHasStar(selectStmt) {
			return true
		}
	}
	return false
}

func selectClauseHasStar(selectClause *tree.SelectClause) bool {
	if selectClause == nil {
		return false
	}
	for _, expr := range selectClause.Exprs {
		if selectExprHasStar(expr) {
			return true
		}
	}
	if fromHasStar(selectClause.From) {
		return true
	}
	if whereHasStar(selectClause.Where) || whereHasStar(selectClause.Having) {
		return true
	}
	return groupByHasStar(selectClause.GroupBy) || windowDefinitionsHaveStar(selectClause.Windows)
}

func windowDefinitionsHaveStar(definitions tree.WindowDefinitions) bool {
	for _, definition := range definitions {
		if definition == nil || definition.Spec == nil {
			continue
		}
		if exprsHasStar(definition.Spec.PartitionBy) || orderByHasStar(definition.Spec.OrderBy) {
			return true
		}
		if definition.Spec.Frame != nil {
			for _, bound := range []*tree.FrameBound{definition.Spec.Frame.Start, definition.Spec.Frame.End} {
				if bound != nil && exprHasStar(bound.Expr) {
					return true
				}
			}
		}
	}
	return false
}

func fromHasStar(from *tree.From) bool {
	if from == nil {
		return false
	}
	for _, table := range from.Tables {
		if tableExprHasStar(table) {
			return true
		}
	}
	return false
}

func tableExprHasStar(table tree.TableExpr) bool {
	switch tableExpr := table.(type) {
	case *tree.Select:
		return viewSelectHasStar(tableExpr)
	case *tree.Subquery:
		return selectStatementHasStar(tableExpr.Select)
	case *tree.AliasedTableExpr:
		return tableExprHasStar(tableExpr.Expr)
	case *tree.ParenTableExpr:
		return tableExprHasStar(tableExpr.Expr)
	case *tree.JoinTableExpr:
		return tableExprHasStar(tableExpr.Left) || tableExprHasStar(tableExpr.Right) || joinCondHasStar(tableExpr.Cond)
	case *tree.ApplyTableExpr:
		return tableExprHasStar(tableExpr.Left) || tableExprHasStar(tableExpr.Right)
	case *tree.StatementSource:
		if selectStmt, ok := tableExpr.Statement.(*tree.Select); ok {
			return viewSelectHasStar(selectStmt)
		}
	case *tree.TableFunction:
		return exprHasStar(tableExpr.Func)
	}
	return false
}

func joinCondHasStar(cond tree.JoinCond) bool {
	onCond, ok := cond.(*tree.OnJoinCond)
	return ok && exprHasStar(onCond.Expr)
}

func whereHasStar(where *tree.Where) bool {
	return where != nil && exprHasStar(where.Expr)
}

func groupByHasStar(groupBy *tree.GroupByClause) bool {
	if groupBy == nil {
		return false
	}
	for _, exprs := range groupBy.GroupByExprsList {
		if exprsHasStar(exprs) {
			return true
		}
	}
	return exprsHasStar(groupBy.GroupingSet)
}

func exprsHasStar(exprs tree.Exprs) bool {
	for _, expr := range exprs {
		if exprHasStar(expr) {
			return true
		}
	}
	return false
}

func orderByHasStar(orderBy tree.OrderBy) bool {
	for _, order := range orderBy {
		if order != nil && exprHasStar(order.Expr) {
			return true
		}
	}
	return false
}

func limitHasStar(limit *tree.Limit) bool {
	return limit != nil && (exprHasStar(limit.Offset) || exprHasStar(limit.Count))
}

func timeWindowHasStar(timeWindow *tree.TimeWindow) bool {
	if timeWindow == nil {
		return false
	}
	if timeWindow.Interval != nil && exprHasStar(timeWindow.Interval.Val) {
		return true
	}
	if timeWindow.Sliding != nil && exprHasStar(timeWindow.Sliding.Val) {
		return true
	}
	return timeWindow.Fill != nil && exprHasStar(timeWindow.Fill.Val)
}

func exprHasStar(expr tree.Expr) bool {
	return exprValueHasStar(reflect.ValueOf(expr), make(map[treeClonePointer]struct{}))
}

func exprValueHasStar(value reflect.Value, visited map[treeClonePointer]struct{}) bool {
	if !value.IsValid() {
		return false
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		key := treeClonePointer{typ: value.Type(), ptr: value.Pointer()}
		if _, ok := visited[key]; ok {
			return false
		}
		visited[key] = struct{}{}
		if expr, ok := value.Interface().(tree.Expr); ok && directExprHasStar(expr) {
			return true
		}
		if sample, ok := value.Interface().(*tree.SampleExpr); ok {
			columns, isStar := sample.GetColumns()
			if isStar {
				return true
			}
			for _, column := range columns {
				if exprValueHasStar(reflect.ValueOf(column), visited) {
					return true
				}
			}
		}
		if subquery, ok := value.Interface().(*tree.Subquery); ok {
			return selectStatementHasStar(subquery.Select)
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Struct {
		if value.CanInterface() {
			if expr, ok := value.Interface().(tree.Expr); ok && directExprHasStar(expr) {
				return true
			}
		}
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if !field.CanInterface() {
				continue
			}
			if selectStmt, ok := field.Interface().(tree.SelectStatement); ok && selectStatementHasStar(selectStmt) {
				return true
			}
			if exprValueHasStar(field, visited) {
				return true
			}
		}
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		for i := 0; i < value.Len(); i++ {
			if exprValueHasStar(value.Index(i), visited) {
				return true
			}
		}
	}
	return false
}

func selectExprHasStar(selectExpr tree.SelectExpr) bool {
	return exprHasStar(selectExpr.Expr)
}

func selectClauseOutputHasStar(selectClause *tree.SelectClause) bool {
	if selectClause == nil {
		return false
	}
	for _, expr := range selectClause.Exprs {
		if directExprHasStar(expr.Expr) {
			return true
		}
	}
	return false
}

func selectClauseHasOrdinaryStar(selectClause *tree.SelectClause) bool {
	if selectClause == nil {
		return false
	}
	for _, expr := range selectClause.Exprs {
		switch expr := expr.Expr.(type) {
		case tree.UnqualifiedStar:
			return true
		case *tree.UnresolvedName:
			if expr.Star {
				return true
			}
		}
	}
	return false
}

func selectClauseHasSampleExpr(selectClause *tree.SelectClause) bool {
	if selectClause == nil {
		return false
	}
	for _, expr := range selectClause.Exprs {
		if _, ok := expr.Expr.(*tree.SampleExpr); ok {
			return true
		}
	}
	return false
}

func selectExprsHaveSampleExpr(exprs tree.SelectExprs) bool {
	for _, expr := range exprs {
		if _, ok := expr.Expr.(*tree.SampleExpr); ok {
			return true
		}
	}
	return false
}

func directExprHasStar(expr tree.Expr) bool {
	switch expr := expr.(type) {
	case tree.UnqualifiedStar:
		return true
	case *tree.UnresolvedName:
		return expr.Star
	case *tree.SampleExpr:
		_, isStar := expr.GetColumns()
		if isStar {
			return true
		}
	}
	return false
}

func viewSelectWithExpandedStars(
	stmt *tree.Select,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (*tree.Select, bool) {
	if stmt == nil {
		return nil, false
	}
	stableSelect := *stmt
	stableWith, withRewritten := viewWithWithExpandedStars(stmt.With, expandedSelectLists)
	stableSelect.With = stableWith
	stableStatement, statementRewritten := viewSelectStatementWithExpandedStars(stmt.Select, expandedSelectLists)
	stableSelect.Select = stableStatement
	stableOrderBy, orderByRewritten := viewOrderByWithExpandedStars(stmt.OrderBy, expandedSelectLists)
	stableSelect.OrderBy = stableOrderBy
	stableLimit, limitRewritten := viewLimitWithExpandedStars(stmt.Limit, expandedSelectLists)
	stableSelect.Limit = stableLimit
	stableTimeWindow, timeWindowRewritten := viewTimeWindowWithExpandedStars(stmt.TimeWindow, expandedSelectLists)
	stableSelect.TimeWindow = stableTimeWindow
	rewritten := withRewritten || statementRewritten || orderByRewritten || limitRewritten || timeWindowRewritten
	if stableStatement == nil || !rewritten {
		return nil, false
	}
	return &stableSelect, rewritten
}

func viewSelectStatementWithExpandedStars(
	stmt tree.SelectStatement,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.SelectStatement, bool) {
	switch selectStmt := stmt.(type) {
	case *tree.SelectClause:
		stableClause := *selectStmt
		rewritten := false
		if selectStmt.From != nil {
			stableFrom, fromRewritten := viewFromWithExpandedStars(selectStmt.From, expandedSelectLists)
			stableClause.From = stableFrom
			rewritten = fromRewritten
		}
		expandedSelectList, ok := expandedSelectLists[selectStmt]
		if ok {
			if selectClauseOutputHasStar(selectStmt) || len(expandedSelectList) != len(selectStmt.Exprs) {
				// A SAMPLE-only clause must be replaced by the captured SAMPLE
				// expression, not by a bare projection list supplied by an
				// incomplete/foreign capture map. The production capture always
				// preserves SAMPLE, while this guard keeps malformed input from
				// silently changing the query shape.
				if selectClauseHasSampleExpr(selectStmt) &&
					!selectClauseHasOrdinaryStar(selectStmt) &&
					!selectExprsHaveSampleExpr(expandedSelectList) {
					stableExprs, exprsRewritten := viewSelectExprsWithExpandedStars(selectStmt.Exprs, expandedSelectLists)
					stableClause.Exprs = stableExprs
					rewritten = rewritten || exprsRewritten
				} else {
					stableExprs, exprsRewritten := viewSelectExprsWithExpandedStars(expandedSelectList, expandedSelectLists)
					stableClause.Exprs = stableExprs
					rewritten = true
					rewritten = rewritten || exprsRewritten
				}
			} else {
				stableExprs, exprsRewritten := viewSelectExprsWithExpandedStableHeadings(selectStmt.Exprs, expandedSelectList, expandedSelectLists)
				stableClause.Exprs = stableExprs
				rewritten = true
				rewritten = rewritten || exprsRewritten
			}
		} else {
			stableExprs, exprsRewritten := viewSelectExprsWithExpandedStars(selectStmt.Exprs, expandedSelectLists)
			stableClause.Exprs = stableExprs
			rewritten = rewritten || exprsRewritten
		}
		if stableWhere, whereRewritten := viewWhereWithExpandedStars(selectStmt.Where, expandedSelectLists); whereRewritten {
			stableClause.Where = stableWhere
			rewritten = true
		}
		if stableHaving, havingRewritten := viewWhereWithExpandedStars(selectStmt.Having, expandedSelectLists); havingRewritten {
			stableClause.Having = stableHaving
			rewritten = true
		}
		if stableGroupBy, groupByRewritten := viewGroupByWithExpandedStars(selectStmt.GroupBy, expandedSelectLists); groupByRewritten {
			stableClause.GroupBy = stableGroupBy
			rewritten = true
		}
		if stableWindows, windowsRewritten := viewWindowDefinitionsWithExpandedStars(selectStmt.Windows, expandedSelectLists); windowsRewritten {
			stableClause.Windows = stableWindows
			rewritten = true
		}
		return &stableClause, rewritten
	case *tree.Select:
		stableSelect := *selectStmt
		stableWith, withRewritten := viewWithWithExpandedStars(selectStmt.With, expandedSelectLists)
		stableSelect.With = stableWith
		stableStatement, statementRewritten := viewSelectStatementWithExpandedStars(selectStmt.Select, expandedSelectLists)
		stableSelect.Select = stableStatement
		stableOrderBy, orderByRewritten := viewOrderByWithExpandedStars(selectStmt.OrderBy, expandedSelectLists)
		stableSelect.OrderBy = stableOrderBy
		stableLimit, limitRewritten := viewLimitWithExpandedStars(selectStmt.Limit, expandedSelectLists)
		stableSelect.Limit = stableLimit
		stableTimeWindow, timeWindowRewritten := viewTimeWindowWithExpandedStars(selectStmt.TimeWindow, expandedSelectLists)
		stableSelect.TimeWindow = stableTimeWindow
		return &stableSelect, withRewritten || statementRewritten || orderByRewritten || limitRewritten || timeWindowRewritten
	case *tree.ParenSelect:
		stableParen := *selectStmt
		stableStatement, rewritten := viewSelectStatementWithExpandedStars(selectStmt.Select, expandedSelectLists)
		if stableStatement == nil {
			return nil, false
		}
		stableSelect, ok := stableStatement.(*tree.Select)
		if !ok {
			return nil, false
		}
		stableParen.Select = stableSelect
		return &stableParen, rewritten
	case *tree.UnionClause:
		stableUnion := *selectStmt
		left, leftRewritten := viewSelectStatementWithExpandedStars(selectStmt.Left, expandedSelectLists)
		right, rightRewritten := viewSelectStatementWithExpandedStars(selectStmt.Right, expandedSelectLists)
		if left == nil || right == nil {
			return nil, false
		}
		stableUnion.Left = left
		stableUnion.Right = right
		return &stableUnion, leftRewritten || rightRewritten
	default:
		return stmt, false
	}
}

func viewWithWithExpandedStars(
	with *tree.With,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (*tree.With, bool) {
	if with == nil {
		return nil, false
	}
	stableWith := *with
	stableWith.CTEs = make([]*tree.CTE, len(with.CTEs))
	rewritten := false
	for i, cte := range with.CTEs {
		if cte == nil {
			continue
		}
		stableCTE := *cte
		selectStmt, ok := cte.Stmt.(tree.SelectStatement)
		if !ok {
			stableWith.CTEs[i] = &stableCTE
			continue
		}
		stableStmt, cteRewritten := viewSelectStatementWithExpandedStars(selectStmt, expandedSelectLists)
		if stableStmt != nil {
			stableCTE.Stmt = stableStmt
		}
		stableWith.CTEs[i] = &stableCTE
		rewritten = rewritten || cteRewritten
	}
	return &stableWith, rewritten
}

func viewSelectExprsWithExpandedStars(
	exprs tree.SelectExprs,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.SelectExprs, bool) {
	if len(exprs) == 0 {
		return exprs, false
	}
	stableExprs := make(tree.SelectExprs, len(exprs))
	rewritten := false
	for i, expr := range exprs {
		stableExprs[i] = expr
		stableExpr, exprRewritten := viewExprWithExpandedStars(expr.Expr, expandedSelectLists)
		if exprRewritten {
			stableExprs[i].Expr = stableExpr
			rewritten = true
		} else {
			stableExprs[i].Expr = cloneTreeExpr(expr.Expr)
		}
		if expr.As != nil {
			stableExprs[i].As = tree.NewCStr(expr.As.Origin(), 1)
		}
	}
	return stableExprs, rewritten
}

func viewSelectExprsWithExpandedStableHeadings(
	originalExprs tree.SelectExprs,
	expandedExprs tree.SelectExprs,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.SelectExprs, bool) {
	stableExprs := make(tree.SelectExprs, len(expandedExprs))
	rewritten := false
	for i, expandedExpr := range expandedExprs {
		stableExprs[i] = expandedExpr
		stableExprs[i].Expr = cloneTreeExpr(expandedExpr.Expr)
		if expandedExpr.As != nil {
			stableExprs[i].As = tree.NewCStr(expandedExpr.As.Origin(), 1)
		}
		if i >= len(originalExprs) {
			continue
		}
		if rewriteClonedExprSubqueriesWithExpandedStars(
			reflect.ValueOf(originalExprs[i].Expr),
			reflect.ValueOf(stableExprs[i].Expr),
			expandedSelectLists,
			make(map[treeClonePointer]struct{}),
		) {
			rewritten = true
		}
	}
	return stableExprs, rewritten
}

func viewWhereWithExpandedStars(
	where *tree.Where,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (*tree.Where, bool) {
	if where == nil {
		return nil, false
	}
	stableWhere := *where
	stableExpr, rewritten := viewExprWithExpandedStars(where.Expr, expandedSelectLists)
	stableWhere.Expr = stableExpr
	return &stableWhere, rewritten
}

func viewGroupByWithExpandedStars(
	groupBy *tree.GroupByClause,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (*tree.GroupByClause, bool) {
	if groupBy == nil {
		return nil, false
	}
	stableGroupBy := *groupBy
	rewritten := false
	if len(groupBy.GroupByExprsList) > 0 {
		stableGroupBy.GroupByExprsList = make([]tree.Exprs, len(groupBy.GroupByExprsList))
		for i, exprs := range groupBy.GroupByExprsList {
			stableExprs, exprsRewritten := viewExprsWithExpandedStars(exprs, expandedSelectLists)
			stableGroupBy.GroupByExprsList[i] = stableExprs
			rewritten = rewritten || exprsRewritten
		}
	}
	if stableGroupingSet, groupingSetRewritten := viewExprsWithExpandedStars(groupBy.GroupingSet, expandedSelectLists); groupingSetRewritten {
		stableGroupBy.GroupingSet = stableGroupingSet
		rewritten = true
	}
	return &stableGroupBy, rewritten
}

func viewWindowDefinitionsWithExpandedStars(
	definitions tree.WindowDefinitions,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.WindowDefinitions, bool) {
	if len(definitions) == 0 {
		return definitions, false
	}
	stableDefinitions := make(tree.WindowDefinitions, len(definitions))
	rewritten := false
	for i, definition := range definitions {
		if definition == nil {
			continue
		}
		stableDefinition := *definition
		if definition.Name != nil {
			stableDefinition.Name = tree.NewCStr(definition.Name.Origin(), 1)
		}
		if definition.Spec != nil {
			stableSpec := *definition.Spec
			var fieldRewritten bool
			stableSpec.PartitionBy, fieldRewritten = viewExprsWithExpandedStars(definition.Spec.PartitionBy, expandedSelectLists)
			rewritten = rewritten || fieldRewritten
			stableSpec.OrderBy, fieldRewritten = viewOrderByWithExpandedStars(definition.Spec.OrderBy, expandedSelectLists)
			rewritten = rewritten || fieldRewritten
			if definition.Spec.Frame != nil {
				stableFrame := *definition.Spec.Frame
				if definition.Spec.Frame.Start != nil {
					stableStart := *definition.Spec.Frame.Start
					stableStart.Expr, fieldRewritten = viewExprWithExpandedStars(definition.Spec.Frame.Start.Expr, expandedSelectLists)
					rewritten = rewritten || fieldRewritten
					stableFrame.Start = &stableStart
				}
				if definition.Spec.Frame.End != nil {
					stableEnd := *definition.Spec.Frame.End
					stableEnd.Expr, fieldRewritten = viewExprWithExpandedStars(definition.Spec.Frame.End.Expr, expandedSelectLists)
					rewritten = rewritten || fieldRewritten
					stableFrame.End = &stableEnd
				}
				stableSpec.Frame = &stableFrame
			}
			stableDefinition.Spec = &stableSpec
		}
		stableDefinitions[i] = &stableDefinition
	}
	return stableDefinitions, rewritten
}

func viewExprsWithExpandedStars(
	exprs tree.Exprs,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.Exprs, bool) {
	if len(exprs) == 0 {
		return exprs, false
	}
	stableExprs := make(tree.Exprs, len(exprs))
	rewritten := false
	for i, expr := range exprs {
		stableExpr, exprRewritten := viewExprWithExpandedStars(expr, expandedSelectLists)
		stableExprs[i] = stableExpr
		rewritten = rewritten || exprRewritten
	}
	return stableExprs, rewritten
}

func viewOrderByWithExpandedStars(
	orderBy tree.OrderBy,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.OrderBy, bool) {
	if len(orderBy) == 0 {
		return orderBy, false
	}
	stableOrderBy := make(tree.OrderBy, len(orderBy))
	rewritten := false
	for i, order := range orderBy {
		if order == nil {
			continue
		}
		stableOrder := *order
		stableExpr, exprRewritten := viewExprWithExpandedStars(order.Expr, expandedSelectLists)
		stableOrder.Expr = stableExpr
		stableOrderBy[i] = &stableOrder
		rewritten = rewritten || exprRewritten
	}
	return stableOrderBy, rewritten
}

func viewLimitWithExpandedStars(
	limit *tree.Limit,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (*tree.Limit, bool) {
	if limit == nil {
		return nil, false
	}
	stableLimit := *limit
	offset, offsetRewritten := viewExprWithExpandedStars(limit.Offset, expandedSelectLists)
	count, countRewritten := viewExprWithExpandedStars(limit.Count, expandedSelectLists)
	stableLimit.Offset = offset
	stableLimit.Count = count
	return &stableLimit, offsetRewritten || countRewritten
}

func viewTimeWindowWithExpandedStars(
	timeWindow *tree.TimeWindow,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (*tree.TimeWindow, bool) {
	if timeWindow == nil {
		return nil, false
	}
	stableTimeWindow := *timeWindow
	rewritten := false
	if timeWindow.Interval != nil {
		stableInterval := *timeWindow.Interval
		stableVal, valRewritten := viewExprWithExpandedStars(timeWindow.Interval.Val, expandedSelectLists)
		stableInterval.Val = stableVal
		stableTimeWindow.Interval = &stableInterval
		rewritten = rewritten || valRewritten
	}
	if timeWindow.Sliding != nil {
		stableSliding := *timeWindow.Sliding
		stableVal, valRewritten := viewExprWithExpandedStars(timeWindow.Sliding.Val, expandedSelectLists)
		stableSliding.Val = stableVal
		stableTimeWindow.Sliding = &stableSliding
		rewritten = rewritten || valRewritten
	}
	if timeWindow.Fill != nil {
		stableFill := *timeWindow.Fill
		stableVal, valRewritten := viewExprWithExpandedStars(timeWindow.Fill.Val, expandedSelectLists)
		stableFill.Val = stableVal
		stableTimeWindow.Fill = &stableFill
		rewritten = rewritten || valRewritten
	}
	return &stableTimeWindow, rewritten
}

func viewExprWithExpandedStars(
	expr tree.Expr,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.Expr, bool) {
	if expr == nil {
		return nil, false
	}
	stableExpr := cloneTreeExpr(expr)
	rewritten := rewriteClonedExprSubqueriesWithExpandedStars(
		reflect.ValueOf(expr),
		reflect.ValueOf(stableExpr),
		expandedSelectLists,
		make(map[treeClonePointer]struct{}),
	)
	return stableExpr, rewritten
}

func rewriteClonedExprSubqueriesWithExpandedStars(
	original reflect.Value,
	cloned reflect.Value,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
	visited map[treeClonePointer]struct{},
) bool {
	if !original.IsValid() || !cloned.IsValid() {
		return false
	}
	for original.Kind() == reflect.Interface {
		if original.IsNil() {
			return false
		}
		original = original.Elem()
	}
	for cloned.Kind() == reflect.Interface {
		if cloned.IsNil() {
			return false
		}
		cloned = cloned.Elem()
	}
	if original.Kind() == reflect.Pointer {
		if original.IsNil() {
			return false
		}
		key := treeClonePointer{typ: original.Type(), ptr: original.Pointer()}
		if _, ok := visited[key]; ok {
			return false
		}
		visited[key] = struct{}{}
		if sample, ok := original.Interface().(*tree.SampleExpr); ok {
			clonedSample, ok := cloned.Interface().(*tree.SampleExpr)
			if !ok {
				return false
			}
			originalColumns, isStar := sample.GetColumns()
			clonedColumns, _ := clonedSample.GetColumns()
			if len(clonedColumns) != len(originalColumns) {
				clonedColumns = make(tree.Exprs, len(originalColumns))
			} else {
				clonedColumns = append(tree.Exprs(nil), clonedColumns...)
			}
			rewritten := false
			for i, column := range originalColumns {
				stableColumn, columnRewritten := viewExprWithExpandedStars(column, expandedSelectLists)
				clonedColumns[i] = stableColumn
				rewritten = rewritten || columnRewritten
			}
			if rewritten {
				clonedSample.SetColumns(clonedColumns, isStar)
			}
			return rewritten
		}
		if subquery, ok := original.Interface().(*tree.Subquery); ok {
			stableStatement, rewritten := viewSelectStatementWithExpandedStars(subquery.Select, expandedSelectLists)
			if rewritten && stableStatement != nil && cloned.Kind() == reflect.Pointer && !cloned.IsNil() {
				if clonedSubquery, ok := cloned.Interface().(*tree.Subquery); ok {
					clonedSubquery.Select = stableStatement
				}
			}
			return rewritten
		}
		original = original.Elem()
		if cloned.Kind() == reflect.Pointer {
			if cloned.IsNil() {
				return false
			}
			cloned = cloned.Elem()
		}
	}
	rewritten := false
	switch original.Kind() {
	case reflect.Struct:
		if cloned.Kind() != reflect.Struct {
			return false
		}
		for i := 0; i < original.NumField() && i < cloned.NumField(); i++ {
			originalField := original.Field(i)
			clonedField := cloned.Field(i)
			if !originalField.CanInterface() {
				continue
			}
			if selectStmt, ok := originalField.Interface().(tree.SelectStatement); ok {
				stableStatement, fieldRewritten := viewSelectStatementWithExpandedStars(selectStmt, expandedSelectLists)
				if fieldRewritten && stableStatement != nil && clonedField.CanSet() {
					stableValue := reflect.ValueOf(stableStatement)
					if stableValue.Type().AssignableTo(clonedField.Type()) {
						clonedField.Set(stableValue)
					}
				}
				rewritten = rewritten || fieldRewritten
				continue
			}
			rewritten = rewriteClonedExprSubqueriesWithExpandedStars(originalField, clonedField, expandedSelectLists, visited) || rewritten
		}
	case reflect.Slice, reflect.Array:
		if cloned.Kind() != reflect.Slice && cloned.Kind() != reflect.Array {
			return false
		}
		for i := 0; i < original.Len() && i < cloned.Len(); i++ {
			rewritten = rewriteClonedExprSubqueriesWithExpandedStars(original.Index(i), cloned.Index(i), expandedSelectLists, visited) || rewritten
		}
	}
	return rewritten
}

func viewFromWithExpandedStars(
	from *tree.From,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (*tree.From, bool) {
	if from == nil {
		return nil, false
	}
	stableFrom := *from
	tables, rewritten := viewTableExprsWithExpandedStars(from.Tables, expandedSelectLists)
	stableFrom.Tables = tables
	return &stableFrom, rewritten
}

func viewTableExprsWithExpandedStars(
	tables tree.TableExprs,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.TableExprs, bool) {
	if len(tables) == 0 {
		return tables, false
	}
	stableTables := make(tree.TableExprs, len(tables))
	rewritten := false
	for i, table := range tables {
		stableTable, tableRewritten := viewTableExprWithExpandedStars(table, expandedSelectLists)
		stableTables[i] = stableTable
		rewritten = rewritten || tableRewritten
	}
	return stableTables, rewritten
}

func viewTableExprWithExpandedStars(
	table tree.TableExpr,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.TableExpr, bool) {
	switch tableExpr := table.(type) {
	case *tree.Select:
		return viewSelectWithExpandedStars(tableExpr, expandedSelectLists)
	case *tree.Subquery:
		stableSubquery := *tableExpr
		stableStatement, rewritten := viewSelectStatementWithExpandedStars(tableExpr.Select, expandedSelectLists)
		stableSubquery.Select = stableStatement
		return &stableSubquery, rewritten
	case *tree.AliasedTableExpr:
		stableAliased := *tableExpr
		stableExpr, rewritten := viewTableExprWithExpandedStars(tableExpr.Expr, expandedSelectLists)
		stableAliased.Expr = stableExpr
		return &stableAliased, rewritten
	case *tree.ParenTableExpr:
		stableParen := *tableExpr
		stableExpr, rewritten := viewTableExprWithExpandedStars(tableExpr.Expr, expandedSelectLists)
		stableParen.Expr = stableExpr
		return &stableParen, rewritten
	case *tree.JoinTableExpr:
		stableJoin := *tableExpr
		left, leftRewritten := viewTableExprWithExpandedStars(tableExpr.Left, expandedSelectLists)
		right, rightRewritten := viewTableExprWithExpandedStars(tableExpr.Right, expandedSelectLists)
		cond, condRewritten := viewJoinCondWithExpandedStars(tableExpr.Cond, expandedSelectLists)
		stableJoin.Left = left
		stableJoin.Right = right
		stableJoin.Cond = cond
		return &stableJoin, leftRewritten || rightRewritten || condRewritten
	case *tree.ApplyTableExpr:
		stableApply := *tableExpr
		left, leftRewritten := viewTableExprWithExpandedStars(tableExpr.Left, expandedSelectLists)
		right, rightRewritten := viewTableExprWithExpandedStars(tableExpr.Right, expandedSelectLists)
		stableApply.Left = left
		stableApply.Right = right
		return &stableApply, leftRewritten || rightRewritten
	case *tree.StatementSource:
		stableSource := *tableExpr
		if selectStmt, ok := tableExpr.Statement.(*tree.Select); ok {
			stableSelect, rewritten := viewSelectWithExpandedStars(selectStmt, expandedSelectLists)
			if rewritten {
				stableSource.Statement = stableSelect
				return &stableSource, true
			}
		}
		return &stableSource, false
	case *tree.TableFunction:
		stableFunction := *tableExpr
		stableFunc, rewritten := viewExprWithExpandedStars(tableExpr.Func, expandedSelectLists)
		if funcExpr, ok := stableFunc.(*tree.FuncExpr); ok {
			stableFunction.Func = funcExpr
		}
		return &stableFunction, rewritten
	default:
		return table, false
	}
}

func viewJoinCondWithExpandedStars(
	cond tree.JoinCond,
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs,
) (tree.JoinCond, bool) {
	onCond, ok := cond.(*tree.OnJoinCond)
	if !ok {
		return cond, false
	}
	stableCond := *onCond
	stableExpr, rewritten := viewExprWithExpandedStars(onCond.Expr, expandedSelectLists)
	stableCond.Expr = stableExpr
	return &stableCond, rewritten
}

func genAsSelectCols(ctx CompilerContext, stmt *tree.Select, isPrepareStmt bool) ([]*ColDef, *Query, error) {
	var err error
	var rootId int32
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt, false)
	bindCtx := NewBindContext(builder, nil)

	if s, ok := stmt.Select.(*tree.ParenSelect); ok {
		stmt = s.Select
	}
	if rootId, err = builder.bindSelect(stmt, bindCtx, true); err != nil {
		return nil, nil, err
	}
	outputColumnProvenance := make([]OutputColumnProvenance, len(bindCtx.headings))
	for i := range outputColumnProvenance {
		outputColumnProvenance[i] = bindCtx.outputColumnProvenanceForProject(int32(i))
	}
	builder.qry.Steps = append(builder.qry.Steps, rootId)
	// CTAS metadata must reflect the final query output: outer joins and scalar
	// subqueries can synthesize NULLs even when their source columns are NOT NULL.
	query, err := builder.createQuery()
	if err != nil {
		return nil, nil, err
	}
	rootNode := query.Nodes[query.Steps[len(query.Steps)-1]]

	cols := make([]*plan.ColDef, len(rootNode.ProjectList))
	for i, expr := range rootNode.ProjectList {
		typ := expr.Typ
		provenance := outputColumnProvenance[i]
		if provenance.State == ProvenanceSingleSource && provenance.Source != nil {
			if isEnumOrSetPlanType(&provenance.Source.Metadata.Typ) {
				typ = provenance.Source.Metadata.Typ
				typ.NotNullable = expr.Typ.NotNullable
			}
		}
		// CTAS creates a new table from the query result.  A source column's
		// AUTO_INCREMENT attribute is not part of that result schema and must
		// not be copied to the new table.
		inheritedAutoIncr := typ.AutoIncr
		if provenance.State == ProvenanceSingleSource && provenance.Source != nil {
			inheritedAutoIncr = inheritedAutoIncr || provenance.Source.Metadata.Typ.AutoIncr
		}
		typ.AutoIncr = false
		nullAbility := ctasExprCanBeNull(expr)
		if provenance.State == ProvenanceSingleSource && provenance.Source != nil {
			nullAbility = nullAbility || provenance.Source.Metadata.NullAbility
		}
		defaultDef := &plan.Default{NullAbility: nullAbility}
		if provenance.State == ProvenanceSingleSource && provenance.Source != nil {
			switch provenance.CTASDefaultPolicy {
			case CTASDefaultInheritSource, CTASDefaultInheritViewSource:
				if provenance.Source.Metadata.Default != nil {
					defaultDef = DeepCopyDefault(provenance.Source.Metadata.Default)
					defaultDef.NullAbility = nullAbility
					if defaultDef.Expr == nil && defaultDef.OriginString != "" {
						defaultDef, err = buildCTASDefaultFromOrigin(
							ctx, typ, nullAbility, defaultDef.OriginString)
						if err != nil {
							return nil, nil, err
						}
					}
				}
			}
		}
		// A derived expression that is guaranteed to be non-NULL needs an
		// executable type default in the materialized CTAS schema. This covers
		// neutral-value aggregates such as COUNT and the BIT_* family without
		// copying defaults through semantic expression boundaries.
		if provenance.CTASDefaultPolicy == CTASDefaultUseTypeDefault {
			defaultDef, err = buildCTASDefaultForView(ctx, typ, nullAbility)
			if err != nil {
				return nil, nil, err
			}
		}

		// AUTO_INCREMENT columns have no ordinary source default. Once the
		// generated attribute is removed from the CTAS target, preserve the
		// non-null insert contract with the type's default (for example,
		// DEFAULT 0 for integer columns). An explicit target declaration is
		// applied later by buildTableDefs and remains authoritative.
		if inheritedAutoIncr && defaultDef.Expr == nil && defaultDef.OriginString == "" {
			defaultDef, err = buildCTASDefaultForView(ctx, typ, nullAbility)
			if err != nil {
				return nil, nil, err
			}
		}

		cols[i] = &plan.ColDef{
			Name:    strings.ToLower(bindCtx.headings[i]),
			Alg:     plan.CompressType_Lz4,
			Typ:     typ,
			Default: defaultDef,
		}
	}
	return cols, query, nil
}

func buildCTASDefaultForView(ctx CompilerContext, typ plan.Type, nullAbility bool) (*plan.Default, error) {
	defaultDef := &plan.Default{NullAbility: nullAbility}
	if nullAbility {
		return defaultDef, nil
	}

	originString, ok := ctasViewTypeDefaultOrigin(typ)
	if !ok {
		return defaultDef, nil
	}

	return buildCTASDefaultFromOrigin(ctx, typ, false, originString)
}

func buildCTASDefaultFromOrigin(
	ctx CompilerContext, typ plan.Type, nullAbility bool, originString string,
) (*plan.Default, error) {
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, "select "+originString, 1)
	if err != nil {
		return nil, err
	}
	defer stmt.Free()
	selectStmt, ok := stmt.(*tree.Select)
	if !ok {
		return nil, moerr.NewInternalError(ctx.GetContext(), "invalid CTAS type default expression")
	}
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(selectClause.Exprs) != 1 {
		return nil, moerr.NewInternalError(ctx.GetContext(), "invalid CTAS type default expression")
	}

	binder := NewDefaultBinder(ctx.GetContext(), nil, nil, typ, nil)
	defaultExpr, err := binder.BindExpr(selectClause.Exprs[0].Expr, 0, false)
	if err != nil {
		return nil, err
	}
	defaultExpr, err = makePlan2AssignmentCastExpr(ctx.GetContext(), defaultExpr, typ)
	if err != nil {
		return nil, err
	}
	defaultExpr, err = ConstantFold(
		batch.EmptyForConstFoldBatch, DeepCopyExpr(defaultExpr), ctx.GetProcess(), false, true)
	if err != nil {
		return nil, err
	}
	return &plan.Default{
		NullAbility:  nullAbility,
		Expr:         defaultExpr,
		OriginString: originString,
	}, nil
}

func ctasViewTypeDefaultOrigin(typ plan.Type) (string, bool) {
	if isSetPlanType(&typ) {
		return "''", true
	}
	if isEnumPlanType(&typ) {
		elements := strings.Split(typ.Enumvalues, ",")
		if len(elements) == 0 {
			return "", false
		}
		return "'" + formatStrInSingleQuotes(elements[0]) + "'", true
	}

	switch types.T(typ.Id) {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_bool, types.T_bit:
		return "0", true
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		if typ.Scale > 0 {
			return "0." + strings.Repeat("0", int(typ.Scale)), true
		}
		return "0", true
	case types.T_time:
		return "'00:00:00'", true
	case types.T_year:
		return "'0000'", true
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return "''", true
	default:
		return "", false
	}
}

func ctasExprCanBeNull(expr *Expr) bool {
	if !expr.Typ.NotNullable {
		return true
	}

	// MySQL CTAS creates nullable columns for explicit DATETIME casts, even
	// when the source expression is a non-NULL literal. Keep this CTAS-only
	// metadata rule separate from normal expression nullability propagation.
	fn := expr.GetF()
	return fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" &&
		types.T(expr.Typ.Id) == types.T_datetime
}

func buildCreateView(stmt *tree.CreateView, ctx CompilerContext) (*Plan, error) {
	viewName := stmt.Name.ObjectName
	if err := validateIdentifier(ctx.GetContext(), string(viewName)); err != nil {
		return nil, err
	}

	createView := &plan.CreateView{
		Replace:     stmt.Replace,
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			Name: string(viewName),
		},
	}

	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		createView.Database = ""
	} else {
		createView.Database = string(stmt.Name.SchemaName)
	}
	if len(createView.Database) == 0 {
		createView.Database = ctx.DefaultDatabase()
	}

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	if IsSnapshotValid(ctx.GetSnapshot()) {
		snapshot = ctx.GetSnapshot()
	}

	if sub, err := ctx.GetSubscriptionMeta(createView.Database, snapshot); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create view in subscription database")
	}

	if stmt.Replace && !stmt.IfNotExists {
		ctx.SetBuildingAlterView(true, createView.Database, string(viewName))
		defer ctx.SetBuildingAlterView(false, "", "")
	}

	tableDef, err := genViewTableDef(
		ctx, stmt.AsSource, stmt.ColNames, createView.Database, string(viewName))
	if err != nil {
		return nil, err
	}
	if err := validatePersistedTableIdentifiers(ctx.GetContext(), tableDef); err != nil {
		return nil, err
	}

	createView.TableDef.Cols = tableDef.Cols
	createView.TableDef.ViewSql = tableDef.ViewSql
	createView.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_VIEW,
				Definition: &plan.DataDefinition_CreateView{
					CreateView: createView,
				},
			},
		},
	}, nil
}

func buildSequenceTableDef(stmt *tree.CreateSequence, ctx CompilerContext, cs *plan.CreateSequence) error {
	// Sequence table got 1 row and 7 col
	// sequence_value, maxvalue,minvalue,startvalue,increment,cycleornot,iscalled.
	cols := make([]*plan.ColDef, len(Sequence_cols_name))

	typ, err := getTypeFromAst(ctx.GetContext(), stmt.Type)
	if err != nil {
		return err
	}
	for i := range cols {
		if i == 4 {
			break
		}
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ:  typ,
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	cols[4] = &plan.ColDef{
		Name: Sequence_cols_name[4],
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_int64),
			Width: 0,
			Scale: 0,
		},
		Primary: true,
		Default: &plan.Default{
			NullAbility:  true,
			Expr:         nil,
			OriginString: "",
		},
	}
	cs.TableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{Sequence_cols_name[4]},
		PkeyColName: Sequence_cols_name[4],
	}
	for i := 5; i <= 6; i++ {
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_bool),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}

	cs.TableDef.Cols = cols

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemSequenceRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}

	cs.TableDef.Defs = append(cs.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return nil
}

func buildAlterSequenceTableDef(stmt *tree.AlterSequence, ctx CompilerContext, as *plan.AlterSequence) error {
	// Sequence table got 1 row and 7 col
	// sequence_value, maxvalue,minvalue,startvalue,increment,cycleornot,iscalled.
	cols := make([]*plan.ColDef, len(Sequence_cols_name))

	var typ plan.Type
	var err error
	if stmt.Type == nil {
		_, tableDef, err := ctx.Resolve(as.GetDatabase(), as.TableDef.Name, nil)
		if err != nil {
			return err
		}
		if tableDef == nil {
			return moerr.NewInvalidInputf(ctx.GetContext(), "no such sequence %s", as.TableDef.Name)
		} else {
			typ = tableDef.Cols[0].Typ
		}
	} else {
		typ, err = getTypeFromAst(ctx.GetContext(), stmt.Type.Type)
		if err != nil {
			return err
		}
	}

	for i := range cols {
		if i == 4 {
			break
		}
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ:  typ,
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	cols[4] = &plan.ColDef{
		Name: Sequence_cols_name[4],
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_int64),
			Width: 0,
			Scale: 0,
		},
		Primary: true,
		Default: &plan.Default{
			NullAbility:  true,
			Expr:         nil,
			OriginString: "",
		},
	}
	as.TableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{Sequence_cols_name[4]},
		PkeyColName: Sequence_cols_name[4],
	}
	for i := 5; i <= 6; i++ {
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_bool),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}

	as.TableDef.Cols = cols

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemSequenceRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}

	as.TableDef.Defs = append(as.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return nil

}

func buildDropSequence(stmt *tree.DropSequence, ctx CompilerContext) (*Plan, error) {
	dropSequence := &plan.DropSequence{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "drop multiple (%d) Sequence in one statement", len(stmt.Names))
	}
	dropSequence.Database = string(stmt.Names[0].SchemaName)
	if dropSequence.Database == "" {
		dropSequence.Database = ctx.DefaultDatabase()
	}
	dropSequence.Table = string(stmt.Names[0].ObjectName)

	obj, tableDef, err := ctx.Resolve(dropSequence.Database, dropSequence.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil || tableDef.TableType != catalog.SystemSequenceRel {
		if !dropSequence.IfExists {
			return nil, moerr.NewNoSuchSequence(ctx.GetContext(), dropSequence.Database, dropSequence.Table)
		}
		dropSequence.Table = ""
	}
	if obj != nil && obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop sequence in subscription database")
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_SEQUENCE,
				Definition: &plan.DataDefinition_DropSequence{
					DropSequence: dropSequence,
				},
			},
		},
	}, nil
}

func buildAlterSequence(stmt *tree.AlterSequence, ctx CompilerContext) (*Plan, error) {
	if stmt.Type == nil && stmt.IncrementBy == nil && stmt.MaxValue == nil && stmt.MinValue == nil && stmt.StartWith == nil && stmt.Cycle == nil {
		return nil, moerr.NewSyntaxErrorf(ctx.GetContext(), "synatx error, %s has nothing to alter", string(stmt.Name.ObjectName))
	}

	alterSequence := &plan.AlterSequence{
		IfExists: stmt.IfExists,
		TableDef: &TableDef{
			Name: string(stmt.Name.ObjectName),
		},
	}
	// Get database name.
	if len(stmt.Name.SchemaName) == 0 {
		alterSequence.Database = ctx.DefaultDatabase()
	} else {
		alterSequence.Database = string(stmt.Name.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(alterSequence.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter sequence in subscription database")
	}

	err := buildAlterSequenceTableDef(stmt, ctx, alterSequence)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_SEQUENCE,
				Definition: &plan.DataDefinition_AlterSequence{
					AlterSequence: alterSequence,
				},
			},
		},
	}, nil
}

func buildCreateSequence(stmt *tree.CreateSequence, ctx CompilerContext) (*Plan, error) {
	createSequence := &plan.CreateSequence{
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			Name: string(stmt.Name.ObjectName),
		},
	}
	// Get database name.
	if len(stmt.Name.SchemaName) == 0 {
		createSequence.Database = ctx.DefaultDatabase()
	} else {
		createSequence.Database = string(stmt.Name.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(createSequence.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create sequence in subscription database")
	}

	err := buildSequenceTableDef(stmt, ctx, createSequence)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_SEQUENCE,
				Definition: &plan.DataDefinition_CreateSequence{
					CreateSequence: createSequence,
				},
			},
		},
	}, nil
}

// preserveIndexSessionVars re-attaches algo_params.session_vars from the source
// table def onto a freshly-built CLONE/LIKE plan's matching index defs.
// ConstructCreateTableSQL rebuilds each index from its flat options only and
// drops session_vars (it isn't an index option), so without this the clone (the
// restore mechanism) loses the captured build-time vars — e.g.
// kmeans_train_percent — that the background restore reindex needs to reproduce
// the original build instead of falling back to defaults.
func preserveIndexSessionVars(p *Plan, src *plan.TableDef) error {
	if p == nil || src == nil {
		return nil
	}
	ct := p.GetDdl().GetCreateTable()
	if ct == nil || ct.GetTableDef() == nil {
		return nil
	}
	for _, ni := range ct.GetTableDef().Indexes {
		for _, si := range src.Indexes {
			if si.IndexName != ni.IndexName || si.IndexAlgoTableType != ni.IndexAlgoTableType {
				continue
			}
			sv, err := catalog.IndexParamsSessionVars(si.IndexAlgoParams)
			if err != nil {
				return err
			}
			if len(sv) == 0 {
				break // source carries no session_vars — nothing to preserve
			}
			flat, err := catalog.IndexParamsStringToMap(ni.IndexAlgoParams)
			if err != nil {
				return err
			}
			merged, err := catalog.IndexParamsMapToJsonStringWithSessionVars(flat, sv)
			if err != nil {
				return err
			}
			ni.IndexAlgoParams = merged
			break
		}
	}
	return nil
}

// preserveChecksForCreateLike installs deep copies of the source table's
// structured CHECK metadata after the LIKE table skeleton has been rebuilt.
// OriginSql is formatted for the source table's creation SQL mode, so it must
// not be reparsed in the LIKE session's mode.
func preserveChecksForCreateLike(p *Plan, src *plan.TableDef) {
	if p == nil || src == nil || len(src.Checks) == 0 {
		return
	}
	ct := p.GetDdl().GetCreateTable()
	if ct == nil || ct.GetTableDef() == nil {
		return
	}
	dstChecks := make([]*plan.CheckDef, len(src.Checks))
	for i, srcCheck := range src.Checks {
		if srcCheck == nil {
			continue
		}
		dstChecks[i] = &plan.CheckDef{
			Name:      srcCheck.Name,
			Check:     DeepCopyExpr(srcCheck.Check),
			OriginSql: srcCheck.OriginSql,
		}
	}
	ct.GetTableDef().Checks = dstChecks
}

func bindLegacyChecks(
	ctx CompilerContext,
	tableDef *plan.TableDef,
	sqlMode string,
) ([]*plan.CheckDef, bool, error) {
	stmt, err := parsers.ParseOneWithSQLMode(
		ctx.GetContext(),
		dialect.MYSQL,
		tableDef.Createsql,
		ctx.GetLowerCaseTableNames(),
		sqlMode,
	)
	if err != nil {
		return nil, false, err
	}
	defer stmt.Free()

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		return nil, true, moerr.NewInvalidInput(
			ctx.GetContext(),
			"legacy CHECK metadata is not a CREATE TABLE statement",
		)
	}

	scratch := &plan.TableDef{
		Name: tableDef.Name,
		Cols: tableDef.Cols,
	}
	for _, def := range createStmt.Defs {
		switch typedDef := def.(type) {
		case *tree.CheckIndex:
			if !typedDef.Enforced {
				return nil, true, moerr.NewNotSupported(
					ctx.GetContext(),
					"NOT ENFORCED CHECK constraints",
				)
			}
			if err := appendCheckDef(ctx, scratch, typedDef.ConstraintSymbol, typedDef.Expr, -1); err != nil {
				return nil, true, err
			}
		case *tree.ColumnTableDef:
			columnPos := -1
			for _, attr := range typedDef.Attributes {
				check, ok := attr.(*tree.AttributeCheckConstraint)
				if !ok {
					continue
				}
				// CREATE SQL can be stale after an earlier ALTER, but columns
				// without a column-level CHECK are irrelevant to recovery. Resolve
				// the catalog position only when this column owns a CHECK.
				if columnPos == -1 {
					columnPos = slices.IndexFunc(
						tableDef.Cols,
						func(col *plan.ColDef) bool { return col.Name == typedDef.Name.ColName() },
					)
					if columnPos == -1 {
						return nil, true, moerr.NewInvalidInputf(
							ctx.GetContext(),
							"legacy CHECK column '%s' does not exist",
							typedDef.Name.ColNameOrigin(),
						)
					}
				}
				if !check.Enforced {
					return nil, true, moerr.NewNotSupported(
						ctx.GetContext(),
						"NOT ENFORCED CHECK constraints",
					)
				}
				if err := appendCheckDef(ctx, scratch, check.Name, check.Expr, columnPos); err != nil {
					return nil, true, err
				}
			}
		}
	}
	return scratch.Checks, true, nil
}

func equalCheckDefs(left, right []*plan.CheckDef) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !proto.Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

// recoverLegacyChecks rebuilds structured CHECK metadata for
// pre-upgrade tables, whose catalog rows contain only the original CREATE SQL.
// The old catalog did not record the creating SQL mode, so recovery must not
// silently choose between two valid but semantically different parses.
func recoverLegacyChecks(ctx CompilerContext, tableDef *plan.TableDef) error {
	if tableDef == nil || len(tableDef.Checks) > 0 || tableDef.Createsql == "" ||
		tableDef.TableType == catalog.SystemExternalRel ||
		!strings.Contains(strings.ToUpper(tableDef.Createsql), "CHECK") {
		return nil
	}

	var canonicalChecks []*plan.CheckDef
	var firstParseErr error
	var firstBindErr error
	parsedModes := 0
	successfulModes := 0
	for _, sqlMode := range mysql.ParserSQLModeCombinations() {
		checks, parsed, err := bindLegacyChecks(ctx, tableDef, sqlMode)
		if !parsed {
			if firstParseErr == nil {
				firstParseErr = err
			}
			continue
		}
		parsedModes++
		if err != nil {
			if firstBindErr == nil {
				firstBindErr = err
			}
			continue
		}
		if successfulModes == 0 {
			canonicalChecks = checks
		} else if !equalCheckDefs(canonicalChecks, checks) {
			return moerr.NewInvalidInput(
				ctx.GetContext(),
				"cannot recover legacy CHECK constraints with ambiguous SQL mode",
			)
		}
		successfulModes++
	}

	switch {
	case successfulModes > 0 && firstBindErr != nil:
		return moerr.NewInvalidInput(
			ctx.GetContext(),
			"cannot recover legacy CHECK constraints with ambiguous SQL mode",
		)
	case successfulModes > 0:
		tableDef.Checks = canonicalChecks
	case parsedModes > 0:
		return firstBindErr
	default:
		return firstParseErr
	}
	return nil
}

func buildCreateTable(
	ctx CompilerContext,
	stmt *tree.CreateTable,
	cloneStmt *tree.CloneTable,
	isPrepareStmt bool,
) (*Plan, error) {
	tableName := string(stmt.Table.ObjectName)
	if err := validateCreateTableIdentifier(ctx, tableName); err != nil {
		return nil, err
	}
	if err := validateTableDefinitionIdentifiers(ctx.GetContext(), stmt.Defs); err != nil {
		return nil, err
	}

	if stmt.IsAsLike {
		var err error
		oldTable := stmt.LikeTableName
		newTable := stmt.Table
		tblName := string(oldTable.ObjectName)
		dbName := string(oldTable.SchemaName)

		snapshot := ctx.GetSnapshot()

		if dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx, snapshot); err != nil {
			return nil, err
		}

		// check if the database is a subscription
		sub, err := ctx.GetSubscriptionMeta(dbName, snapshot)
		if err != nil {
			return nil, err
		}
		previousSubscription := ctx.GetQueryingSubscription()
		if sub != nil {
			ctx.SetQueryingSubscription(sub)
			defer func() {
				ctx.SetQueryingSubscription(previousSubscription)
			}()
		}

		_, tableDef, err := ctx.Resolve(dbName, tblName, snapshot)
		if err != nil {
			return nil, err
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
		}
		if err := validateTableIndexDefinitions(tableDef); err != nil {
			return nil, err
		}
		// Resolve and rewrite a private source definition. Resolve can return a
		// cached TableDef, and the reconstruction below changes its table name.
		tableDef = DeepCopyTableDef(tableDef, true)
		// IndexDef.Visible is ambiguous for pre-upgrade tables. Resolve the
		// authoritative catalog value before CREATE TABLE LIKE/CLONE serializes
		// a local source definition and asks the normal CREATE planner to rebuild
		// it. A subscription definition belongs to the publisher, whose catalog
		// is not available through this compiler context.
		if sub == nil {
			if err := reconcileIndexVisibility(ctx, tableDef.TblId, tableDef, snapshot); err != nil {
				return nil, err
			}
		}
		hadStructuredChecks := len(tableDef.Checks) > 0
		if err := recoverLegacyChecks(ctx, tableDef); err != nil {
			return nil, err
		}
		recoveredLegacyChecks := !hadStructuredChecks && len(tableDef.Checks) > 0
		if len(tableDef.Checks) > 0 {
			if err := requireCheckConstraintProtocol(ctx.GetContext(), ctx.GetProcess()); err != nil {
				return nil, err
			}
		}
		// TODO WHY?
		if tableDef.TableType == catalog.SystemViewRel ||
			tableDef.TableType == catalog.SystemExternalRel ||
			tableDef.TableType == catalog.SystemSequenceRel {
			isIceberg, err := IsIcebergTableDef(ctx.GetContext(), tableDef)
			if err != nil {
				return nil, err
			}
			if isIceberg {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "cannot create table like Iceberg table mapping %s.%s", dbName, tblName)
			}
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "%s.%s is not BASE TABLE", dbName, tblName)
		}

		tableDef.Name = string(newTable.ObjectName)
		tableDef.DbName = string(newTable.SchemaName)
		if len(tableDef.DbName) == 0 {
			tableDef.DbName = ctx.DefaultDatabase()
		}
		tableDef.IsTemporary = stmt.Temporary

		// CHECK expressions are stored in source-session SQL syntax. Exclude them
		// from the temporary SQL skeleton because the rewrite parser uses the
		// current/default SQL mode, then restore the structured metadata below.
		likeSkeletonDef := normalizeLegacyTextCollationForCreateLike(tableDef)
		if sub != nil {
			ctx.SetQueryingSubscription(previousSubscription)
		}
		_, newStmt, err := constructCreateTableSQL(
			ctx,
			likeSkeletonDef,
			snapshot,
			true,
			cloneStmt,
			recoveredLegacyChecks,
			sub,
		)
		if err != nil {
			return nil, err
		}
		if stmtLike, ok := newStmt.(*tree.CreateTable); ok {
			// The subscription binding belongs to the LIKE source only. The
			// rewritten statement names the local target, so plan it with the
			// caller's subscription context instead of the publisher binding.
			// ConstructCreateTableSQL emits a bare `CREATE TABLE ...` without the
			// IF NOT EXISTS clause, so propagate the original flag. Otherwise
			// `CREATE TABLE IF NOT EXISTS T LIKE S` errors with "table already
			// exists" when T exists instead of being a no-op (issue #25119).
			stmtLike.IfNotExists = stmt.IfNotExists
			p, err := buildCreateTable(ctx, stmtLike, nil, isPrepareStmt)
			if err != nil {
				return nil, err
			}
			// ConstructCreateTableSQL above rebuilds each index from its flat
			// options only (session_vars is not an index option), so re-attach the
			// source's algo_params.session_vars onto the clone — otherwise the
			// restore reindex loses the captured build-time vars (e.g.
			// kmeans_train_percent) and falls back to defaults.
			if err := preserveIndexSessionVars(p, tableDef); err != nil {
				return nil, err
			}
			preserveChecksForCreateLike(p, tableDef)
			return p, nil
		}

		return nil, moerr.NewInternalError(ctx.GetContext(), "rewrite for create table like failed")
	}

	createTable := &plan.CreateTable{
		IfNotExists: stmt.IfNotExists,
		Temporary:   stmt.Temporary,
		TableDef: &TableDef{
			Name: string(stmt.Table.ObjectName),
		},
	}

	if stmt.PartitionOption != nil {
		createTable.RawSQL = canonicalCreateTableSQL(stmt)
		createTable.TableDef.FeatureFlag |= features.Partitioned
	}

	// get database name
	if len(stmt.Table.SchemaName) == 0 {
		createTable.Database = ctx.DefaultDatabase()
	} else {
		createTable.Database = string(stmt.Table.SchemaName)
	}

	if stmt.Temporary && stmt.PartitionOption != nil {
		return nil, moerr.NewPartitionNoTemporary(ctx.GetContext())
	}

	if sub, err := ctx.GetSubscriptionMeta(createTable.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create table in subscription database")
	}

	// set tableDef
	var err error
	var asSelectCols []*ColDef
	var asSelectQuery *Query
	if stmt.IsAsSelect {
		if asSelectCols, asSelectQuery, err = genAsSelectCols(ctx, stmt.AsSource, isPrepareStmt); err != nil {
			return nil, err
		}
	}

	if err = buildTableDefs(stmt, ctx, createTable, asSelectCols); err != nil {
		return nil, err
	}

	v, ok := getAutoIncrementOffsetFromVariables(ctx)
	if ok {
		createTable.TableDef.AutoIncrOffset = v
	}

	// set option
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.TableOptionProperties:
			properties := make([]*plan.Property, len(opt.Preperties))
			for idx, property := range opt.Preperties {
				properties[idx] = &plan.Property{
					Key:   property.Key,
					Value: property.Value,
				}
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		// todo confirm: option data store like this?
		case *tree.TableOptionComment:
			if getNumOfCharacters(opt.Comment) > maxLengthOfTableComment {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "comment for field '%s' is too long", createTable.TableDef.Name)
			}

			properties := []*plan.Property{
				{
					Key:   catalog.SystemRelAttr_Comment,
					Value: opt.Comment,
				},
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		case *tree.TableOptionAutoIncrement:
			if opt.Value != 0 {
				createTable.TableDef.AutoIncrOffset = autoIncrementValueToOffset(opt.Value)
			}

		// these table options is not support in plan
		// case *tree.TableOptionEngine, *tree.TableOptionSecondaryEngine, *tree.TableOptionCharset,
		// 	*tree.TableOptionCollate, *tree.TableOptionAutoIncrement, *tree.TableOptionComment,
		// 	*tree.TableOptionAvgRowLength, *tree.TableOptionChecksum, *tree.TableOptionCompression,
		// 	*tree.TableOptionConnection, *tree.TableOptionPassword, *tree.TableOptionKeyBlockSize,
		// 	*tree.TableOptionMaxRows, *tree.TableOptionMinRows, *tree.TableOptionDelayKeyWrite,
		// 	*tree.TableOptionRowFormat, *tree.TableOptionStatsPersistent, *tree.TableOptionStatsAutoRecalc,
		// 	*tree.TableOptionPackKeys, *tree.TableOptionTablespace, *tree.TableOptionDataDirectory,
		// 	*tree.TableOptionIndexDirectory, *tree.TableOptionStorageMedia, *tree.TableOptionStatsSamplePages,
		// 	*tree.TableOptionUnion, *tree.TableOptionEncryption:
		// 	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
		case *tree.TableOptionAUTOEXTEND_SIZE, *tree.TableOptionAvgRowLength,
			*tree.TableOptionCharset, *tree.TableOptionChecksum, *tree.TableOptionCollate, *tree.TableOptionCompression,
			*tree.TableOptionConnection, *tree.TableOptionDataDirectory, *tree.TableOptionIndexDirectory,
			*tree.TableOptionDelayKeyWrite, *tree.TableOptionEncryption, *tree.TableOptionEngine, *tree.TableOptionEngineAttr,
			*tree.TableOptionKeyBlockSize, *tree.TableOptionMaxRows, *tree.TableOptionMinRows, *tree.TableOptionPackKeys,
			*tree.TableOptionPassword, *tree.TableOptionRowFormat, *tree.TableOptionStartTrans, *tree.TableOptionSecondaryEngineAttr,
			*tree.TableOptionStatsAutoRecalc, *tree.TableOptionStatsPersistent, *tree.TableOptionStatsSamplePages,
			*tree.TableOptionTablespace, *tree.TableOptionUnion:

		default:
			return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
		}
	}

	// After handleTableOptions, so begin the partitions processing depend on TableDef
	if stmt.IcebergParam != nil {
		if err := ensureIcebergTableSurfaceEnabled(ctx.GetContext(), "CREATE EXTERNAL TABLE ENGINE=ICEBERG"); err != nil {
			return nil, err
		}
		spec, err := sqliceberg.ParseTableMappingSpec(ctx.GetContext(), stmt.IcebergParam)
		if err != nil {
			return nil, err
		}
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemExternalRel,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: sqliceberg.BuildCreateSQLEnvelope(spec.Mapping, spec.CatalogName),
			},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	} else if stmt.MongoDBParam != nil {
		if err := ensureMongoDBTableSurfaceEnabled(ctx.GetContext()); err != nil {
			return nil, err
		}
		spec, err := sqlmongodb.ParseTableMappingSpec(ctx.GetContext(), stmt.MongoDBParam, stmt.Defs, createTable.TableDef)
		if err != nil {
			return nil, err
		}
		// FeatureFlag is durable, planner-owned catalog metadata. Unlike the
		// user-controlled rel_createsql payload of a generic external table, it
		// is a typed discriminator that cannot be injected through filepath JSON.
		createTable.TableDef.FeatureFlag |= features.MongoDBExternal
		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemExternalRel},
			{Key: catalog.SystemRelAttr_CreateSQL, Value: sqlmongodb.BuildCreateSQLEnvelope(spec.Mapping)},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	} else if stmt.DataStreamParam != nil {
		cfg, err := sqldatastream.ParseTableOptions(ctx.GetContext(), stmt.DataStreamParam)
		if err != nil {
			return nil, err
		}
		// Like MongoDB, the durable typed feature bit is the discriminator that
		// cannot be injected through the user-controlled rel_createsql JSON of a
		// generic external table.
		createTable.TableDef.FeatureFlag |= features.DataStreamExternal
		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemExternalRel},
			{Key: catalog.SystemRelAttr_CreateSQL, Value: sqldatastream.BuildCreateSQLEnvelope(cfg)},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	} else if stmt.ForeignParam != nil {
		cfg, err := foreignext.ParseTableOptions(ctx.GetContext(), stmt.ForeignParam)
		if err != nil {
			return nil, err
		}
		// Validate the JSON shape of an inline config without dialing (the
		// session-variable fallback is resolved at scan time, so there is
		// nothing to validate here for it).
		if cfg.ConfigJSON != "" {
			if err := foreigntvf.ValidateConfig(ctx.GetContext(), foreigntvf.Kind(cfg.Kind), cfg.ConfigJSON); err != nil {
				return nil, err
			}
		}
		// Like MongoDB/datastream, the durable typed feature bit is the
		// discriminator that cannot be injected through the user-controlled
		// rel_createsql JSON of a generic external table.
		createTable.TableDef.FeatureFlag |= features.ForeignExternal
		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemExternalRel},
			{Key: catalog.SystemRelAttr_CreateSQL, Value: foreignext.BuildCreateSQLEnvelope(cfg)},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	} else if stmt.KafkaParam != nil {
		cfg, err := sqlkafka.ParseTableOptions(ctx.GetContext(), stmt.KafkaParam)
		if err != nil {
			return nil, err
		}
		// The consumer group carries the committed-offset exactly-once
		// bookmark; default it per table so it is stable across sessions and
		// persisted concretely in the envelope.
		if cfg.Group == "" {
			cfg.Group = sqlkafka.DefaultGroup(createTable.Database, createTable.TableDef.Name)
		}
		// Like MongoDB/datastream/foreign, the durable typed feature bit is
		// the discriminator that cannot be injected through the
		// user-controlled rel_createsql JSON of a generic external table.
		createTable.TableDef.FeatureFlag |= features.KafkaExternal
		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemExternalRel},
			{Key: catalog.SystemRelAttr_CreateSQL, Value: sqlkafka.BuildCreateSQLEnvelope(cfg)},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	} else if stmt.Param != nil {
		for i := 0; i < len(stmt.Param.Option); i += 2 {
			switch strings.ToLower(stmt.Param.Option[i]) {
			case "endpoint", "region", "access_key_id", "secret_access_key", "bucket", "filepath", "compression", "format", "jsondata", "provider", "role_arn", "external_id", "hive_partitioning", "hive_partition_columns", ExternalWriteFilePatternKey, CSVCommentKey:
			default:
				return nil, moerr.NewBadConfigf(ctx.GetContext(), "the keyword '%s' is not support", strings.ToLower(stmt.Param.Option[i]))
			}
		}

		if err := validateWriteFilePattern(ctx.GetContext(), stmt.Param, createTable.TableDef); err != nil {
			return nil, err
		}

		if err := validateAndSetHivePartitionOptions(ctx.GetContext(), stmt, createTable); err != nil {
			return nil, err
		}

		if err := InitNullMap(stmt.Param, ctx); err != nil {
			return nil, err
		}
		json_byte, err := json.Marshal(stmt.Param)
		if err != nil {
			return nil, err
		}
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemExternalRel,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: string(json_byte),
			},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	} else {
		kind := catalog.SystemOrdinaryRel
		if stmt.IsClusterTable {
			kind = catalog.SystemClusterRel
		}
		// when create hidden talbe(like: auto_incr_table, index_table)， we set relKind to empty
		if catalog.IsHiddenTable(createTable.TableDef.Name) {
			kind = ""
		}
		createSQL := createTableSQLForCatalog(ctx, stmt)
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: kind,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: createSQL,
			},
		}
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindContext := NewBindContext(builder, nil)

	// set partition(unsupport now)
	if stmt.PartitionOption != nil {
		// Foreign keys are not yet supported in conjunction with partitioning
		// see: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-14.html
		if len(createTable.TableDef.Fkeys) > 0 {
			return nil, moerr.NewErrForeignKeyOnPartitioned(ctx.GetContext())
		}

		nodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Stats:       nil,
			ObjRef:      nil,
			TableDef:    createTable.TableDef,
			BindingTags: []int32{builder.genNewBindTag()},
		}, bindContext)

		err = builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
		if err != nil {
			return nil, err
		}

		partitionBinder := NewPartitionBinder(builder, bindContext)
		createTable.TableDef.Partition, err = partitionBinder.buildPartitionDefs(ctx.GetContext(), stmt.PartitionOption)
		if err != nil {
			return nil, err
		}
	}

	if stmt.Temporary {
		catalog.MarkTableDefTemporary(createTable.TableDef)
	}
	if !isPrepareStmt {
		asSelectQuery = nil
	}
	if err := validatePersistedTableIdentifiers(ctx.GetContext(), createTable.TableDef); err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE,
				Query:   asSelectQuery,
				Definition: &plan.DataDefinition_CreateTable{
					CreateTable: createTable,
				},
			},
		},
	}, nil
}

func validateIdentifier(ctx context.Context, name string) error {
	if getNumOfCharacters(name) <= MaxIdentifierLength {
		return nil
	}

	return moerr.NewTooLongIdent(ctx, name)
}

func validateCreateTableIdentifier(ctx CompilerContext, name string) error {
	err := validateIdentifier(ctx.GetContext(), name)
	if err == nil {
		return nil
	}

	// Internal DDL and session-scoped temporary tables can materialize generated
	// physical names with UUID prefixes or suffixes.
	if defines.IsInternalExecutor(ctx.GetContext()) || isGeneratedSessionTempTableName(ctx, name) {
		return nil
	}

	return err
}

func validateTableDefinitionIdentifiers(ctx context.Context, defs tree.TableDefs) error {
	for _, def := range defs {
		if err := validateTableDefinitionIdentifier(ctx, def); err != nil {
			return err
		}
	}
	return nil
}

func validateTableDefinitionIdentifier(ctx context.Context, def tree.TableDef) error {
	validateNames := func(names ...string) error {
		for _, name := range names {
			if err := validateIdentifier(ctx, name); err != nil {
				return err
			}
		}
		return nil
	}

	switch def := def.(type) {
	case *tree.ColumnTableDef:
		if err := validateNames(def.Name.ColNameOrigin()); err != nil {
			return err
		}
		for _, attr := range def.Attributes {
			if check, ok := attr.(*tree.AttributeCheckConstraint); ok {
				if err := validateNames(check.Name); err != nil {
					return err
				}
			}
		}
	case *tree.PrimaryKeyIndex:
		return validateNames(def.Name, def.ConstraintSymbol)
	case *tree.Index:
		return validateNames(def.Name)
	case *tree.UniqueIndex:
		return validateNames(def.Name, def.ConstraintSymbol)
	case *tree.ForeignKey:
		return validateNames(def.Name, def.ConstraintSymbol)
	case *tree.FullTextIndex:
		return validateNames(def.Name)
	case *tree.CheckIndex:
		return validateNames(def.ConstraintSymbol)
	}
	return nil
}

func validatePersistedTableIdentifiers(ctx context.Context, tableDef *plan.TableDef) error {
	if tableDef == nil {
		return nil
	}
	for _, col := range tableDef.Cols {
		if col == nil || col.Hidden {
			continue
		}
		name := col.OriginName
		if name == "" {
			name = col.Name
		}
		if err := validateIdentifier(ctx, name); err != nil {
			return err
		}
	}
	for _, index := range tableDef.Indexes {
		if index != nil {
			if err := validateIdentifier(ctx, index.IndexName); err != nil {
				return err
			}
		}
	}
	for _, foreignKey := range tableDef.Fkeys {
		if foreignKey != nil {
			if err := validateIdentifier(ctx, foreignKey.Name); err != nil {
				return err
			}
		}
	}
	for _, check := range tableDef.Checks {
		if check != nil {
			if err := validateIdentifier(ctx, check.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func isGeneratedSessionTempTableName(ctx CompilerContext, name string) bool {
	rootStmt, err := parsers.ParseOne(
		ctx.GetContext(),
		dialect.MYSQL,
		ctx.GetRootSql(),
		ctx.GetLowerCaseTableNames(),
	)
	if err != nil {
		return false
	}
	defer rootStmt.Free()
	if _, isCreateTable := rootStmt.(*tree.CreateTable); isCreateTable {
		return false
	}

	proc := ctx.GetProcess()
	if proc == nil || proc.GetSessionInfo() == nil {
		return false
	}
	sessionID := strings.ReplaceAll(proc.GetSessionInfo().SessionId.String(), "-", "")
	return strings.HasPrefix(name, defines.TempTableNamePrefix+sessionID+"_")
}

func normalizeLegacyTextCollationForCreateLike(tableDef *plan.TableDef) *plan.TableDef {
	legacy := tableDef.DefaultCharset == uint32(types.CharsetLegacy)
	if !legacy {
		for _, col := range tableDef.Cols {
			switch types.T(col.Typ.Id) {
			case types.T_char, types.T_varchar, types.T_text:
				if col.Typ.Charset == uint32(types.CharsetLegacy) {
					legacy = true
				}
			}
		}
	}
	if !legacy {
		return tableDef
	}

	// Charset zero was persisted before the field had semantics. CREATE LIKE
	// reparses a generated DDL skeleton, so spell legacy text as utf8mb4_bin to
	// retain its historical bytewise ordering. SHOW CREATE uses the same public
	// compatibility spelling for catalog rows that still contain legacy text.
	clone := DeepCopyTableDef(tableDef, true)
	if clone.DefaultCharset == uint32(types.CharsetLegacy) {
		clone.DefaultCharset = uint32(types.CharsetUTF8MB4Bin)
	}
	for _, col := range clone.Cols {
		switch types.T(col.Typ.Id) {
		case types.T_char, types.T_varchar, types.T_text:
			if col.Typ.Charset == uint32(types.CharsetLegacy) {
				col.Typ.Charset = uint32(types.CharsetUTF8MB4Bin)
			}
		}
	}
	return clone
}

func buildTableDefs(stmt *tree.CreateTable, ctx CompilerContext, createTable *plan.CreateTable, asSelectCols []*ColDef) error {
	// all below fields' key is lower case
	var primaryKeys []string
	colMap := make(map[string]*ColDef)
	defaultMap := make(map[string]string)
	uniqueIndexInfos := make([]*tree.UniqueIndex, 0)
	fullTextIndexInfos := make([]*tree.FullTextIndex, 0)
	secondaryIndexInfos := make([]*tree.Index, 0)
	fkDatasOfFKSelfRefer := make([]*FkData, 0)
	dedupFkName := make(UnorderedSet[string])
	type pendingCheckDef struct {
		name       string
		expr       tree.Expr
		columnName string
		enforced   bool
	}
	pendingChecks := make([]pendingCheckDef, 0)
	tableCharset, err := tableDefaultCharset(ctx, stmt.Options)
	if err != nil {
		return err
	}
	createTable.TableDef.DefaultCharset = tableCharset

	if stmt.Param != nil || stmt.IcebergParam != nil || stmt.MongoDBParam != nil {
		if err := rejectExternalTableInlineIndexes(ctx.GetContext(), stmt); err != nil {
			return err
		}
	}
	if stmt.MongoDBParam != nil {
		if err := rejectMongoDBExternalTableOnUpdate(ctx.GetContext(), stmt); err != nil {
			return err
		}
	}

	// Pre-scan all column definitions so that generated columns can reference
	// base columns defined later in the CREATE TABLE statement (forward reference).
	var allColDefs []*ColDef
	var isGeneratedCol []bool
	for _, item := range stmt.Defs {
		if def, ok := item.(*tree.ColumnTableDef); ok {
			cType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			cType.Charset = uint32(types.CharsetType(types.T(cType.Id)))
			if err = applyDefaultAndColumnAttributesToType(ctx.GetContext(), &cType, tableCharset, def.Attributes); err != nil {
				return err
			}
			isGen := false
			for _, attr := range def.Attributes {
				switch attr.(type) {
				case *tree.AttributeGeneratedAlways:
					isGen = true
				case *tree.AttributeAutoIncrement:
					cType.AutoIncr = true
				}
			}
			allColDefs = append(allColDefs, &ColDef{Name: def.Name.ColName(), Typ: cType})
			isGeneratedCol = append(isGeneratedCol, isGen)
		}
	}

	genColIdx := 0 // tracks the current column's position in allColDefs
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			colType.Charset = uint32(types.CharsetType(types.T(colType.Id)))
			if err = applyDefaultAndColumnAttributesToType(ctx.GetContext(), &colType, tableCharset, def.Attributes); err != nil {
				return err
			}
			if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) ||
				colType.Id == int32(types.T_binary) || colType.Id == int32(types.T_varbinary) {
				if colType.GetWidth() > types.MaxStringSize {
					return moerr.NewInvalidInputf(ctx.GetContext(), "string width (%d) is too long", colType.GetWidth())
				}
			}
			if colType.Id == int32(types.T_array_float32) || colType.Id == int32(types.T_array_float64) {
				if colType.GetWidth() > types.MaxArrayDimension {
					return moerr.NewInvalidInputf(ctx.GetContext(), "vector width (%d) is too long", colType.GetWidth())
				}
			}
			if colType.Id == int32(types.T_bit) {
				if colType.Width == 0 {
					colType.Width = 1
				}
				if colType.Width > types.MaxBitLen {
					return moerr.NewInvalidInputf(ctx.GetContext(), "bit width (%d) is too long (max = %d) ", colType.GetWidth(), types.MaxBitLen)
				}
			}
			var pks []string
			var comment string
			var auto_incr bool
			var isGenerated bool
			colName := def.Name.ColName()
			// only used in error message and ColDef.OriginName
			colNameOrigin := def.Name.ColNameOrigin()
			// __mo_filepath / __mo_query are the synthetic hidden columns of
			// external scans and are hidden BY NAME in star expansion and the
			// external readers; a real column with either name would silently
			// disappear from SELECT * or shadow the synthetic value.
			if catalog.IsReservedExternalColName(colName) {
				return moerr.NewInvalidInputf(ctx.GetContext(),
					"column name %s is reserved for external table scans", colNameOrigin)
			}
			for _, attr := range def.Attributes {
				switch attribute := attr.(type) {
				case *tree.AttributeCheckConstraint:
					if err := rejectWindowFunction(ctx, attribute.Expr); err != nil {
						return err
					}
				case *tree.AttributeGeneratedAlways:
					isGenerated = true
				case *tree.AttributePrimaryKey, *tree.AttributeKey:
					if colType.GetId() == int32(types.T_blob) {
						return moerr.NewNotSupported(ctx.GetContext(), "blob type in primary key")
					}
					if colType.GetId() == int32(types.T_text) {
						return moerr.NewNotSupported(ctx.GetContext(), "text type in primary key")
					}
					if colType.GetId() == int32(types.T_datalink) {
						return moerr.NewNotSupported(ctx.GetContext(), "datalink type in primary key")
					}
					if colType.GetId() == int32(types.T_json) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in primary key", colNameOrigin))
					}
					if types.T(colType.GetId()).IsArrayRelate() {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("VECTOR column '%s' cannot be in primary key", colNameOrigin))
					}
					if isSetPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("SET column '%s' cannot be in primary key", colNameOrigin))

					}
					if isGeometryPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("GEOMETRY column '%s' cannot be in primary key", colNameOrigin))
					}
					pks = append(pks, colName)
				case *tree.AttributeComment:
					comment = attribute.CMT.String()
					if getNumOfCharacters(comment) > maxLengthOfColumnComment {
						return moerr.NewInvalidInputf(ctx.GetContext(), "comment for column '%s' is too long", colNameOrigin)
					}
				case *tree.AttributeAutoIncrement:
					auto_incr = true
					if !types.T(colType.GetId()).IsInteger() {
						return moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
					}
				case *tree.AttributeUnique, *tree.AttributeUniqueKey:
					if isSetPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("SET column '%s' cannot be in unique index", colNameOrigin))

					}
					if isGeometryPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("GEOMETRY column '%s' cannot be in unique index", colNameOrigin))
					}
					uniqueIndexInfos = append(uniqueIndexInfos, &tree.UniqueIndex{
						KeyParts: []*tree.KeyPart{{ColName: def.Name}},
						Name:     colName,
					})
				}
			}
			if len(pks) > 0 {
				if len(primaryKeys) > 0 {
					return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
				}
				primaryKeys = pks
			}

			var defaultValue *plan.Default
			var onUpdateExpr *plan.OnUpdate
			var generatedCol *plan.GeneratedCol

			if isGenerated {
				// Build generated column expression using the full column list
				// so that base columns defined later can be referenced (forward reference).
				generatedCol, err = buildGeneratedExpr(def, colType, allColDefs, ctx.GetProcess())
				if err != nil {
					return err
				}
				// Self-reference is still invalid even though base-column forward references are allowed.
				if exprReferencesColumn(generatedCol.Expr, colName, allColDefs) {
					return moerr.NewInvalidInputf(ctx.GetContext(), "generated column '%s' cannot refer to itself", colNameOrigin)
				}
				// Validate: no forward reference to generated columns defined later
				if err := validateNoForwardGenRef(ctx.GetContext(), generatedCol.Expr, genColIdx, allColDefs, isGeneratedCol); err != nil {
					return err
				}
				// Generated columns preserve declared nullability but use no default expr for storage layer compatibility
				defaultValue = &plan.Default{
					NullAbility:  getColumnNullAbility(def),
					Expr:         nil,
					OriginString: "",
				}
			} else {
				defaultValue, err = buildDefaultExpr(def, colType, ctx.GetProcess())
				if err != nil {
					return err
				}
				if auto_incr && defaultValue.Expr != nil {
					return moerr.NewInvalidInputf(ctx.GetContext(), "invalid default value for '%s'", colNameOrigin)
				}

				onUpdateExpr, err = buildOnUpdate(def, colType, ctx.GetProcess())
				if err != nil {
					return err
				}
			}

			if !checkTableColumnNameValid(colName) {
				return moerr.NewInvalidInputf(ctx.GetContext(), "table column name '%s' is illegal and conflicts with internal keyword", colNameOrigin)
			}

			colType.AutoIncr = auto_incr
			col := &ColDef{
				Name:         colName,
				OriginName:   colNameOrigin,
				Alg:          plan.CompressType_Lz4,
				Typ:          colType,
				Default:      defaultValue,
				OnUpdate:     onUpdateExpr,
				Comment:      comment,
				GeneratedCol: generatedCol,
			}
			// if same name col in asSelectCols, overwrite it; add into colMap && createTable.TableDef.Cols later
			if idx := slices.IndexFunc(asSelectCols, func(c *ColDef) bool { return c.Name == col.Name }); idx != -1 {
				asSelectCols[idx] = col
			} else {
				colMap[colName] = col
				createTable.TableDef.Cols = append(createTable.TableDef.Cols, col)

				// get default val from ast node
				attrIdx := slices.IndexFunc(def.Attributes, func(a tree.ColumnAttribute) bool {
					_, ok := a.(*tree.AttributeDefault)
					return ok
				})
				if attrIdx != -1 {
					defaultAttr := def.Attributes[attrIdx].(*tree.AttributeDefault)
					fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
					defaultAttr.Format(fmtCtx)
					// defaultAttr.Format start with "default ", trim first 8 chars
					defaultMap[colName] = fmtCtx.String()[8:]
				} else {
					defaultMap[colName] = "NULL"
				}
			}
			for _, attr := range def.Attributes {
				check, ok := attr.(*tree.AttributeCheckConstraint)
				if !ok {
					continue
				}
				if stmt.Param != nil || stmt.IcebergParam != nil || stmt.MongoDBParam != nil {
					return moerr.NewNotSupported(
						ctx.GetContext(),
						"CHECK constraints on external tables",
					)
				}
				pendingChecks = append(pendingChecks, pendingCheckDef{
					name:       check.Name,
					expr:       check.Expr,
					columnName: colName,
					enforced:   check.Enforced,
				})
			}
			genColIdx++
		case *tree.PrimaryKeyIndex:
			if len(primaryKeys) > 0 {
				return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
			}
			pksMap := map[string]bool{}
			for _, key := range def.KeyParts {
				name := key.ColName.ColName() // name of primary key column
				if _, ok := pksMap[name]; ok {
					return moerr.NewInvalidInputf(ctx.GetContext(), "duplicate column name '%s' in primary key", key.ColName.ColNameOrigin())
				}

				if col, ok := colMap[name]; ok {
					if err := checkIndexColumnSupportability(ctx.GetContext(), col, key, "primary"); err != nil {
						return err
					}
				}

				primaryKeys = append(primaryKeys, name)
				pksMap[name] = true
			}
		case *tree.Index:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}
			if err = checkSpatialIndexColumnSupport(ctx, def, colMap); err != nil {
				return err
			}

			secondaryIndexInfos = append(secondaryIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()

				if col, ok := colMap[name]; ok {
					if err := checkIndexColumnSupportability(ctx.GetContext(), col, key, indexColumnCheckKind(def.KeyType)); err != nil {
						return err
					}
				}
			}
		case *tree.UniqueIndex:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}

			uniqueIndexInfos = append(uniqueIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()

				if col, ok := colMap[name]; ok {
					if err := checkIndexColumnSupportability(ctx.GetContext(), col, key, "unique"); err != nil {
						return err
					}
				}
			}
		case *tree.FullTextIndex:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}

			fullTextIndexInfos = append(fullTextIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()
				if col, ok := colMap[name]; ok {
					if col.Typ.Id == int32(types.T_blob) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("BLOB column '%s' cannot be in index", key.ColName.ColNameOrigin()))
					}
				}
			}
		case *tree.ForeignKey:
			if stmt.MongoDBParam != nil {
				return moerr.NewNotSupported(
					ctx.GetContext(),
					"FOREIGN KEY constraints on MongoDB external tables",
				)
			}
			if createTable.Temporary {
				return moerr.NewNotSupported(ctx.GetContext(), "add foreign key for temporary table")
			}
			if len(asSelectCols) != 0 {
				return moerr.NewNYI(ctx.GetContext(), "add foreign key in create table ... as select statement")
			}
			if IsFkBannedDatabase(createTable.Database) {
				return moerr.NewInternalErrorf(ctx.GetContext(), "can not create foreign keys in %s", createTable.Database)
			}
			err := adjustConstraintName(ctx.GetContext(), def)
			if err != nil {
				return err
			}
			fkData, err := getForeignKeyData(ctx, createTable.Database, createTable.TableDef, def)
			if err != nil {
				return err
			}

			if def.ConstraintSymbol != fkData.Def.Name {
				return moerr.NewInternalErrorf(ctx.GetContext(), "different fk name %s %s", def.ConstraintSymbol, fkData.Def.Name)
			}

			// dedup
			if dedupFkName.Find(fkData.Def.Name) {
				return moerr.NewInternalErrorf(ctx.GetContext(), "duplicate fk name %s", fkData.Def.Name)
			}
			dedupFkName.Insert(fkData.Def.Name)

			// only setups foreign key without forward reference
			if !fkData.ForwardRefer {
				createTable.FkDbs = append(createTable.FkDbs, fkData.ParentDbName)
				createTable.FkTables = append(createTable.FkTables, fkData.ParentTableName)
				createTable.FkCols = append(createTable.FkCols, fkData.Cols)
				createTable.TableDef.Fkeys = append(createTable.TableDef.Fkeys, fkData.Def)
			}

			// save self reference foreign keys
			if fkData.IsSelfRefer {
				fkDatasOfFKSelfRefer = append(fkDatasOfFKSelfRefer, fkData)
			} else {
				createTable.UpdateFkSqls = append(createTable.UpdateFkSqls, fkData.UpdateSql)
			}
		case *tree.CheckIndex:
			if err := rejectWindowFunction(ctx, def.Expr); err != nil {
				return err
			}
			if stmt.Param != nil || stmt.IcebergParam != nil || stmt.MongoDBParam != nil {
				return moerr.NewNotSupported(
					ctx.GetContext(),
					"CHECK constraints on external tables",
				)
			}
			pendingChecks = append(pendingChecks, pendingCheckDef{
				name:     def.ConstraintSymbol,
				expr:     def.Expr,
				enforced: def.Enforced,
			})
		default:
			return moerr.NewNYIf(ctx.GetContext(), "table def: '%v'", def)
		}
	}

	if stmt.IsAsSelect {
		// add as select cols
		for _, col := range asSelectCols {
			if !checkTableColumnNameValid(col.Name) {
				colName := col.OriginName
				if colName == "" {
					colName = col.Name
				}
				return moerr.NewInvalidInputf(
					ctx.GetContext(),
					"table column name '%s' is illegal and conflicts with internal keyword",
					colName,
				)
			}
			colMap[col.Name] = col
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, col)
		}

		// insert into new_table select default_val1, default_val2, ..., * from (select clause);
		var insertSqlBuilder strings.Builder
		insertSqlBuilder.WriteString("insert into ")
		targetFmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteIdentifier())
		targetFmtCtx.WriteIdentifier(tree.Identifier(createTable.Database))
		targetFmtCtx.WriteByte('.')
		targetFmtCtx.WriteIdentifier(tree.Identifier(createTable.TableDef.Name))
		insertSqlBuilder.WriteString(targetFmtCtx.String())
		insertSqlBuilder.WriteString(" select ")

		cols := createTable.TableDef.Cols
		firstCol := true
		for i := range cols {
			// insert default values if col[i] only in create clause
			if !slices.ContainsFunc(asSelectCols, func(c *ColDef) bool { return c.Name == cols[i].Name }) {
				if !firstCol {
					insertSqlBuilder.WriteString(", ")
				}
				insertSqlBuilder.WriteString(defaultMap[cols[i].Name])
				firstCol = false
			}
		}
		if !firstCol {
			insertSqlBuilder.WriteString(", ")
		}
		// add all cols from select clause
		insertSqlBuilder.WriteString("*")

		// from
		// The generated INSERT ... SELECT is re-parsed by the internal SQL
		// executor, which always parses in DEFAULT sql_mode (parsers.Parse
		// passes an empty mode) regardless of the session's mode. So string
		// literals here must be default-escaped: a backslash stored literally
		// under a NO_BACKSLASH_ESCAPES session is emitted doubled and the
		// default-mode reparse reduces it back to the original literal. Do
		// NOT make this formatting session-mode-aware unless the internal
		// executor's parse becomes session-mode-aware too (#24823).
		fmtCtx := tree.NewFmtCtx(
			dialect.MYSQL,
			tree.WithQuoteString(true),
			tree.WithQuoteIdentifier(),
		)
		stmt.AsSource.Format(fmtCtx)
		insertSqlBuilder.WriteString(fmt.Sprintf(" from (%s) as __mo_ctas_source", restoreIntervalSyntaxForCTAS(fmtCtx.String())))

		createTable.CreateAsSelectSql = insertSqlBuilder.String()
	}

	for _, check := range pendingChecks {
		if !check.enforced {
			return moerr.NewNotSupported(
				ctx.GetContext(),
				"NOT ENFORCED CHECK constraints",
			)
		}
		columnPos := -1
		if check.columnName != "" {
			columnPos = slices.IndexFunc(
				createTable.TableDef.Cols,
				func(col *ColDef) bool { return col.Name == check.columnName },
			)
			if columnPos == -1 {
				return moerr.NewInternalErrorf(
					ctx.GetContext(),
					"column check constraint references missing column '%s'",
					check.columnName,
				)
			}
		}
		if err := appendCheckDef(ctx, createTable.TableDef, check.name, check.expr, columnPos); err != nil {
			return err
		}
	}

	// table must have one visible column
	if len(createTable.TableDef.Cols) == 0 {
		return moerr.NewTableMustHaveVisibleColumn(ctx.GetContext())
	}

	// add cluster table attribute
	if stmt.IsClusterTable {
		internal := defines.IsInternalExecutor(ctx.GetContext())
		_, has := colMap[util.GetClusterTableAttributeName()]
		if has && !internal {
			return moerr.NewInvalidInput(ctx.GetContext(), "the attribute account_id in the cluster table can not be defined directly by the user")
		}
		if !has {
			colType, err := getTypeFromAst(ctx.GetContext(), util.GetClusterTableAttributeType())
			if err != nil {
				return err
			}
			colDef := &ColDef{
				Name:    util.GetClusterTableAttributeName(),
				Alg:     plan.CompressType_Lz4,
				Typ:     colType,
				NotNull: true,
				Default: &plan.Default{
					Expr: &Expr{
						Expr: &plan.Expr_Lit{
							Lit: &Const{
								Isnull: false,
								Value:  &plan.Literal_U32Val{U32Val: catalog.System_Account},
							},
						},
						Typ: plan.Type{
							Id:          colType.Id,
							NotNullable: true,
						},
					},
					NullAbility: false,
				},
				Comment: "the account_id added by the mo",
			}
			colMap[util.GetClusterTableAttributeName()] = colDef
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
		}
	}

	pkeyName := ""
	// If the primary key is explicitly defined in the ddl statement
	if len(primaryKeys) > 0 {
		for _, primaryKey := range primaryKeys {
			if _, ok := colMap[primaryKey]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' doesn't exist in table", primaryKey)
			}
			// Reject VIRTUAL generated columns in PRIMARY KEY
			col := colMap[primaryKey]
			if col.GeneratedCol != nil && !col.GeneratedCol.IsStored {
				return moerr.NewNotSupported(ctx.GetContext(),
					fmt.Sprintf("defining a virtual generated column '%s' as primary key", col.OriginName))
			}
		}
		if len(primaryKeys) == 1 {
			pkeyName = primaryKeys[0]
			for _, col := range createTable.TableDef.Cols {
				if col.Name == pkeyName {
					col.Primary = true
					createTable.TableDef.Pkey = &PrimaryKeyDef{
						Names:       primaryKeys,
						PkeyColName: pkeyName,
					}
					break
				}
			}
		} else {
			// pkeyName = util.BuildCompositePrimaryKeyColumnName(primaryKeys)
			pkeyName = catalog.CPrimaryKeyColName
			colDef := MakeHiddenColDefByName(pkeyName)
			colDef.Primary = true
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[pkeyName] = colDef

			pkeyDef := &PrimaryKeyDef{
				Names:       primaryKeys,
				PkeyColName: pkeyName,
				CompPkeyCol: colDef,
			}
			createTable.TableDef.Pkey = pkeyDef
		}
		for _, primaryKey := range primaryKeys {
			colMap[primaryKey].Default.NullAbility = false
			colMap[primaryKey].NotNull = true
		}
	} else {
		// If table does not have a explicit primary key in the ddl statement, a new hidden primary key column will be add,
		// which will not be sorted or used for any other purpose, but will only be used to add
		// locks to the Lock operator in pessimistic transaction mode.
		if !createTable.IsSystemExternalRel() {
			pkeyName = catalog.FakePrimaryKeyColName
			colDef := &ColDef{
				ColId:  uint64(len(createTable.TableDef.Cols)),
				Name:   pkeyName,
				Hidden: true,
				Typ: Type{
					Id:       int32(types.T_uint64),
					AutoIncr: true,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
				NotNull: true,
				Primary: true,
			}

			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[pkeyName] = colDef

			createTable.TableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{pkeyName},
				PkeyColName: pkeyName,
			}

			idx := len(createTable.TableDef.Cols) - 1
			// FIXME: due to the special treatment of insert and update for composite primary key, cluster-by, the
			// hidden primary key cannot be placed in the last column, otherwise it will cause the columns sent to
			// tae will not match the definition of schema, resulting in panic.
			if createTable.TableDef.ClusterBy != nil &&
				len(stmt.ClusterByOption.ColumnList) > 1 {
				// we must swap hide pk and cluster_by
				createTable.TableDef.Cols[idx-1], createTable.TableDef.Cols[idx] = createTable.TableDef.Cols[idx], createTable.TableDef.Cols[idx-1]
			}
		}
	}

	// handle cluster by keys
	if stmt.ClusterByOption != nil {
		if stmt.Temporary {
			return moerr.NewNotSupported(ctx.GetContext(), "cluster by with temporary table is not support")
		}
		if len(primaryKeys) > 0 {
			return moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
		}
		lenClusterBy := len(stmt.ClusterByOption.ColumnList)
		var clusterByKeys []string
		for i := 0; i < lenClusterBy; i++ {
			colName := stmt.ClusterByOption.ColumnList[i].ColName()
			if _, ok := colMap[colName]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' doesn't exist in table", stmt.ClusterByOption.ColumnList[i].ColNameOrigin())
			}
			clusterByKeys = append(clusterByKeys, colName)
		}

		if lenClusterBy == 1 {
			clusterByColName := clusterByKeys[0]
			for _, col := range createTable.TableDef.Cols {
				if col.Name == clusterByColName {
					col.ClusterBy = true
				}
			}

			createTable.TableDef.ClusterBy = &plan.ClusterByDef{
				Name: clusterByColName,
			}
		} else {
			clusterByColName := util.BuildCompositeClusterByColumnName(clusterByKeys)
			colDef := MakeHiddenColDefByName(clusterByColName)
			colDef.Default.NullAbility = true
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[clusterByColName] = colDef

			createTable.TableDef.ClusterBy = &plan.ClusterByDef{
				Name:         clusterByColName,
				CompCbkeyCol: colDef,
			}
		}
	}

	// check Constraint Name (include index/ unique)
	err = checkConstraintNames(uniqueIndexInfos, secondaryIndexInfos, ctx.GetContext())
	if err != nil {
		return err
	}

	// build index table
	if len(uniqueIndexInfos) != 0 {
		err = buildUniqueIndexTable(createTable, uniqueIndexInfos, colMap, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	if len(fullTextIndexInfos) != 0 {
		err = buildFullTextIndexTable(createTable, fullTextIndexInfos, colMap, nil, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	if len(secondaryIndexInfos) != 0 {
		err = buildSecondaryIndexDef(createTable, secondaryIndexInfos, colMap, nil, pkeyName, ctx)
		if err != nil {
			return err
		}
	}

	// process self reference foreign keys after colDefs and indexes are processed.
	if len(fkDatasOfFKSelfRefer) > 0 {
		// for fk self refer. the column id of the tableDef is not ready.
		// setup fake column id to distinguish the columns
		for i, def := range createTable.TableDef.Cols {
			def.ColId = uint64(i)
		}
		for _, selfRefer := range fkDatasOfFKSelfRefer {
			if err := checkFkColsAreValid(ctx, selfRefer, createTable.TableDef); err != nil {
				return err
			}
			selfRefer.UpdateSql = getSqlForAddFkWithCatalogLayout(
				createTable.Database, createTable.TableDef.Name, selfRefer, selfRefer.catalogLayout)
			createTable.UpdateFkSqls = append(createTable.UpdateFkSqls, selfRefer.UpdateSql)
		}
	}

	skip := IsFkBannedDatabase(createTable.Database)
	if !skip {
		// Existing relations are handled by the execution-time RelationExists
		// check. Their reverse foreign keys belong to the existing definition and
		// must never be validated against the ignored replacement definition.
		_, existingTableDef, err := ctx.Resolve(
			createTable.Database, createTable.TableDef.Name, nil,
		)
		if err != nil {
			return err
		}
		if existingTableDef == nil {
			fks, catalogLayout, err := getFkReferredToWithCatalogLayout(ctx, createTable.Database, createTable.TableDef.Name)
			if err != nil {
				return err
			}
			// for fk forward reference. the column id of the tableDef is not ready.
			// setup fake column id to distinguish the columns
			for i, def := range createTable.TableDef.Cols {
				def.ColId = uint64(i)
			}
			for rkey, fkDefs := range fks {
				for constraintName, defs := range fkDefs {
					data, err := buildFkDataOfForwardRefer(ctx, constraintName, defs, createTable)
					if err != nil {
						return err
					}
					// The child was created while foreign_key_checks was disabled, so
					// its catalog row has no parent key name. Persist the selected key
					// when the metadata column exists; an old-layout row is reconciled
					// by the tenant migration after the columns are committed.
					if catalogLayout == foreignKeyCatalogExtended {
						createTable.UpdateFkSqls = append(createTable.UpdateFkSqls,
							getSqlForUpdateFkReferencedIndex(rkey.Db, rkey.Tbl, constraintName, data.Def.ReferencedIndexName))
					}
					info := &plan.ForeignKeyInfo{
						Db:           rkey.Db,
						Table:        rkey.Tbl,
						ColsReferred: data.ColsReferred,
						Def:          data.Def,
					}
					createTable.FksReferToMe = append(createTable.FksReferToMe, info)
				}
			}
		}
	}

	return nil
}

func appendCheckDef(
	ctx CompilerContext,
	tableDef *TableDef,
	name string,
	astExpr tree.Expr,
	columnPos int,
) error {
	if err := requireCheckConstraintProtocol(ctx.GetContext(), ctx.GetProcess()); err != nil {
		return err
	}
	colNames := make([]string, 0, len(tableDef.Cols))
	colTypes := make([]plan.Type, 0, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		colNames = append(colNames, col.Name)
		colTypes = append(colTypes, col.Typ)
	}

	originSQL := formatCheckConstraintExpr(astExpr)
	canonicalStmt, err := parsers.ParseOne(
		ctx.GetContext(),
		dialect.MYSQL,
		"select "+originSQL,
		1,
	)
	if err != nil {
		return err
	}
	defer canonicalStmt.Free()
	canonicalSelect, ok := canonicalStmt.(*tree.Select)
	if !ok {
		return moerr.NewInternalError(ctx.GetContext(), "invalid canonical check constraint")
	}
	canonicalClause, ok := canonicalSelect.Select.(*tree.SelectClause)
	if !ok || len(canonicalClause.Exprs) != 1 {
		return moerr.NewInternalError(ctx.GetContext(), "invalid canonical check constraint expression")
	}

	binder := NewGeneratedColBinder(ctx.GetContext(), colNames, colTypes)
	binder.enableCanonicalNameConstValueCast()
	checkExpr, err := binder.BindExpr(canonicalClause.Exprs[0].Expr, 0, true)
	if err != nil {
		return err
	}
	if err = validateCheckExpr(ctx.GetContext(), tableDef, checkExpr, columnPos); err != nil {
		return err
	}
	if checkExpr.Typ.Id != int32(types.T_bool) {
		checkExpr, err = makePlan2CastExpr(
			ctx.GetContext(),
			checkExpr,
			plan.Type{Id: int32(types.T_bool)},
		)
		if err != nil {
			return err
		}
	}
	if name == "" {
		name = fmt.Sprintf("__mo_chk_%d", len(tableDef.Checks)+1)
	}
	for _, check := range tableDef.Checks {
		if check.Name == name {
			return moerr.NewInvalidInputf(ctx.GetContext(), "duplicate check constraint name '%s'", name)
		}
	}
	tableDef.Checks = append(tableDef.Checks, &plan.CheckDef{
		Name:      name,
		Check:     checkExpr,
		OriginSql: originSQL,
	})
	return nil
}

func formatCheckConstraintExpr(expr tree.Expr) string {
	opts := []tree.FmtCtxOption{
		tree.WithSingleQuoteString(),
		tree.WithQuoteIdentifier(),
		tree.WithModeIndependentStringLiterals(),
	}
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, opts...)
	expr.Format(fmtCtx)
	return fmtCtx.String()
}

func validateCheckExpr(ctx context.Context, tableDef *TableDef, expr *plan.Expr, columnPos int) error {
	if expr == nil {
		return moerr.NewInvalidInput(ctx, "check constraint expression cannot be empty")
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		pos := int(e.Col.ColPos)
		if pos < 0 || pos >= len(tableDef.Cols) {
			return moerr.NewInvalidInput(ctx, "check constraint references an invalid column")
		}
		if columnPos >= 0 && pos != columnPos {
			return moerr.NewInvalidInputf(
				ctx,
				"column check constraint cannot refer to column '%s'",
				tableDef.Cols[pos].OriginName,
			)
		}
		if tableDef.Cols[pos].Typ.AutoIncr {
			return moerr.NewInvalidInputf(
				ctx,
				"check constraint cannot refer to auto-increment column '%s'",
				tableDef.Cols[pos].OriginName,
			)
		}
	case *plan.Expr_V:
		return moerr.NewInvalidInput(ctx, "check constraint cannot refer to a variable")
	case *plan.Expr_P:
		return moerr.NewInvalidInput(ctx, "check constraint cannot contain a parameter marker")
	case *plan.Expr_F:
		switch strings.ToLower(e.F.Func.ObjName) {
		case "connection_id",
			"current_account_id",
			"current_account_name",
			"current_role",
			"current_role_id",
			"current_role_name",
			"current_user",
			"current_user_id",
			"current_user_name",
			"database",
			"found_rows",
			"last_insert_id",
			"row_count",
			"session_user",
			"system_user",
			"user":
			return moerr.NewInvalidInputf(
				ctx,
				"check constraint cannot contain session-dependent function '%s'",
				e.F.Func.ObjName,
			)
		}
		if err := checkExprForVolatileFunc(ctx, expr); err != nil {
			return moerr.NewInvalidInputf(
				ctx,
				"check constraint cannot contain a non-deterministic function",
			)
		}
		for _, arg := range e.F.Args {
			if err := validateCheckExpr(ctx, tableDef, arg, columnPos); err != nil {
				return err
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if err := validateCheckExpr(ctx, tableDef, item, columnPos); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreIntervalSyntaxForCTAS(sql string) string {
	var out strings.Builder
	for i := 0; i < len(sql); {
		if sql[i] == '\'' || sql[i] == '"' {
			next := skipQuotedStringForCTAS(sql, i, sql[i])
			out.WriteString(sql[i:next])
			i = next
			continue
		}
		if sql[i] == '`' {
			next := skipBacktickIdentifierForCTAS(sql, i)
			out.WriteString(sql[i:next])
			i = next
			continue
		}
		if !hasIntervalKeywordAt(sql, i) {
			out.WriteByte(sql[i])
			i++
			continue
		}

		expr, unit, next, ok := parseIntervalCall(sql, i)
		if !ok || !isIntervalUnitToken(unit) {
			out.WriteByte(sql[i])
			i++
			continue
		}

		out.WriteString("interval ")
		out.WriteString(strings.TrimSpace(expr))
		out.WriteByte(' ')
		out.WriteString(strings.TrimSpace(unit))
		i = next
	}
	return out.String()
}

func parseIntervalCall(sql string, start int) (expr string, unit string, next int, ok bool) {
	const prefix = "interval("
	pos := start + len(prefix)
	depth := 1
	comma := -1

	for pos < len(sql) {
		ch := sql[pos]
		if ch == '\'' || ch == '"' {
			pos = skipQuotedStringForCTAS(sql, pos, ch)
			continue
		}
		if ch == '`' {
			pos = skipBacktickIdentifierForCTAS(sql, pos)
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if comma == -1 {
					return "", "", 0, false
				}
				return sql[start+len(prefix) : comma], sql[comma+1 : pos], pos + 1, true
			}
		case ',':
			if depth == 1 && comma == -1 {
				comma = pos
			}
		}
		pos++
	}
	return "", "", 0, false
}

func hasIntervalKeywordAt(sql string, start int) bool {
	const keyword = "interval"
	end := start + len(keyword)
	if end >= len(sql) || sql[end] != '(' || !strings.EqualFold(sql[start:end], keyword) {
		return false
	}
	return start == 0 || !isSQLIdentifierByte(sql[start-1])
}

func isSQLIdentifierByte(ch byte) bool {
	return ch >= 0x80 || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' ||
		ch >= '0' && ch <= '9' || ch == '_' || ch == '$'
}

func skipQuotedStringForCTAS(sql string, start int, quote byte) int {
	for pos := start + 1; pos < len(sql); pos++ {
		if sql[pos] == '\\' {
			if pos+1 < len(sql) {
				pos++
			}
			continue
		}
		if sql[pos] != quote {
			continue
		}
		if pos+1 < len(sql) && sql[pos+1] == quote {
			pos++
			continue
		}
		return pos + 1
	}
	return len(sql)
}

func skipBacktickIdentifierForCTAS(sql string, start int) int {
	for pos := start + 1; pos < len(sql); pos++ {
		if sql[pos] != '`' {
			continue
		}
		if pos+1 < len(sql) && sql[pos+1] == '`' {
			pos++
			continue
		}
		return pos + 1
	}
	return len(sql)
}

func isIntervalUnitToken(unit string) bool {
	switch strings.ToLower(strings.Trim(strings.TrimSpace(unit), "`'\"")) {
	case "microsecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
		"second_microsecond", "minute_microsecond", "minute_second", "hour_microsecond",
		"hour_second", "hour_minute", "day_microsecond", "day_second", "day_minute",
		"day_hour", "year_month":
		return true
	default:
		return false
	}
}

func getRefAction(typ tree.ReferenceOptionType) plan.ForeignKeyDef_RefAction {
	switch typ {
	case tree.REFERENCE_OPTION_CASCADE:
		return plan.ForeignKeyDef_CASCADE
	case tree.REFERENCE_OPTION_NO_ACTION:
		return plan.ForeignKeyDef_NO_ACTION
	case tree.REFERENCE_OPTION_RESTRICT:
		return plan.ForeignKeyDef_RESTRICT
	case tree.REFERENCE_OPTION_SET_NULL:
		return plan.ForeignKeyDef_SET_NULL
	case tree.REFERENCE_OPTION_SET_DEFAULT:
		return plan.ForeignKeyDef_SET_DEFAULT
	default:
		return plan.ForeignKeyDef_RESTRICT
	}
}

// buildFullTextIndexTable routes each fulltext index through the
// fulltext plugin's plan.BuildFullTextIndexDefs hook (lifted body
// lives at pkg/fulltext/plugin/plan/schema.go). It keeps the
// batched, in-place-append signature the legacy callers used.
func buildFullTextIndexTable(createTable *plan.CreateTable, indexInfos []*tree.FullTextIndex, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string, ctx CompilerContext) error {
	if err := checkFulltextEngineConflict(indexInfos, existedIndexes, ctx); err != nil {
		return err
	}
	for _, indexInfo := range indexInfos {
		// CREATE FULLTEXT2 INDEX (IsV2) routes to the distinct fulltext2 plugin;
		// classic CREATE FULLTEXT INDEX to the fulltext plugin.
		algo := catalog.MOIndexFullTextAlgo.ToString()
		if indexInfo.IsV2 {
			algo = catalog.MoIndexFullText2Algo.ToString()
		}
		p, ok := indexplugin.Get(algo)
		if !ok {
			return moerr.NewInternalErrorNoCtx(algo + " plugin not registered")
		}
		idxDefs, tblDefs, err := p.Plan().BuildFullTextIndexDefs(
			ctx, indexInfo, colMap, existedIndexes, pkeyName,
		)
		if err != nil {
			return err
		}
		// Capture the plugin's build-time session vars (BuildSessionVars) into each
		// index def's algo_params.session_vars — mirroring CreateIndexDef's vector
		// path — so background builds (idxcron reindex, ISCP async, clone/restore)
		// reproduce the create-time config. fulltext2 pins lower_case_table_names;
		// classic fulltext's BuildSessionVars returns nil, so this is a no-op there.
		if ctx != nil {
			if names := p.Catalog().BuildSessionVars(); len(names) > 0 {
				sv, cerr := compileplugin.CaptureVars(ctx.ResolveVariable, names)
				if cerr != nil {
					return cerr
				}
				if len(sv) > 0 {
					for _, idxDef := range idxDefs {
						flat := map[string]string{}
						if idxDef.IndexAlgoParams != "" {
							if flat, err = catalog.IndexParamsStringToMap(idxDef.IndexAlgoParams); err != nil {
								return err
							}
						}
						if idxDef.IndexAlgoParams, err = catalog.IndexParamsMapToJsonStringWithSessionVars(flat, sv); err != nil {
							return err
						}
					}
				}
			}
		}
		setIndexDefsVisibility(idxDefs, indexInfo.IndexOption)
		createTable.IndexTables = append(createTable.IndexTables, tblDefs...)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, idxDefs...)
	}
	return nil
}

// ftColSetKey is an order-independent key for a fulltext index's column set, so
// MATCH(a,b) and MATCH(b,a) map to the same key.
func ftColSetKey(cols []string) string {
	c := append([]string(nil), cols...)
	slices.Sort(c)
	return strings.Join(c, "\x00")
}

// checkFulltextEngineConflict rejects creating a classic FULLTEXT and a FULLTEXT2
// index over the SAME column set — on the same table they would both resolve the same
// MATCH(...) verb, so the engine (classic SQL fulltext vs WAND positional) that answers
// the query would depend on index enumeration order and could flip across DDL/catalog
// reloads, giving inconsistent scores/rows. This covers CREATE TABLE (all indexes in
// indexInfos, existedIndexes nil) and CREATE INDEX / ALTER ADD (one new index vs the
// table's existing indexes) since every fulltext creation routes through here.
func checkFulltextEngineConflict(indexInfos []*tree.FullTextIndex, existedIndexes []*plan.IndexDef, ctx CompilerContext) error {
	// colSet -> the fulltext engine (isV2) already claiming it (existing indexes).
	engine := make(map[string]bool)
	for _, idx := range existedIndexes {
		v2 := catalog.IsFullText2IndexAlgo(idx.IndexAlgo)
		if !v2 && !catalog.IsFullTextIndexAlgo(idx.IndexAlgo) {
			continue
		}
		engine[ftColSetKey(idx.Parts)] = v2
	}
	for _, info := range indexInfos {
		cols := make([]string, 0, len(info.KeyParts))
		for _, kp := range info.KeyParts {
			cols = append(cols, kp.ColName.ColName())
		}
		key := ftColSetKey(cols)
		if prev, ok := engine[key]; ok && prev != info.IsV2 {
			return moerr.NewInvalidInput(ctx.GetContext(),
				"cannot create both a FULLTEXT and a FULLTEXT2 index on the same column(s); drop the existing one first")
		}
		engine[key] = info.IsV2 // also catches two conflicting indexes in one CREATE TABLE
	}
	return nil
}

func buildUniqueIndexTable(createTable *plan.CreateTable, indexInfos []*tree.UniqueIndex, colMap map[string]*ColDef, pkeyName string, ctx CompilerContext) error {
	for _, indexInfo := range indexInfos {
		indexDef := &plan.IndexDef{}
		indexDef.Unique = true

		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), true)

		if err != nil {
			return err
		}
		tableDef := &TableDef{
			Name: indexTableName,
		}
		indexParts := make([]string, 0)

		for _, keyPart := range indexInfo.KeyParts {
			nameOrigin := keyPart.ColName.ColNameOrigin()
			name := keyPart.ColName.ColName()
			if _, ok := colMap[name]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
			}
			if err := checkIndexColumnSupportability(ctx.GetContext(), colMap[name], keyPart, "unique"); err != nil {
				return err
			}

			indexParts = append(indexParts, name)
		}

		var keyName string
		if len(indexInfo.KeyParts) == 1 {
			keyName = catalog.IndexTableIndexColName
			keyPart := indexInfo.KeyParts[0]
			colName := keyPart.ColName.ColName()
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ:  indexTableKeyTypeForSinglePart(colMap[colName], keyPart),
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{keyName},
				PkeyColName: keyName,
			}
		} else {
			keyName = catalog.IndexTableIndexColName
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ:  makeHiddenColTyp(),
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{keyName},
				PkeyColName: keyName,
			}
		}
		if pkeyName != "" {
			colDef := &ColDef{
				Name: catalog.IndexTablePrimaryColName,
				Alg:  plan.CompressType_Lz4,
				Typ: plan.Type{
					// don't copy auto increment
					Id:      colMap[pkeyName].Typ.Id,
					Width:   colMap[pkeyName].Typ.Width,
					Scale:   colMap[pkeyName].Typ.Scale,
					Charset: colMap[pkeyName].Typ.Charset,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemIndexRel,
			},
		}
		tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})

		// indexDef.IndexName = indexInfo.Name
		indexDef.IndexName = indexInfo.GetIndexName()
		indexDef.IndexTableName = indexTableName
		indexDef.Parts = indexParts
		indexDef.TableExist = true
		setIndexDefVisibility(indexDef, indexInfo.IndexOption)
		if indexInfo.IndexOption != nil {
			indexDef.Comment = indexInfo.IndexOption.Comment
		} else {
			indexDef.Comment = ""
		}
		indexDef.IndexAlgoParams, err = addIndexPrefixLengthsToParams(ctx, indexDef.IndexAlgoParams, indexInfo.KeyParts)
		if err != nil {
			return err
		}
		createTable.IndexTables = append(createTable.IndexTables, tableDef)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, indexDef)
	}
	return nil
}

// buildIndexAlgoParams converts the parsed CREATE INDEX options into the
// algo_params JSON. Per-algo parameter rules live in each index plugin's
// Catalog().ParamsFromTree hook; non-plugin algorithms (btree/rtree/master)
// fall through to catalog (which produces no algo_params for them).
func buildIndexAlgoParams(indexInfo *tree.Index) (string, error) {
	if p, ok := indexplugin.Get(indexInfo.KeyType.ToString()); ok {
		res, err := p.Catalog().ParamsFromTree(indexInfo)
		if err != nil {
			return "", err
		}
		if len(res) == 0 {
			return "", nil
		}
		return catalog.IndexParamsMapToJsonString(res)
	}
	return catalog.IndexParamsToJsonString(indexInfo)
}

func addIndexPrefixLengthsToParams(ctx CompilerContext, indexParams string, keyParts []*tree.KeyPart) (string, error) {
	for _, keyPart := range keyParts {
		if keyPart == nil || keyPart.ColName == nil || keyPart.Length <= 0 {
			continue
		}
		if err := requirePrefixIndexV2Protocol(
			ctx.GetContext(), ctx.GetProcess(), keyPart.ColName.ColName(),
		); err != nil {
			return "", err
		}
	}
	return catalog.AddIndexPrefixLengthsToParams(indexParams, keyParts)
}

func buildSecondaryIndexDef(createTable *plan.CreateTable, indexInfos []*tree.Index, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string, ctx CompilerContext) (err error) {
	if len(pkeyName) == 0 {
		return moerr.NewInternalErrorNoCtx("primary key cannot be empty for secondary index")
	}

	for _, indexInfo := range indexInfos {
		err = checkIndexKeypartSupportability(ctx.GetContext(), indexInfo.KeyParts)
		if err != nil {
			return err
		}
		if err = checkSpatialIndexColumnSupport(ctx, indexInfo, colMap); err != nil {
			return err
		}

		var indexDef []*plan.IndexDef
		var tableDef []*TableDef
		switch indexInfo.KeyType {
		case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID, tree.INDEX_TYPE_RTREE:
			indexDef, tableDef, err = buildRegularSecondaryIndexDef(ctx, indexInfo, colMap, pkeyName)
		case tree.INDEX_TYPE_MASTER:
			indexDef, tableDef, err = buildMasterSecondaryIndexDef(ctx, indexInfo, colMap, pkeyName)
		default:
			// Vector-index algorithms live in pkg/vectorindex/<algo>/plugin/plan
			// (BuildSecondaryIndexDefs). Any KeyType registered with the
			// plugin registry is supported; anything else is rejected.
			if p, ok := indexplugin.Get(indexInfo.KeyType.ToString()); ok {
				indexDef, tableDef, err = p.Plan().BuildSecondaryIndexDefs(ctx, indexInfo, colMap, existedIndexes, pkeyName)
			} else {
				return moerr.NewInvalidInputNoCtxf("unsupported index type: %s", indexInfo.KeyType.ToString())
			}
		}

		if err != nil {
			return err
		}
		setIndexDefsVisibility(indexDef, indexInfo.IndexOption)
		createTable.IndexTables = append(createTable.IndexTables, tableDef...)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, indexDef...)

	}
	return nil
}

func setIndexDefsVisibility(indexDefs []*plan.IndexDef, option *tree.IndexOption) {
	for _, indexDef := range indexDefs {
		setIndexDefVisibility(indexDef, option)
	}
}

func setIndexDefVisibility(indexDef *plan.IndexDef, option *tree.IndexOption) {
	catalog.SetIndexVisibility(indexDef, option == nil || option.Visible != tree.VISIBLE_TYPE_INVISIBLE)
}

func checkSpatialIndexColumnSupport(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef) error {
	if indexInfo.KeyType != tree.INDEX_TYPE_RTREE {
		return nil
	}
	if len(indexInfo.KeyParts) != 1 {
		return moerr.NewNotSupported(ctx.GetContext(), "SPATIAL INDEX only supports a single GEOMETRY column")
	}

	name := indexInfo.KeyParts[0].ColName.ColName()
	nameOrigin := indexInfo.KeyParts[0].ColName.ColNameOrigin()
	col, ok := colMap[name]
	if !ok {
		return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
	}
	if !isGeometryPlanType(&col.Typ) {
		return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("SPATIAL INDEX can only be created on GEOMETRY column '%s'", nameOrigin))
	}
	return nil
}

// buildMasterSecondaryIndexDef will create hidden internal table with schema.
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col varchar,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
func buildMasterSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {
	// 1. indexDef init
	indexDef := &plan.IndexDef{}
	indexDef.Unique = false

	// 2. tableDef init
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	tableDef := &TableDef{
		Name: indexTableName,
	}

	nameCount := make(map[string]int)
	// Note: Index Parts will store the ColName, as Parts is used to populate mo_index_table.
	// However, when inserting Index, we convert Parts (ie ColName) to ColIdx.
	indexParts := make([]string, 0)

	for _, keyPart := range indexInfo.KeyParts {
		nameOrigin := keyPart.ColName.ColNameOrigin()
		name := keyPart.ColName.ColName()
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
		}
		if colMap[name].Typ.Id != int32(types.T_varchar) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("column '%s' is not varchar type.", nameOrigin))
		}
		indexParts = append(indexParts, name)
	}

	var keyName = catalog.MasterIndexTableIndexColName
	colDef := &ColDef{
		Name: keyName,
		Alg:  plan.CompressType_Lz4,
		Typ:  makeHiddenColTyp(),
		Default: &plan.Default{
			NullAbility:  false,
			Expr:         nil,
			OriginString: "",
		},
	}
	tableDef.Cols = append(tableDef.Cols, colDef)
	tableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{keyName},
		PkeyColName: keyName,
	}
	if pkeyName != "" {
		pkColDef := &ColDef{
			Name: catalog.MasterIndexTablePrimaryColName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:      colMap[pkeyName].Typ.Id,
				Width:   colMap[pkeyName].Typ.Width,
				Scale:   colMap[pkeyName].Typ.Scale,
				Charset: colMap[pkeyName].Typ.Charset,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, pkColDef)
	}

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemIndexRel,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		}})

	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts
	indexDef.TableExist = true
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = ""

	if indexInfo.IndexOption != nil {
		indexDef.Comment = indexInfo.IndexOption.Comment

		params, err := buildIndexAlgoParams(indexInfo)
		if err != nil {
			return nil, nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
	}
	indexDef.IndexAlgoParams, err = addIndexPrefixLengthsToParams(ctx, indexDef.IndexAlgoParams, indexInfo.KeyParts)
	if err != nil {
		return nil, nil, err
	}
	return []*plan.IndexDef{indexDef}, []*TableDef{tableDef}, nil
}

// buildRegularSecondingIndexDef will create a hidden index table with schema
//
// when number of primary key == 1
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col src_pk_type,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
//
// when number of primary key > 1
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col varchar,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
func buildRegularSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {

	// 1. indexDef init
	indexDef := &plan.IndexDef{}
	indexDef.Unique = false
	spatialIndex := indexInfo.KeyType == tree.INDEX_TYPE_RTREE

	// 2. tableDef init
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	tableDef := &TableDef{
		Name: indexTableName,
	}

	nameCount := make(map[string]int)
	indexParts := make([]string, 0)

	isPkAlreadyPresentInIndexParts := false
	for _, keyPart := range indexInfo.KeyParts {
		name := keyPart.ColName.ColName()
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", keyPart.ColName.ColNameOrigin())
		}
		indexKind := "secondary"
		if indexInfo.KeyType == tree.INDEX_TYPE_RTREE {
			indexKind = "rtree"
		}
		if err := checkIndexColumnSupportability(ctx.GetContext(), colMap[name], keyPart, indexKind); err != nil {
			return nil, nil, err
		}

		if strings.Compare(name, pkeyName) == 0 || catalog.IsAlias(name) {
			isPkAlreadyPresentInIndexParts = true
		}
		indexParts = append(indexParts, name)
	}

	if !isPkAlreadyPresentInIndexParts {
		indexParts = append(indexParts, catalog.CreateAlias(pkeyName))
	}

	var keyName string
	if len(indexParts) == 1 {
		// This means indexParts only contains the primary key column
		keyName = catalog.IndexTableIndexColName
		colDef := &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:      colMap[pkeyName].Typ.Id,
				Width:   colMap[pkeyName].Typ.Width,
				Scale:   colMap[pkeyName].Typ.Scale,
				Charset: colMap[pkeyName].Typ.Charset,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		tableDef.Pkey = &PrimaryKeyDef{
			Names:       []string{keyName},
			PkeyColName: keyName,
		}
	} else {
		keyName = catalog.IndexTableIndexColName
		idxColType := makeHiddenColTyp()
		if spatialIndex {
			idxColType = colMap[indexParts[0]].Typ
		}
		colDef := &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ:  idxColType,
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		tableDef.Pkey = &PrimaryKeyDef{
			Names:       []string{keyName},
			PkeyColName: keyName,
		}
	}
	if pkeyName != "" {
		colDef := &ColDef{
			Name: catalog.IndexTablePrimaryColName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:      colMap[pkeyName].Typ.Id,
				Width:   colMap[pkeyName].Typ.Width,
				Scale:   colMap[pkeyName].Typ.Scale,
				Charset: colMap[pkeyName].Typ.Charset,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		if spatialIndex {
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{catalog.IndexTablePrimaryColName},
				PkeyColName: catalog.IndexTablePrimaryColName,
			}
		}
	}

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemIndexRel,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		}})

	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts
	indexDef.TableExist = true
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = ""

	if indexInfo.IndexOption != nil {
		indexDef.Comment = indexInfo.IndexOption.Comment

		params, err := buildIndexAlgoParams(indexInfo)
		if err != nil {
			return nil, nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
	}
	indexDef.IndexAlgoParams, err = addIndexPrefixLengthsToParams(ctx, indexDef.IndexAlgoParams, indexInfo.KeyParts)
	if err != nil {
		return nil, nil, err
	}
	return []*plan.IndexDef{indexDef}, []*TableDef{tableDef}, nil
}

// buildIvfFlatSecondIndexDef create three internal tables
//
// with the following schemas,
//
// create __mo_secondary_metadata (
//	__mo_index_key varchar,
//	__mo_index_val varhcar,
// 	primary key __mo_index_key,
// )
//
// create __mo_secondary_centroids (
//	__mo_index_centroid_version bigint,
//	__mo_index_centroid_id bigint,
//	__mo_index_centroid vecf32 or vecf64,
//	primary key (__mo_index_centroid_version, __mo_index_centroid_id),
// )
// create __mo_seconary_entries (
//	__mo_index_centroid_fk_version bigint,
//	__mo_index_centroid_fk_id bigint,
//	__mo_index_pri_col src_pk_type,
//	__mo_index_centroid_fk_entry vecf32 or vecf64,
//	primary key (__mo_index_centriod_fk_version, __mo_index_centroid_fk_id, __mo_index_pri_col)
// )

// validateIncludeColumns enforces DDL-time rules for INCLUDE columns on GPU
// vector (CAGRA / IVF-PQ) indexes. The execute-time path in
// filter_helper_gpu.go validates types lazily, so without this check a bogus
// CREATE INDEX ... INCLUDE (...) only breaks at the first INSERT. Failing up
// front gives users a clear, immediate error.
//
// Rules:
//   - each INCLUDE column must exist on the base table
//   - column type must be one the GPU FilterStore accepts: int32, int64,
//     float32, float64. VARCHAR is NOT accepted — the executor path expects a
//     pre-hashed uint64 and the DDL-side hashing pipeline is not wired in
//     yet, so reject it here until that support lands.
//   - INCLUDE columns must not duplicate each other or the indexed vector
//     column.
//   - INCLUDE must not contain the primary key column. PK predicates are
//     pushed down automatically via the reserved __mo_pk_host_id virtual
//     column (pkg/sql/plan/filter_predicate.go), which evaluates against the
//     index's host_ids array. Listing the PK as an INCLUDE column would
//     duplicate the PK values in filter_host_ for no benefit — the planner
//     would route the predicate to host_ids anyway.
func validateIncludeColumns(ctx CompilerContext,
	includeCols []*tree.UnresolvedName,
	colMap map[string]*ColDef,
	vecColName string,
	pkeyName string,
	supportedTypes []types.T) error {
	if len(includeCols) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(includeCols))
	for _, uc := range includeCols {
		name := uc.ColName()
		origin := uc.ColNameOrigin()
		if name == "" {
			return moerr.NewInvalidInputf(ctx.GetContext(), "INCLUDE column name cannot be empty")
		}
		if name == vecColName {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"INCLUDE column '%s' cannot be the indexed vector column", origin)
		}
		if pkeyName != "" && name == pkeyName {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"INCLUDE column '%s' must not be the primary key; "+
					"predicates on the pk are pushed down automatically via the "+
					"__mo_pk_host_id virtual column, so listing it here only "+
					"duplicates storage", origin)
		}
		if _, dup := seen[name]; dup {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"duplicate INCLUDE column '%s'", origin)
		}
		seen[name] = struct{}{}

		col, ok := colMap[name]
		if !ok {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"INCLUDE column '%s' is not exist", origin)
		}
		// Supported INCLUDE column types are declared by the index plugin
		// (catalog.Hooks.SupportedIncludeColumnTypes()) and threaded in via
		// supportedTypes — the single source of truth, not hardcoded here.
		colType := types.T(col.Typ.Id)
		supported := false
		for _, st := range supportedTypes {
			if colType == st {
				supported = true
				break
			}
		}
		if !supported {
			names := make([]string, 0, len(supportedTypes))
			for _, st := range supportedTypes {
				names = append(names, st.String())
			}
			return moerr.NewNotSupportedf(ctx.GetContext(),
				"INCLUDE column '%s' has unsupported type %s (supported: %s)",
				origin, colType.String(), strings.Join(names, ", "))
		}
	}
	return nil
}

func getVectorIndexIncludeColumnNames(indexInfo *tree.Index) []string {
	if indexInfo == nil || indexInfo.IndexOption == nil || len(indexInfo.IndexOption.IncludeColumns) == 0 {
		return nil
	}

	names := make([]string, 0, len(indexInfo.IndexOption.IncludeColumns))
	for _, col := range indexInfo.IndexOption.IncludeColumns {
		names = append(names, col.ColName())
	}
	return names
}

func CreateIndexDef(ctx planplugin.CompilerContext, indexInfo *tree.Index,
	indexTableName, indexAlgoTableType string,
	indexParts []string, isUnique bool) (*plan.IndexDef, error) {

	// TODO: later use this function for RegularSecondaryIndex and UniqueIndex.

	indexDef := &plan.IndexDef{}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts
	indexDef.IncludedColumns = getVectorIndexIncludeColumnNames(indexInfo)

	indexDef.Unique = isUnique
	indexDef.TableExist = true
	setIndexDefVisibility(indexDef, indexInfo.IndexOption)

	// Algorithm related fields
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = indexAlgoTableType
	if indexInfo.IndexOption != nil {
		// Copy Comment as it is
		indexDef.Comment = indexInfo.IndexOption.Comment

		// Create params JSON string and set it
		params, err := buildIndexAlgoParams(indexInfo)
		if err != nil {
			return nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		// default indexInfo.IndexOption values
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
		if p, ok := indexplugin.Get(indexInfo.KeyType.ToString()); ok {
			// Vector-index algorithms supply their default params via the
			// plugin (DefaultOptions). Non-vector algos miss the registry
			// and leave the empty defaults set above.
			if defaults := p.Catalog().DefaultOptions(); len(defaults) > 0 {
				params, err := catalog.IndexParamsMapToJsonString(defaults)
				if err != nil {
					return nil, err
				}
				indexDef.IndexAlgoParams = params
			}
		}

	}

	// Capture the plugin's build-time session vars (BuildSessionVars) into the
	// typed algo_params.session_vars blob so background builds (restore reindex,
	// idxcron, async create) reproduce the create-time config. Index-defining
	// knobs (kmeans_*, max_index_capacity) are NOT auto-captured here: they ride
	// flat algo_params keys written by ParamsFromTree only when explicitly set
	// in CREATE INDEX (so an unset option never pollutes algo_params).
	if ctx != nil {
		if p, ok := indexplugin.Get(catalog.ToLower(indexDef.IndexAlgo)); ok {
			if names := p.Catalog().BuildSessionVars(); len(names) > 0 {
				sv, err := compileplugin.CaptureVars(ctx.ResolveVariable, names)
				if err != nil {
					return nil, err
				}
				if len(sv) > 0 {
					flat := map[string]string{}
					if indexDef.IndexAlgoParams != "" {
						if flat, err = catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams); err != nil {
							return nil, err
						}
					}
					if indexDef.IndexAlgoParams, err = catalog.IndexParamsMapToJsonStringWithSessionVars(flat, sv); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	nameCount := make(map[string]int)
	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	return indexDef, nil
}

func buildTruncateTable(stmt *tree.TruncateTable, ctx CompilerContext) (*Plan, error) {
	truncateTable := &plan.TruncateTable{}

	truncateTable.Database = string(stmt.Name.SchemaName)
	if truncateTable.Database == "" {
		truncateTable.Database = ctx.DefaultDatabase()
	}
	truncateTable.Table = string(stmt.Name.ObjectName)
	obj, tableDef, err := ctx.Resolve(truncateTable.Database, truncateTable.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
	} else {
		if err := validateTableIndexDefinitions(tableDef); err != nil {
			return nil, err
		}
		// Temporary tables shadow same-named permanent tables, but TRUNCATE is
		// not supported for temporary tables. Reject the visible temporary table
		// here so execution can never fall through to the hidden permanent table.
		if tableDef.GetIsTemporary() {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
		}

		if tableDef.TableType == catalog.SystemSourceRel {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate source '%v' ", truncateTable.Table)
		}

		// TRUNCATE has historically been a silent no-op for generic read-only
		// external tables. MongoDB mappings, however, have an explicit read-only
		// DML contract, so fail closed with the same stable error as other direct
		// mutations. Keep the existing behavior for other generic mappings.
		if tableDef.TableType == catalog.SystemExternalRel {
			isMongoDB, err := IsMongoDBTableDef(ctx.GetContext(), tableDef)
			if err != nil {
				return nil, err
			}
			if isMongoDB {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot insert/update/delete from external table")
			}
			isIceberg, err := IsIcebergTableDef(ctx.GetContext(), tableDef)
			if err != nil {
				return nil, err
			}
			if isIceberg {
				return nil, moerr.NewNotSupportedf(ctx.GetContext(), "truncate Iceberg table mapping '%v'", truncateTable.Table)
			}
			if _, ok := GetWriteFilePattern(getExternParamFromTableDef(tableDef)); ok {
				return nil, moerr.NewNotSupportedf(ctx.GetContext(),
					"truncate writable external table '%v'; its files in the stage are not managed by the table", truncateTable.Table)
			}
		}

		if len(tableDef.RefChildTbls) > 0 {
			// if all children tables are self reference, we can drop the table
			if !HasFkSelfReferOnly(tableDef) {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate table '%v' referenced by some foreign key constraint", truncateTable.Table)
			}
		}

		if tableDef.ViewSql != nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
		}

		truncateTable.TableId = tableDef.TblId
		if tableDef.Fkeys != nil {
			for _, fk := range tableDef.Fkeys {
				// A self-referencing foreign key uses table ID 0 as the durable
				// marker for "this table". Its metadata is recreated together with
				// the table and there is no external parent relation to refresh.
				if fk.ForeignTbl == 0 {
					continue
				}
				truncateTable.ForeignTbl = append(truncateTable.ForeignTbl, fk.ForeignTbl)
			}
		}

		truncateTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		// non-sys account can not truncate the cluster table
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if truncateTable.GetClusterTable().GetIsClusterTable() && accountId != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can truncate the cluster table")
		}

		if obj.PubInfo != nil {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate table '%v' which is published by other account", truncateTable.Table)
		}

		truncateTable.IndexTableNames = make([]string, 0)
		if tableDef.Indexes != nil {
			for _, indexdef := range tableDef.Indexes {
				if !indexdef.TableExist {
					continue
				}
				if catalog.IsRegularIndexAlgo(indexdef.IndexAlgo) ||
					catalog.IsMasterIndexAlgo(indexdef.IndexAlgo) ||
					catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo) {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
				} else if p, ok := indexplugin.Get(indexdef.IndexAlgo); ok {
					// Vector indexes delegate to the plugin's catalog
					// hook. HNSW/CAGRA/IVF-PQ truncate all hidden
					// tables; IVF-FLAT preserves metadata + centroids
					// (k-means model) and only drops entries.
					if p.Catalog().ShouldTruncateHiddenTable(indexdef.IndexAlgoTableType) {
						truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
					}
				}
			}
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_TRUNCATE_TABLE,
				Definition: &plan.DataDefinition_TruncateTable{
					TruncateTable: truncateTable,
				},
			},
		},
	}, nil
}

func buildDropTable(stmt *tree.DropTable, ctx CompilerContext) (*Plan, error) {
	if len(stmt.Names) == 1 {
		dropTable, err := buildDropTableSingle(stmt.IfExists, stmt.Temporary, stmt.Names[0], ctx)
		if err != nil {
			return nil, err
		}
		return &Plan{
			Plan: &plan.Plan_Ddl{
				Ddl: &plan.DataDefinition{
					DdlType: plan.DataDefinition_DROP_TABLE,
					Definition: &plan.DataDefinition_DropTable{
						DropTable: dropTable,
					},
				},
			},
		}, nil
	}

	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
		Tables:   make([]*plan.DropTable, 0, len(stmt.Names)),
	}
	for _, name := range stmt.Names {
		entry, err := buildDropTableSingle(stmt.IfExists, stmt.Temporary, name, ctx)
		if err != nil {
			return nil, err
		}
		dropTable.Tables = append(dropTable.Tables, entry)
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildDropTableSingle(ifExists bool, temporary bool, name *tree.TableName, ctx CompilerContext) (*plan.DropTable, error) {
	dropTable := &plan.DropTable{
		IfExists: ifExists,
	}

	dropTable.Database = string(name.SchemaName)
	// If the database name is empty, attempt to get default database name
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	// If the final database name is still empty, return an error
	if dropTable.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	dropTable.Table = string(name.ObjectName)

	obj, tableDef, err := ctx.Resolve(dropTable.Database, dropTable.Table, nil)
	if err != nil {
		return nil, err
	}

	// DROP TEMPORARY TABLE must never fall through to a same-named permanent
	// table. Resolve prefers a session temporary table when one exists, so a
	// non-temporary result means that the requested temporary table is absent.
	if tableDef == nil || (temporary && !tableDef.IsTemporary) {
		if !dropTable.IfExists {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
		return dropTable, nil
	}
	if err := validateTableIndexDefinitions(tableDef); err != nil {
		return nil, err
	}

	if obj.PubInfo != nil {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop subscription table %s", dropTable.Table)
	}

	enabled, err := IsForeignKeyChecksEnabled(ctx)
	if err != nil {
		return nil, err
	}
	if enabled && len(tableDef.RefChildTbls) > 0 {
		// if all children tables are self reference, we can drop the table
		if !HasFkSelfReferOnly(tableDef) {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop table '%v' referenced by some foreign key constraint", dropTable.Table)
		}
	}

	isView := (tableDef.ViewSql != nil)
	dropTable.IsView = isView
	if isView {
		if !dropTable.IfExists {
			// drop table v0, v0 is view
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
		// drop table if exists v0, v0 is view
		dropTable.Table = ""
		dropTable.TableDef = nil
		return dropTable, nil
	}

	// Can not use drop table to drop sequence.
	if tableDef.TableType == catalog.SystemSequenceRel {
		if !dropTable.IfExists {
			return nil, moerr.NewInternalError(ctx.GetContext(), "Should use 'drop sequence' to drop a sequence")
		}
		// If exists, don't drop anything.
		dropTable.Table = ""
		dropTable.TableDef = nil
		return dropTable, nil
	}

	dropTable.ClusterTable = &plan.ClusterTable{
		IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
	}

	// non-sys account can not drop the cluster table
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if dropTable.GetClusterTable().GetIsClusterTable() && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can drop the cluster table")
	}

	ignore := false
	val := ctx.GetContext().Value(defines.IgnoreForeignKey{})
	if val != nil {
		ignore = val.(bool)
	}

	dropTable.TableId = tableDef.TblId
	if tableDef.Fkeys != nil && !ignore {
		for _, fk := range tableDef.Fkeys {
			if fk.ForeignTbl == 0 {
				continue
			}
			dropTable.ForeignTbl = append(dropTable.ForeignTbl, fk.ForeignTbl)
		}
	}

	// collect child tables that needs remove fk relationships
	// with the table
	if tableDef.RefChildTbls != nil && !ignore {
		for _, childTbl := range tableDef.RefChildTbls {
			if childTbl == 0 {
				continue
			}
			dropTable.FkChildTblsReferToMe = append(dropTable.FkChildTblsReferToMe, childTbl)
		}
	}

	dropTable.IndexTableNames = make([]string, 0)
	if tableDef.Indexes != nil {
		for _, indexdef := range tableDef.Indexes {
			if indexdef.TableExist {
				dropTable.IndexTableNames = append(dropTable.IndexTableNames, indexdef.IndexTableName)
			}
		}
	}

	dropTable.TableDef = tableDef
	if !tableDef.IsTemporary {
		dropTable.UpdateFkSqls = []string{getSqlForDeleteTable(dropTable.Database, dropTable.Table)}
	}
	return dropTable, nil
}

func buildDropView(stmt *tree.DropView, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "drop multiple (%d) view", len(stmt.Names))
	}

	dropTable.Database = string(stmt.Names[0].SchemaName)

	// If the database name is empty, attempt to get default database name
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	// If the final database name is still empty, return an error
	if dropTable.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	dropTable.Table = string(stmt.Names[0].ObjectName)

	obj, tableDef, err := ctx.Resolve(dropTable.Database, dropTable.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
	} else {
		if tableDef.ViewSql == nil {
			if !dropTable.IfExists {
				return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
			}
			// DROP VIEW IF EXISTS must not target a same-named base table.
			// An empty table name is the established DropTable executor no-op.
			dropTable.Table = ""
		}
		if tableDef.ViewSql != nil && obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop view in subscription database")
		}
	}
	dropTable.IsView = true

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildCreateDatabase(stmt *tree.CreateDatabase, ctx CompilerContext) (*Plan, error) {
	if err := validateIdentifier(ctx.GetContext(), string(stmt.Name)); err != nil {
		return nil, err
	}

	createDB := &plan.CreateDatabase{
		IfNotExists: stmt.IfNotExists,
		Database:    string(stmt.Name),
	}

	if stmt.SubscriptionOption != nil {
		accName := string(stmt.SubscriptionOption.From)
		pubName := string(stmt.SubscriptionOption.Publication)
		subName := string(stmt.Name)
		if err := ctx.CheckSubscriptionValid(subName, accName, pubName); err != nil {
			return nil, err
		}
		createDB.SubscriptionOption = &plan.SubscriptionOption{
			From:        string(stmt.SubscriptionOption.From),
			Publication: string(stmt.SubscriptionOption.Publication),
		}
	}
	createDB.Sql = stmt.Sql

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_DATABASE,
				Definition: &plan.DataDefinition_CreateDatabase{
					CreateDatabase: createDB,
				},
			},
		},
	}, nil
}

func buildDropDatabase(stmt *tree.DropDatabase, ctx CompilerContext) (*Plan, error) {
	dropDB := &plan.DropDatabase{
		IfExists: stmt.IfExists,
		Database: string(stmt.Name),
	}
	if publishing, err := ctx.IsPublishing(dropDB.Database); err != nil {
		return nil, err
	} else if publishing {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop database '%v' which is publishing", dropDB.Database)
	}

	if ctx.DatabaseExists(string(stmt.Name), nil) {
		databaseId, err := ctx.GetDatabaseId(string(stmt.Name), nil)
		if err != nil {
			return nil, err
		}
		dropDB.DatabaseId = databaseId

		// check foreign keys exists or not
		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if enabled {
			dropDB.CheckFKSql = getSqlForCheckHasDBRefersTo(dropDB.Database)
		}
	}

	dropDB.UpdateFkSql = getSqlForDeleteDB(dropDB.Database)

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_DATABASE,
				Definition: &plan.DataDefinition_DropDatabase{
					DropDatabase: dropDB,
				},
			},
		},
	}, nil
}

// In MySQL, the CREATE INDEX syntax can only create one index instance at a time
func buildCreateIndex(stmt *tree.CreateIndex, ctx CompilerContext) (*Plan, error) {
	if err := validateIdentifier(ctx.GetContext(), string(stmt.Name)); err != nil {
		return nil, err
	}
	createIndex := &plan.CreateIndex{}
	if len(stmt.Table.SchemaName) == 0 {
		createIndex.Database = ctx.DefaultDatabase()
	} else {
		createIndex.Database = string(stmt.Table.SchemaName)
	}
	// check table
	tableName := string(stmt.Table.ObjectName)
	obj, tableDef, err := ctx.Resolve(createIndex.Database, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), createIndex.Database, tableName)
	}
	if err := validateTableIndexDefinitions(tableDef); err != nil {
		return nil, err
	}
	if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
		return nil, err
	}
	if obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create index in subscription database")
	}
	// check index
	indexName := string(stmt.Name)
	if _, found := resolveIndexName(tableDef.Indexes, indexName); found {
		return nil, moerr.NewDuplicateKey(ctx.GetContext(), indexName)
	}
	// build index
	var ftIdx *tree.FullTextIndex
	var uIdx *tree.UniqueIndex
	var sIdx *tree.Index
	switch stmt.IndexCat {
	case tree.INDEX_CATEGORY_UNIQUE:
		uIdx = &tree.UniqueIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	case tree.INDEX_CATEGORY_NONE:
		sIdx = &tree.Index{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
			KeyType:     stmt.IndexOption.IType,
		}
	case tree.INDEX_CATEGORY_FULLTEXT:
		ftIdx = &tree.FullTextIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	case tree.INDEX_CATEGORY_FULLTEXT2:
		// CREATE FULLTEXT2 INDEX — the distinct WAND positional engine; routed to
		// the fulltext2 plugin by buildFullTextIndexTable via IsV2.
		ftIdx = &tree.FullTextIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
			IsV2:        true,
		}
	case tree.INDEX_CATEGORY_SPATIAL:
		keyType := tree.INDEX_TYPE_RTREE
		if stmt.IndexOption != nil && stmt.IndexOption.IType != tree.INDEX_TYPE_INVALID {
			if stmt.IndexOption.IType != tree.INDEX_TYPE_RTREE {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "SPATIAL INDEX only supports USING RTREE")
			}
			keyType = stmt.IndexOption.IType
		}
		sIdx = &tree.Index{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
			KeyType:     keyType,
		}
	default:
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}
	colMap := make(map[string]*ColDef)
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}

	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		colMap[tableDef.Pkey.CompPkeyCol.Name] = tableDef.Pkey.CompPkeyCol
	}

	// index.TableDef.Defs store info of index need to be modified
	// index.IndexTables store index table need to be created
	oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
	createIndex.OriginTablePrimaryKey = oriPriKeyName

	indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
	if uIdx != nil {
		if err := buildUniqueIndexTable(indexInfo, []*tree.UniqueIndex{uIdx}, colMap, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	if ftIdx != nil {
		if err := buildFullTextIndexTable(indexInfo, []*tree.FullTextIndex{ftIdx}, colMap, tableDef.Indexes, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	if sIdx != nil {
		if err := buildSecondaryIndexDef(indexInfo, []*tree.Index{sIdx}, colMap, tableDef.Indexes, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	createIndex.Index = indexInfo
	createIndex.Table = tableName
	createIndex.TableDef = tableDef

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_INDEX,
				Definition: &plan.DataDefinition_CreateIndex{
					CreateIndex: createIndex,
				},
			},
		},
	}, nil
}

func checkCreateIndexTableType(ctx context.Context, tableDef *TableDef) error {
	if tableDef.TableType == catalog.SystemExternalRel {
		isIceberg, err := IsIcebergTableDef(ctx, tableDef)
		if err != nil {
			return err
		}
		if isIceberg {
			return moerr.NewInvalidInput(ctx, "cannot create index on Iceberg table mapping")
		}
		return moerr.NewInvalidInput(ctx, "cannot create index on external table")
	}
	return nil
}

func rejectExternalTableInlineIndexes(ctx context.Context, stmt *tree.CreateTable) error {
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			for _, attr := range def.Attributes {
				switch attr.(type) {
				case *tree.AttributePrimaryKey, *tree.AttributeKey, *tree.AttributeUnique, *tree.AttributeUniqueKey:
					return moerr.NewInvalidInput(ctx, "cannot create index on external table")
				}
			}
		case *tree.PrimaryKeyIndex, *tree.Index, *tree.UniqueIndex, *tree.FullTextIndex:
			return moerr.NewInvalidInput(ctx, "cannot create index on external table")
		}
	}
	return nil
}

func rejectMongoDBExternalTableOnUpdate(ctx context.Context, stmt *tree.CreateTable) error {
	for _, item := range stmt.Defs {
		column, ok := item.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		for _, attribute := range column.Attributes {
			if _, ok := attribute.(*tree.AttributeOnUpdate); ok {
				return moerr.NewNotSupportedf(ctx,
					"MongoDB external table column '%s' does not support ON UPDATE",
					column.Name.ColNameOrigin())
			}
		}
	}
	return nil
}

// checkDropReferencedKeyForeignKeyDependency rejects removal of the exact
// PRIMARY/UNIQUE key selected by a live child FK. ForeignKeyDef instances
// written before ReferencedIndexName existed cannot prove which of several
// compatible keys was historically selected, so every compatible key is
// protected rather than guessing from today's index set and risking a stale
// catalog binding.
func checkDropReferencedKeyForeignKeyDependency(
	ctx CompilerContext,
	parentTableDef *TableDef,
	indexName string,
	ignoredSelfForeignKeys map[string]struct{},
) error {
	var targetColumns []string
	if indexName == "PRIMARY" && parentTableDef.Pkey != nil {
		targetColumns = parentTableDef.Pkey.Names
	} else {
		for _, index := range parentTableDef.Indexes {
			if index.IndexName == indexName && index.Unique {
				targetColumns = index.Parts
				break
			}
		}
	}
	if len(targetColumns) == 0 || len(parentTableDef.RefChildTbls) == 0 {
		return nil
	}

	for _, childTableID := range parentTableDef.RefChildTbls {
		selfReference := childTableID == 0 || childTableID == parentTableDef.TblId
		childTableDef := parentTableDef
		if !selfReference {
			_, resolved, err := ctx.ResolveById(childTableID, nil)
			if err != nil {
				return err
			}
			if resolved == nil {
				return moerr.NewInternalErrorf(ctx.GetContext(),
					"The reference foreign key table %d does not exist", childTableID)
			}
			childTableDef = resolved
		}

		for _, fk := range childTableDef.Fkeys {
			if fk.ForeignTbl != parentTableDef.TblId && !(selfReference && fk.ForeignTbl == 0) {
				continue
			}
			if selfReference {
				if _, ignored := ignoredSelfForeignKeys[fk.Name]; ignored {
					continue
				}
			}

			referencedIndexName := fk.ReferencedIndexName
			if referencedIndexName == "" {
				referredColumns := make([]string, len(fk.ForeignCols))
				for i, columnID := range fk.ForeignCols {
					column := FindColumnByColId(parentTableDef.Cols, columnID)
					if column == nil {
						return moerr.NewInternalErrorf(ctx.GetContext(),
							"foreign key %s references missing column id %d", fk.Name, columnID)
					}
					referredColumns[i] = column.Name
				}
				if foreignKeyReferencedColumnsMatch(targetColumns, referredColumns) {
					return moerr.NewErrDropIndexNeededInForeignKey(ctx.GetContext(), indexName)
				}
				continue
			}

			if referencedIndexName == indexName {
				return moerr.NewErrDropIndexNeededInForeignKey(ctx.GetContext(), indexName)
			}
		}
	}
	return nil
}

func buildDropIndex(stmt *tree.DropIndex, ctx CompilerContext) (*Plan, error) {
	dropIndex := &plan.DropIndex{}
	if len(stmt.TableName.SchemaName) == 0 {
		dropIndex.Database = ctx.DefaultDatabase()
	} else {
		dropIndex.Database = string(stmt.TableName.SchemaName)
	}

	// If the final database name is still empty, return an error
	if dropIndex.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	// check table
	dropIndex.Table = string(stmt.TableName.ObjectName)
	obj, tableDef, err := ctx.Resolve(dropIndex.Database, dropIndex.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropIndex.Database, dropIndex.Table)
	}
	if err := validateTableIndexDefinitions(tableDef); err != nil {
		return nil, err
	}

	if obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop index in subscription database")
	}

	// check index
	requestedIndexName := string(stmt.Name)
	resolvedIndexName, found := resolveIndexName(tableDef.Indexes, requestedIndexName)
	dropIndex.IndexName = resolvedIndexName

	if !found {
		if stmt.IfExists {
			// An empty index name represents the no-op path for DROP INDEX IF EXISTS.
			dropIndex.IndexName = ""
		} else {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "not found index: %s", requestedIndexName)
		}
	} else if err := checkDropReferencedKeyForeignKeyDependency(ctx, tableDef, dropIndex.IndexName, nil); err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_INDEX,
				Definition: &plan.DataDefinition_DropIndex{
					DropIndex: dropIndex,
				},
			},
		},
	}, nil
}

// Get tabledef(col, viewsql, properties) for alterview.
func buildAlterView(stmt *tree.AlterView, ctx CompilerContext) (*Plan, error) {
	viewName := string(stmt.Name.ObjectName)
	alterView := &plan.AlterView{
		IfExists: stmt.IfExists,
		TableDef: &plan.TableDef{
			Name: viewName,
		},
	}
	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		alterView.Database = ""
	} else {
		alterView.Database = string(stmt.Name.SchemaName)
	}
	if alterView.Database == "" {
		alterView.Database = ctx.DefaultDatabase()
	}

	// step 1: check the view exists or not
	obj, oldViewDef, err := ctx.Resolve(alterView.Database, viewName, nil)
	if err != nil {
		return nil, err
	}
	if oldViewDef == nil {
		if !alterView.IfExists {
			return nil, moerr.NewBadView(ctx.GetContext(),
				alterView.Database,
				viewName)
		}
	} else {
		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter view in subscription database")
		}
		if oldViewDef.ViewSql == nil {
			return nil, moerr.NewBadView(ctx.GetContext(),
				alterView.Database,
				viewName)
		}
	}

	// step 2: generate new view def
	ctx.SetBuildingAlterView(true, alterView.Database, viewName)
	// restore
	defer func() {
		ctx.SetBuildingAlterView(false, "", "")
	}()
	tableDef, err := genViewTableDef(ctx, stmt.AsSource, stmt.ColNames, alterView.Database, viewName)
	if err != nil {
		return nil, err
	}
	if err := validatePersistedTableIdentifiers(ctx.GetContext(), tableDef); err != nil {
		return nil, err
	}

	alterView.TableDef.Cols = tableDef.Cols
	alterView.TableDef.ViewSql = tableDef.ViewSql
	alterView.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_VIEW,
				Definition: &plan.DataDefinition_AlterView{
					AlterView: alterView,
				},
			},
		},
	}, nil
}

func rejectCrossDatabaseTableRename(
	ctx context.Context,
	sourceDatabase string,
	option *tree.AlterOptionTableName,
) error {
	target := option.Name.ToTableName()
	if !target.ExplicitSchema || string(target.Schema()) == sourceDatabase {
		return nil
	}

	return moerr.NewNotSupportedf(
		ctx,
		"cross-database table rename from database '%s' to '%s'",
		sourceDatabase,
		target.Schema(),
	)
}

func buildRenameTable(stmt *tree.RenameTable, ctx CompilerContext) (*Plan, error) {

	type renamedInfo struct {
		objRef   *ObjectRef
		tableDef *TableDef
	}
	alterTables := stmt.AlterTables
	renameTables := make([]*plan.AlterTable, 0)
	removed := make(map[string]bool)
	nameMapping := make(map[string]*renamedInfo)
	for _, alterTable := range alterTables {
		schemaName, tableName := string(alterTable.Table.Schema()), string(alterTable.Table.Name())
		if schemaName == "" {
			schemaName = ctx.DefaultDatabase()
		}
		srcKey := schemaName + "." + tableName
		var objRef *ObjectRef
		var tableDef *TableDef
		var err error
		if info, ok := nameMapping[srcKey]; ok {
			objRef = info.objRef
			tableDef = DeepCopyTableDef(info.tableDef, true)
			tableDef.Name = tableName
		} else if removed[srcKey] {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
		} else {
			objRef, tableDef, err = ctx.Resolve(schemaName, tableName, nil)
			if err != nil {
				return nil, err
			}
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
		}
		if err := validateTableIndexDefinitions(tableDef); err != nil {
			return nil, err
		}
		for _, option := range alterTable.Options {
			if rename, ok := option.(*tree.AlterOptionTableName); ok {
				if err := rejectCrossDatabaseTableRename(ctx.GetContext(), schemaName, rename); err != nil {
					return nil, err
				}
			}
		}

		if tableDef.IsTemporary {
			return nil, moerr.NewNYI(ctx.GetContext(), "alter table for temporary table")
		}

		if tableDef.ViewSql != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "you should use alter view statemnt for View")
		}
		if objRef.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter table in subscription database")
		}
		isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if isClusterTable && accountId != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
		}

		alterTablePlan := &plan.AlterTable{
			Actions:        make([]*plan.AlterTable_Action, len(alterTable.Options)),
			AlgorithmType:  plan.AlterTable_INPLACE,
			Database:       schemaName,
			TableDef:       tableDef,
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		var updateSqls []string
		for i, option := range alterTable.Options {
			switch opt := option.(type) {
			case *tree.AlterOptionTableName:
				oldName := tableName
				newName := string(opt.Name.ToTableName().ObjectName)
				if err := validateIdentifier(ctx.GetContext(), newName); err != nil {
					return nil, err
				}
				dstKey := schemaName + "." + newName
				if oldName != newName {
					if _, ok := nameMapping[dstKey]; ok {
						return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
					}
					if !removed[dstKey] {
						_, existDef, err := ctx.Resolve(schemaName, newName, nil)
						if err != nil {
							return nil, err
						}
						if existDef != nil {
							return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
						}
					}
				}
				alterTablePlan.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AlterName{
						AlterName: &plan.AlterTableName{
							OldName: oldName,
							NewName: newName,
						},
					},
				}
				updateSqls = append(updateSqls, getSqlForRenameTable(schemaName, oldName, newName)...)
				delete(nameMapping, srcKey)
				removed[srcKey] = true
				nameMapping[dstKey] = &renamedInfo{objRef: objRef, tableDef: tableDef}
				delete(removed, dstKey)

			default:
				return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
			}
			alterTablePlan.UpdateFkSqls = updateSqls
		}
		renameTables = append(renameTables, alterTablePlan)
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_RENAME_TABLE,
				Definition: &plan.DataDefinition_RenameTable{
					RenameTable: &plan.RenameTable{
						AlterTables: renameTables,
					},
				},
			},
		},
	}, nil
}

func formatTreeNode(opt tree.NodeFormatter) string {
	// get callsite
	ft := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
	opt.Format(ft)
	return ft.String()
}

func buildAlterTableInplace(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	tableName := string(stmt.Table.ObjectName)
	databaseName := string(stmt.Table.SchemaName)
	if databaseName == "" {
		databaseName = ctx.DefaultDatabase()
	}

	_, tableDef, err := ctx.Resolve(databaseName, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), databaseName, tableName)
	}

	alterTable := &plan.AlterTable{
		Actions:        make([]*plan.AlterTable_Action, len(stmt.Options)),
		AlgorithmType:  plan.AlterTable_INPLACE,
		Database:       databaseName,
		TableDef:       tableDef,
		IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		RawSQL: tree.StringWithOpts(
			stmt,
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithSingleQuoteString(),
		),
	}
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if alterTable.IsClusterTable && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(
			ctx.GetContext(),
			"only the sys account can alter the cluster table",
		)
	}

	colMap := make(map[string]*ColDef)
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}
	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		colMap[tableDef.Pkey.CompPkeyCol.Name] = tableDef.Pkey.CompPkeyCol
	}
	unsupportedErrFmt := "unsupported alter option in inplace mode: %s"

	var detectSqls []string
	var updateSqls []string
	uniqueIndexInfos := make([]*tree.UniqueIndex, 0)
	secondaryIndexInfos := make([]*tree.Index, 0)
	// Planning must consume ALTER actions in statement order without mutating
	// tableDef: tableDef is also shipped to execution, where its original index
	// list drives pre-DDL locking. currentTableDef is the planner-owned evolving
	// schema used by every subsequent existence and semantic check.
	currentTableDef := DeepCopyTableDef(tableDef, true)
	currentIndexNames := make(map[string]bool, len(currentTableDef.Indexes))
	for _, indexDef := range currentTableDef.Indexes {
		currentIndexNames[indexNameKey(indexDef.IndexName)] = true
	}
	currentForeignKeyNames := make(map[string]bool, len(currentTableDef.Fkeys))
	for _, foreignKey := range currentTableDef.Fkeys {
		if foreignKey.Name != "" {
			currentForeignKeyNames[foreignKey.Name] = true
		}
	}
	droppedIndexNames := make(map[string]bool)
	droppedForeignKeyNames := make(map[string]bool)
	for i, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.AlterOptionDrop:
			alterTableDrop := new(plan.AlterTableDrop)
			constraintName := string(opt.Name)
			if constraintNameAreWhiteSpaces(constraintName) {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't DROP '%s'; check that column/key exists",
					constraintName,
				)
			}
			name_not_found := true
			sequentiallyDropped := false
			switch opt.Typ {
			case tree.AlterTableDropIndex, tree.AlterTableDropKey:
				alterTableDrop.Typ = plan.AlterTableDrop_INDEX
				resolvedName, found := resolveIndexName(currentTableDef.Indexes, constraintName)
				if found {
					constraintName = resolvedName
					if err := checkDropReferencedKeyForeignKeyDependency(ctx, currentTableDef, constraintName, nil); err != nil {
						return nil, err
					}
					name_not_found = false
				}
				if !name_not_found {
					delete(currentIndexNames, indexNameKey(constraintName))
					droppedIndexNames[indexNameKey(constraintName)] = true
					currentTableDef.Indexes = RemoveIf(currentTableDef.Indexes, func(indexDef *plan.IndexDef) bool {
						return indexDef.IndexName == constraintName
					})
				} else {
					sequentiallyDropped = droppedIndexNames[indexNameKey(constraintName)]
				}
			case tree.AlterTableDropForeignKey:
				alterTableDrop.Typ = plan.AlterTableDrop_FOREIGN_KEY
				for _, fk := range currentTableDef.Fkeys {
					if fk.Name == constraintName {
						name_not_found = false
						updateSqls = append(
							updateSqls,
							getSqlForDeleteConstraint(
								databaseName,
								tableName,
								constraintName,
							),
						)
						break
					}
				}
				if !name_not_found {
					delete(currentForeignKeyNames, constraintName)
					droppedForeignKeyNames[constraintName] = true
					currentTableDef.Fkeys = RemoveIf(currentTableDef.Fkeys, func(fk *plan.ForeignKeyDef) bool {
						return fk.Name == constraintName
					})
				} else {
					sequentiallyDropped = droppedForeignKeyNames[constraintName]
				}
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					formatTreeNode(opt),
				)
			}
			alterTableDrop.Name = constraintName
			if name_not_found {
				if sequentiallyDropped {
					return nil, moerr.NewErrCantDropFieldOrKey(ctx.GetContext(), constraintName)
				}
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't DROP '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_Drop{
					Drop: alterTableDrop,
				},
			}

		case *tree.AlterOptionAdd:
			switch def := opt.Def.(type) {
			case *tree.ForeignKey:
				err = adjustConstraintName(ctx.GetContext(), def)
				if err != nil {
					return nil, err
				}
				if currentForeignKeyNames[def.ConstraintSymbol] {
					return nil, moerr.NewErrDuplicateKeyName(ctx.GetContext(), def.ConstraintSymbol)
				}
				currentForeignKeyNames[def.ConstraintSymbol] = true

				fkData, err := getForeignKeyData(ctx, databaseName, currentTableDef, def)
				if err != nil {
					return nil, err
				}
				currentTableDef.Fkeys = append(currentTableDef.Fkeys, fkData.Def)
				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddFk{
						AddFk: &plan.AlterTableAddFk{
							DbName:    fkData.ParentDbName,
							TableName: fkData.ParentTableName,
							Cols:      fkData.Cols.Cols,
							Fkey:      fkData.Def,
						},
					},
				}
				// for new fk in this alter table, the data in the table must
				// be checked to confirm that it is compliant with foreign key constraints.
				if fkData.IsSelfRefer {
					// fk self refer.
					// check columns of fk self refer are valid
					err = checkFkColsAreValid(ctx, fkData, currentTableDef)
					if err != nil {
						return nil, err
					}
					sqls, err := genSqlsForCheckFKSelfRefer(
						ctx.GetContext(),
						databaseName,
						currentTableDef.Name,
						currentTableDef.Cols,
						[]*plan.ForeignKeyDef{fkData.Def},
					)
					if err != nil {
						return nil, err
					}
					detectSqls = append(detectSqls, sqls...)
					if !slices.ContainsFunc(currentTableDef.RefChildTbls, func(tableID uint64) bool {
						return tableID == 0 || tableID == currentTableDef.TblId
					}) {
						// Keep the planner-only evolving parent/child state in sync.
						// The executor materializes this relationship after the ALTER,
						// but later actions in this statement must already observe it.
						currentTableDef.RefChildTbls = append(currentTableDef.RefChildTbls, 0)
					}
				} else {
					// get table def of parent table
					_, parentTableDef, err := ctx.Resolve(
						fkData.ParentDbName,
						fkData.ParentTableName,
						nil,
					)
					if err != nil {
						return nil, err
					}
					if parentTableDef == nil {
						return nil, moerr.NewNoSuchTable(
							ctx.GetContext(),
							fkData.ParentDbName,
							fkData.ParentTableName,
						)
					}
					sql, err := genSqlForCheckFKConstraints(
						ctx.GetContext(),
						fkData.Def,
						databaseName, tableDef.Name, tableDef.Cols,
						fkData.ParentDbName,
						fkData.ParentTableName,
						parentTableDef.Cols,
					)
					if err != nil {
						return nil, err
					}
					detectSqls = append(detectSqls, sql)
				}
				updateSqls = append(updateSqls, fkData.UpdateSql)
			case *tree.UniqueIndex:
				if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
					return nil, err
				}
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.GetIndexName()
				if err := checkDuplicateConstraint(
					currentIndexNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}
				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyUniqueIndexName(currentIndexNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildUniqueIndexTable(
					indexInfo,
					[]*tree.UniqueIndex{def},
					colMap,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}
				currentTableDef.Indexes = append(currentTableDef.Indexes, indexInfo.TableDef.Indexes...)

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			case *tree.FullTextIndex:
				if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
					return nil, err
				}
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.Name
				if err := checkDuplicateConstraint(
					currentIndexNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}

				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyFullTextIndexName(currentIndexNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildFullTextIndexTable(
					indexInfo,
					[]*tree.FullTextIndex{def},
					colMap,
					currentTableDef.Indexes,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}
				currentTableDef.Indexes = append(currentTableDef.Indexes, indexInfo.TableDef.Indexes...)

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			case *tree.Index:
				if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
					return nil, err
				}
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.Name

				if err := checkDuplicateConstraint(
					currentIndexNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}

				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyIndexName(currentIndexNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)

				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildSecondaryIndexDef(
					indexInfo,
					[]*tree.Index{def},
					colMap,
					currentTableDef.Indexes,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}
				currentTableDef.Indexes = append(currentTableDef.Indexes, indexInfo.TableDef.Indexes...)

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					formatTreeNode(def),
				)
			}

		case *tree.AlterOptionAlterIndex:
			alterTableIndex := new(plan.AlterTableAlterIndex)
			constraintName := string(opt.Name)
			alterTableIndex.Visible = opt.Visibility == tree.VISIBLE_TYPE_VISIBLE

			resolvedName, found := resolveIndexName(currentTableDef.Indexes, constraintName)
			if !found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't ALTER '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTableIndex.IndexName = resolvedName
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterIndex{
					AlterIndex: alterTableIndex,
				},
			}

		case *tree.AlterOptionAlterReIndex:
			alterTableReIndex := new(plan.AlterTableAlterReIndex)
			constraintName := string(opt.Name)
			// ForceSync (sync vs async rebuild) is the only build-time flag the
			// plan node carries. The shared index_option_list grammar already
			// restricts the algo (REINDEX rules cover only ivfflat/hnsw/ivfpq/
			// cagra) and validates option values (> 0); the per-index option
			// merge + reject happens at compile in Compile.ValidateReindexParams,
			// reading the options straight off the parse tree.
			alterTableReIndex.ForceSync = opt.ForceSync
			alterTableReIndex.Merge = opt.Merge

			resolvedName, found := resolveIndexName(currentTableDef.Indexes, constraintName)
			if !found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't REINDEX '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTableReIndex.IndexName = resolvedName
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterReindex{
					AlterReindex: alterTableReIndex,
				},
			}

		case *tree.AlterOptionAlterAutoUpdate:
			alterTableAutoUpdate := new(plan.AlterTableAlterAutoUpdate)
			constraintName := string(opt.Name)

			switch opt.KeyType {
			case tree.INDEX_TYPE_IVFFLAT:
				if opt.Day < 0 {
					return nil, moerr.NewInternalErrorf(
						ctx.GetContext(),
						"day should be >= 0.",
					)
				}
				if opt.Hour < 0 || opt.Hour > 23 {
					return nil, moerr.NewInternalErrorf(
						ctx.GetContext(),
						"hour should be between 0 and 23.",
					)
				}
				alterTableAutoUpdate.AutoUpdate = opt.AutoUpdate
				alterTableAutoUpdate.Day = opt.Day
				alterTableAutoUpdate.Hour = opt.Hour
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					opt.KeyType.ToString(),
				)
			}

			resolvedName, found := resolveIndexName(currentTableDef.Indexes, constraintName)
			if !found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't REINDEX '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTableAutoUpdate.IndexName = resolvedName
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterAutoUpdate{
					AlterAutoUpdate: alterTableAutoUpdate,
				},
			}

		case *tree.TableOptionComment:
			if getNumOfCharacters(opt.Comment) > maxLengthOfTableComment {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"comment for field '%s' is too long",
					alterTable.TableDef.Name,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterComment{
					AlterComment: &plan.AlterTableComment{
						NewComment: opt.Comment,
					},
				},
			}

		case *tree.AlterOptionTableName:
			oldName := tableDef.Name
			newName := string(opt.Name.ToTableName().ObjectName)
			if oldName == newName {
				continue
			}

			// TODO ONLY Check
			_, tableDef, err := ctx.Resolve(databaseName, newName, nil)
			if err != nil {
				return nil, err
			}
			if tableDef != nil {
				return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
			}

			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterName{
					AlterName: &plan.AlterTableName{
						OldName: oldName,
						NewName: newName,
					},
				},
			}

			updateSqls = append(
				updateSqls,
				getSqlForRenameTable(databaseName, oldName, newName)...,
			)
		case *tree.TableOptionAutoIncrement:
			if !tableHasAutoIncrementColumn(tableDef) {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"Table '%s' does not have an AUTO_INCREMENT column", tableDef.Name)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterAutoIncrement{
					AlterAutoIncrement: &plan.AlterTableAutoIncrement{
						NewOffset: autoIncrementValueToOffset(opt.Value),
					},
				},
			}
		case *tree.AlterOptionAlgorithm:
			// algorithm hint already consumed by ResolveAlterTableAlgorithm
			alterTable.Actions[i] = nil
		case *tree.AlterOptionLock:
			// lock already validated by resolveAndValidateLock
			alterTable.Actions[i] = nil

		case *tree.AlterOptionAlterCheck, *tree.TableOptionCharset:
			continue

		case *tree.AlterTableModifyColumnClause:
			// defensively check again
			ok, _ := isInplaceModifyColumn(ctx.GetContext(), opt, tableDef)
			if !ok {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"failed inplace check: %s",
					formatTreeNode(opt),
				)
			}

			if alterTable.CopyTableDef == nil {
				alterTable.CopyTableDef = DeepCopyTableDef(tableDef, true)
			}

			// update new column info to copy_table_def
			_, err := updateNewColumnInTableDef(
				ctx,
				alterTable.CopyTableDef,
				FindColumn(tableDef.Cols, opt.NewColumn.Name.ColName()),
				opt.NewColumn,
				opt.Position,
			)
			if err != nil {
				return nil, err
			}
		case *tree.AlterTableChangeColumnClause:
			// A same-name CHANGE with an unchanged storage layout has the same
			// execution semantics as MODIFY, but historically took the COPY path.
			ok, _ := isInplaceChangeColumn(ctx.GetContext(), opt, tableDef)
			if !ok {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"failed inplace check: %s",
					formatTreeNode(opt),
				)
			}

			if alterTable.CopyTableDef == nil {
				alterTable.CopyTableDef = DeepCopyTableDef(tableDef, true)
			}

			_, err := updateNewColumnInTableDef(
				ctx,
				alterTable.CopyTableDef,
				FindColumn(alterTable.CopyTableDef.Cols, opt.OldColumnName.ColName()),
				opt.NewColumn,
				opt.Position,
			)
			if err != nil {
				return nil, err
			}
		case *tree.AlterTableRenameColumnClause:
			if err := checkTableType(ctx.GetContext(), tableDef, ""); err != nil {
				return nil, err
			}

			if alterTable.CopyTableDef == nil {
				alterTable.CopyTableDef = DeepCopyTableDef(tableDef, true)
			}

			col := FindColumn(
				alterTable.CopyTableDef.Cols,
				opt.OldColumnName.ColName(),
			)
			if col == nil {
				return nil, moerr.NewBadFieldError(
					ctx.GetContext(),
					opt.OldColumnName.ColNameOrigin(),
					alterTable.TableDef.Name,
				)
			}
			oldColNameOrigin := col.OriginName
			newColNameOrigin := opt.NewColumnName.ColNameOrigin()

			if oldColNameOrigin == newColNameOrigin {
				continue
			}

			sqls, err := updateRenameColumnInTableDef(
				ctx,
				col,
				alterTable.CopyTableDef,
				opt,
			)
			if err != nil {
				return nil, err
			}
			// Only INPLACE sends AlterTableRenameCol.checks to TN. COPY persists
			// the rewritten CHECK definitions through its temporary-table schema
			// and therefore remains compatible with protocol versions before v15.
			if err := requireCheckRenameProtocol(ctx, alterTable.CopyTableDef.Checks); err != nil {
				return nil, err
			}

			updateSqls = append(updateSqls,
				getSqlForRenameColumn(tableDef.DbName,
					alterTable.TableDef.Name,
					oldColNameOrigin,
					newColNameOrigin)...)

			updateSqls = append(updateSqls, sqls...)

			alterTable.Actions = append(
				alterTable.Actions,
				&plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AlterRenameColumn{
						AlterRenameColumn: &plan.AlterRenameColumn{
							OldName:     oldColNameOrigin,
							NewName:     newColNameOrigin,
							SequenceNum: int32(col.Seqnum),
						},
					},
				},
			)

		default:
			return nil, moerr.NewInvalidInputf(
				ctx.GetContext(),
				unsupportedErrFmt,
				formatTreeNode(opt),
			)
		}
	}

	if alterTable.CopyTableDef != nil {
		alterTable.Actions = append(alterTable.Actions, &plan.AlterTable_Action{
			Action: &plan.AlterTable_Action_AlterReplaceDef{
				AlterReplaceDef: &plan.AlterReplaceDef{},
			},
		})
	}

	if stmt.PartitionOption != nil {
		alterTable.AlterPartition = &plan.AlterPartitionOption{}

		switch p := stmt.PartitionOption.(type) {
		case *tree.AlterPartitionAddPartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_AddPartitionTables
			defs, err := constructAddedPartitionDefs(ctx, tableDef, p)
			if err != nil {
				return nil, err
			}
			alterTable.AlterPartition.PartitionDefs = defs
		case *tree.AlterPartitionDropPartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_DropPartitionTables
		case *tree.AlterPartitionTruncatePartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_TruncatePartitionTables
		case *tree.AlterPartitionRedefinePartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_RedefinePartitionTables
		default:
			return nil, moerr.NewNotSupportedf(
				ctx.GetContext(),
				unsupportedErrFmt,
				formatTreeNode(stmt.PartitionOption),
			)
		}
	}

	// check Constraint Name (include index/ unique)
	if err := checkConstraintNames(
		uniqueIndexInfos,
		secondaryIndexInfos,
		ctx.GetContext(),
	); err != nil {
		return nil, err
	}

	alterTable.DetectSqls = detectSqls
	alterTable.UpdateFkSqls = updateSqls
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_TABLE,
				Definition: &plan.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}, nil
}

func buildLockTables(stmt *tree.LockTableStmt, ctx CompilerContext) (*Plan, error) {
	lockTables := make([]*plan.TableLockInfo, 0, len(stmt.TableLocks))
	uniqueTableName := make(map[string]bool)

	// Check table locks
	for _, tableLock := range stmt.TableLocks {
		tb := tableLock.Table

		// get table name
		tblName := string(tb.ObjectName)

		// get database name
		var schemaName string
		if len(tb.SchemaName) == 0 {
			schemaName = ctx.DefaultDatabase()
		} else {
			schemaName = string(tb.SchemaName)
		}

		// check table whether exist
		obj, tableDef, err := ctx.Resolve(schemaName, tblName, nil)
		if err != nil {
			return nil, err
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tblName)
		}

		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot lock table in subscription database")
		}

		// check the stmt whether locks the same table
		if _, ok := uniqueTableName[tblName]; ok {
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Not unique table %s", tblName)
		}

		uniqueTableName[tblName] = true

		tableLockInfo := &plan.TableLockInfo{
			LockType: plan.TableLockType(tableLock.LockType),
			TableDef: tableDef,
		}
		lockTables = append(lockTables, tableLockInfo)
	}

	LockTables := &plan.LockTables{
		TableLocks: lockTables,
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_LOCK_TABLES,
				Definition: &plan.DataDefinition_LockTables{
					LockTables: LockTables,
				},
			},
		},
	}, nil
}

func buildUnLockTables(stmt *tree.UnLockTableStmt, ctx CompilerContext) (*Plan, error) {
	unLockTables := &plan.UnLockTables{}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_UNLOCK_TABLES,
				Definition: &plan.DataDefinition_UnlockTables{
					UnlockTables: unLockTables,
				},
			},
		},
	}, nil
}

type FkData struct {
	// fk reference to itself
	IsSelfRefer bool
	// the database that the fk refers to
	ParentDbName string
	// the table that the fk refers to
	ParentTableName string
	// the columns in foreign key
	Cols *plan.FkColName
	// the columns referred
	ColsReferred *plan.FkColName
	// fk definition
	Def *plan.ForeignKeyDef
	// the column typs in foreign key
	ColTyps map[int]*plan.Type
	// update foreign keys relations
	UpdateSql string
	// forward reference
	ForwardRefer bool
	// catalogLayout records whether the tenant has committed all FK metadata
	// columns. During an asynchronous same-version offset upgrade, legacy SQL
	// remains valid against both the old and partially upgraded table layouts.
	catalogLayout foreignKeyCatalogLayout
}

// getForeignKeyData prepares the foreign key data.
// for fk refer except the self refer, it is same as the previous one.
// but for fk self refer, it is different in not checking fk self refer instantly.
// because it is not ready. It should be checked after the pk,uk has been ready.
func getForeignKeyData(ctx CompilerContext, dbName string, tableDef *TableDef, def *tree.ForeignKey) (*FkData, error) {
	refer := def.Refer
	catalogLayout, err := resolveForeignKeyCatalogLayout(ctx)
	if err != nil {
		return nil, err
	}
	fkData := FkData{
		Def: &plan.ForeignKeyDef{
			Name:           def.ConstraintSymbol,
			Cols:           make([]uint64, len(def.KeyParts)),
			OnDelete:       getRefAction(refer.OnDelete),
			OnUpdate:       getRefAction(refer.OnUpdate),
			ForeignCols:    make([]uint64, len(refer.KeyParts)),
			OnDeleteOrigin: foreignKeyActionOrigin(refer.OnDelete),
			OnUpdateOrigin: foreignKeyActionOrigin(refer.OnUpdate),
		},
		catalogLayout: catalogLayout,
	}

	// get fk columns of create table
	fkData.Cols = &plan.FkColName{
		Cols: make([]string, len(def.KeyParts)),
	}
	fkData.ColTyps = make(map[int]*plan.Type)
	name2ColDef := make(map[string]*ColDef)
	for _, colDef := range tableDef.Cols {
		name2ColDef[colDef.Name] = colDef
	}
	// get the column (id,name,type) from tableDef for the foreign key
	for i, keyPart := range def.KeyParts {
		colName := keyPart.ColName.ColName()
		if colDef, has := name2ColDef[colName]; has {
			// column id from tableDef
			fkData.Def.Cols[i] = colDef.ColId
			// column name from tableDef
			fkData.Cols.Cols[i] = colDef.Name
			// column type from tableDef
			fkData.ColTyps[i] = &colDef.Typ
		} else {
			return nil, moerr.NewBadFieldErrorf(ctx.GetContext(), "internal error: column '%v' no exists in the creating table '%v'", keyPart.ColName.ColNameOrigin(), tableDef.Name)
		}
	}

	fkData.ColsReferred = &plan.FkColName{
		Cols: make([]string, len(refer.KeyParts)),
	}
	for i, part := range refer.KeyParts {
		fkData.ColsReferred.Cols[i] = part.ColName.ColName()
	}

	// get foreign table & their columns
	parentTableName := string(refer.TableName.ObjectName)
	parentDbName := string(refer.TableName.SchemaName)
	if parentDbName == "" {
		parentDbName = ctx.DefaultDatabase()
	}

	if IsFkBannedDatabase(parentDbName) {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not refer foreign keys in %s", parentDbName)
	}

	// foreign key reference to itself
	if IsFkSelfRefer(parentDbName, parentTableName, dbName, tableDef.Name) {
		// should be handled later for fk self reference
		// PK and unique key may not be processed now
		// check fk columns can not reference to themselves
		// In self refer, the parent table is the table itself
		parentColumnsMap := make(map[string]int8)
		for _, part := range refer.KeyParts {
			parentColumnsMap[part.ColName.ColName()] = 0
		}
		for _, name := range fkData.Cols.Cols {
			if _, ok := parentColumnsMap[name]; ok {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "foreign key %s can not reference to itself", name)
			}
		}
		// for fk self refer. column id may be not ready.
		fkData.IsSelfRefer = true
		fkData.ParentDbName = parentDbName
		fkData.ParentTableName = parentTableName
		fkData.Def.ForeignTbl = 0
		return &fkData, nil
	}

	fkData.ParentDbName = parentDbName
	fkData.ParentTableName = parentTableName

	_, parentTableDef, err := ctx.Resolve(parentDbName, parentTableName, nil)
	if err != nil {
		return nil, err
	}
	if err := validateTableIndexDefinitions(parentTableDef); err != nil {
		return nil, err
	}
	if parentTableDef == nil {
		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if !enabled {
			fkData.ForwardRefer = true
			// There is no parent key to bind yet, but the catalog row is the
			// durable record that lets the later parent CREATE reconcile this
			// forward reference. Its referenced_index_name is backfilled then.
			fkData.UpdateSql = getSqlForAddFkWithCatalogLayout(dbName, tableDef.Name, &fkData, fkData.catalogLayout)
			return &fkData, nil
		}
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), ctx.DefaultDatabase(), parentTableName)
	}

	if parentTableDef.IsTemporary {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "add foreign key for temporary table")
	}

	fkData.Def.ForeignTbl = parentTableDef.TblId

	// separate the rest of the logic in previous version
	// into an independent function checkFkColsAreValid
	// for reusing it in fk self refer that checks the
	// columns in fk definition are valid or not.
	if err := checkFkColsAreValid(ctx, &fkData, parentTableDef); err != nil {
		return nil, err
	}
	fkData.UpdateSql = getSqlForAddFkWithCatalogLayout(dbName, tableDef.Name, &fkData, fkData.catalogLayout)

	return &fkData, nil
}

func foreignKeyActionOrigin(option tree.ReferenceOptionType) plan.ForeignKeyDef_RefActionOrigin {
	if option == tree.REFERENCE_OPTION_INVALID {
		return plan.ForeignKeyDef_ACTION_ORIGIN_DEFAULT
	}
	return plan.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT
}

type foreignKeyReferencedKey struct {
	name    string
	columns []string
}

// selectForeignKeyReferencedIndex applies the one binding contract shared by
// FK creation and by DDL lifecycle checks. A reference may use an ordered
// leading prefix of a PRIMARY/UNIQUE key. PRIMARY wins ties; otherwise the
// lexicographically first named UNIQUE key wins.
func selectForeignKeyReferencedIndex(parentTableDef *TableDef, referredColumns []string) (string, bool) {
	keys := make([]foreignKeyReferencedKey, 0, len(parentTableDef.Indexes)+1)
	if parentTableDef.Pkey != nil {
		keys = append(keys, foreignKeyReferencedKey{name: "PRIMARY", columns: parentTableDef.Pkey.Names})
	}
	for _, index := range parentTableDef.Indexes {
		if index.Unique {
			keys = append(keys, foreignKeyReferencedKey{name: index.IndexName, columns: index.Parts})
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i].name == "PRIMARY" || keys[j].name == "PRIMARY" {
			return keys[i].name == "PRIMARY"
		}
		return keys[i].name < keys[j].name
	})

	for _, key := range keys {
		if foreignKeyReferencedColumnsMatch(key.columns, referredColumns) {
			return key.name, true
		}
	}
	return "", false
}

func foreignKeyReferencedColumnsMatch(keyColumns, referredColumns []string) bool {
	if len(keyColumns) < len(referredColumns) {
		return false
	}
	for i, column := range referredColumns {
		if keyColumns[i] != column {
			return false
		}
	}
	return true
}

/*
checkFkColsAreValid check foreign key columns is valid or not, then it saves them.
the columns referred by the foreign key in the children table must appear in the unique keys or primary key
in the parent table.

For instance:
create table f1 (a int ,b int, c int ,d int ,e int,

	primary key(a,b),  unique key(c,d), unique key (e))

The referenced columns must be a leading prefix of one PRIMARY or UNIQUE key,
in the same order. With PRIMARY KEY(a, b), both (a) and (a, b) are valid, but
(b) and (b, a) are not.

When more than one key has the same prefix, PRIMARY is selected first;
otherwise the lexicographically first named UNIQUE key is selected. Persisting
this selected name makes information_schema.REFERENTIAL_CONSTRAINTS deterministic.
*/
func checkFkColsAreValid(ctx CompilerContext, fkData *FkData, parentTableDef *TableDef) error {
	// colId in parent table-> position in parent table
	columnIdPos := make(map[uint64]int)
	// columnName in parent table -> position in parent table
	columnNamePos := make(map[string]int)
	// 1. collect parent column info
	for i, col := range parentTableDef.Cols {
		columnIdPos[col.ColId] = i
		columnNamePos[col.Name] = i
	}

	// 2. check if the referred column does not exist in the parent table
	for _, colName := range fkData.ColsReferred.Cols {
		if _, exists := columnNamePos[colName]; !exists { // column exists in parent table
			return moerr.NewBadFieldErrorf(ctx.GetContext(), "internal error: column '%v' no exists in table '%v'", colName, fkData.ParentTableName)
		}
	}
	if err := checkFkVirtualGeneratedColumns(ctx.GetContext(), parentTableDef, fkData.ColsReferred.Cols); err != nil {
		return err
	}

	indexName, matched := selectForeignKeyReferencedIndex(parentTableDef, fkData.ColsReferred.Cols)
	if !matched {
		return moerr.NewInternalError(ctx.GetContext(), "failed to add the foreign key constraint")
	}

	matchCols := make([]uint64, len(fkData.ColsReferred.Cols))
	for i, referredColName := range fkData.ColsReferred.Cols {
		colID := parentTableDef.Cols[columnNamePos[referredColName]].ColId
		if parentTableDef.Cols[columnIdPos[colID]].Typ.Id != fkData.ColTyps[i].Id {
			return moerr.NewInternalErrorf(ctx.GetContext(), "type of reference column '%v' is not match for column '%v'", referredColName, fkData.Cols.Cols[i])
		}
		matchCols[i] = colID
	}
	fkData.Def.ForeignCols = matchCols
	fkData.Def.ReferencedIndexName = indexName
	return nil
}

func checkFkVirtualGeneratedColumns(ctx context.Context, parentTableDef *TableDef, referredCols []string) error {
	for _, colName := range referredCols {
		colDef := FindColumn(parentTableDef.Cols, colName)
		if colDef == nil || colDef.GeneratedCol == nil || colDef.GeneratedCol.IsStored {
			continue
		}
		return moerr.NewInvalidInputf(ctx,
			"foreign key cannot reference virtual generated column '%s'",
			colDef.GetOriginCaseName())
	}
	return nil
}

// buildFkDataOfForwardRefer rebuilds the fk relationships based on
// the mo_catalog.mo_foreign_keys.
func buildFkDataOfForwardRefer(ctx CompilerContext,
	constraintName string,
	fkDefs []*FkReferDef,
	createTable *plan.CreateTable) (*FkData, error) {
	fkData := FkData{
		Def: &plan.ForeignKeyDef{
			Name:                constraintName,
			Cols:                make([]uint64, len(fkDefs)),
			OnDelete:            convertIntoReferAction(fkDefs[0].OnDelete),
			OnUpdate:            convertIntoReferAction(fkDefs[0].OnUpdate),
			ForeignCols:         make([]uint64, len(fkDefs)),
			ReferencedIndexName: fkDefs[0].ReferencedIndexName,
			OnDeleteOrigin:      convertIntoReferActionOrigin(fkDefs[0].OnDeleteOrigin),
			OnUpdateOrigin:      convertIntoReferActionOrigin(fkDefs[0].OnUpdateOrigin),
		},
	}
	// 1. get tableDef of the child table
	_, childTableDef, err := ctx.Resolve(fkDefs[0].Db, fkDefs[0].Tbl, nil)
	if err != nil {
		return nil, err
	}
	if childTableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), fkDefs[0].Db, fkDefs[0].Tbl)
	}
	// 2. fill fkdata
	fkData.Cols = &plan.FkColName{
		Cols: make([]string, len(fkDefs)),
	}
	fkData.ColTyps = make(map[int]*plan.Type)

	name2ColDef := make(map[string]*ColDef)
	for _, def := range childTableDef.Cols {
		name2ColDef[def.Name] = def
	}
	for i, fkDef := range fkDefs {
		if colDef, has := name2ColDef[fkDef.Col]; has {
			// column id from tableDef
			fkData.Def.Cols[i] = colDef.ColId
			// column name from tableDef
			fkData.Cols.Cols[i] = colDef.Name
			// column type from tableDef
			fkData.ColTyps[i] = &colDef.Typ
		} else {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' no exists in table '%v'", fkDef.Col, fkDefs[0].Tbl)
		}
	}

	fkData.ColsReferred = &plan.FkColName{
		Cols: make([]string, len(fkDefs)),
	}
	for i, def := range fkDefs {
		fkData.ColsReferred.Cols[i] = def.ReferCol
	}

	// 3. check fk valid or not
	if err := checkFkColsAreValid(ctx, &fkData, createTable.TableDef); err != nil {
		return nil, err
	}
	return &fkData, nil
}

func getAutoIncrementOffsetFromVariables(ctx CompilerContext) (uint64, bool) {
	v, err := ctx.ResolveVariable("auto_increment_offset", true, false)
	if err == nil {
		if offset, ok := v.(int64); ok && offset > 1 {
			return uint64(offset - 1), true
		}
	}
	return 0, false
}

var unitDurations = map[string]time.Duration{
	"second": time.Second,
	"minute": time.Minute,
	"hour":   time.Hour,
	"day":    time.Hour * 24,
	"week":   time.Hour * 24 * 7,
	"month":  time.Hour * 24 * 30,
}

func parseDuration(ctx context.Context, period uint64, unit string) (time.Duration, error) {
	unitDuration, ok := unitDurations[strings.ToLower(unit)]
	if !ok {
		return 0, moerr.NewInvalidArg(ctx, "time unit", unit)
	}
	seconds := period * uint64(unitDuration)
	return time.Duration(seconds), nil
}

func buildCreatePitr(stmt *tree.CreatePitr, ctx CompilerContext) (*Plan, error) {
	// only sys can create cluster level pitr
	currentAccount := ctx.GetAccountName()
	currentAccountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if stmt.Level == tree.PITRLEVELCLUSTER && currentAccount != "sys" {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only sys tenant can create cluster level pitr")
	}

	// only sys can create tenant level pitr for other tenant
	if stmt.Level == tree.PITRLEVELACCOUNT {
		if len(stmt.AccountName) > 0 && currentAccount != "sys" {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only sys tenant can create tenant level pitr for other tenant")
		}
	}

	// Check PITR value range
	pitrVal := stmt.PitrValue
	if pitrVal <= 0 || pitrVal > 100 {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "invalid pitr value %d", pitrVal)
	}

	// Check if PITR unit is valid
	pitrUnit := strings.ToLower(stmt.PitrUnit)
	if pitrUnit != "h" && pitrUnit != "d" && pitrUnit != "mo" && pitrUnit != "y" {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "invalid pitr unit %s", pitrUnit)
	}

	// check pitr exists or not
	if string(stmt.Name) == SYSMOCATALOGPITR {
		return nil, moerr.NewInternalError(ctx.GetContext(), "pitr name is reserved")
	}

	// Validate related objects according to PITR level
	var databaseId uint64
	var tableId uint64
	accountId := currentAccountId
	accountName := currentAccount
	switch stmt.Level {
	case tree.PITRLEVELACCOUNT:
		if len(stmt.AccountName) > 0 {
			accountIds, err := ctx.ResolveAccountIds([]string{string(stmt.AccountName)})
			if err != nil {
				return nil, err
			}
			if len(accountIds) == 0 {
				return nil, moerr.NewInternalError(ctx.GetContext(), "account "+string(stmt.AccountName)+" does not exist")
			}
			accountId = accountIds[len(accountIds)-1]
			accountName = string(stmt.AccountName)
		}
	case tree.PITRLEVELDATABASE:
		if !ctx.DatabaseExists(string(stmt.DatabaseName), nil) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "database "+string(stmt.DatabaseName)+" does not exist")
		}
		databaseId, err = ctx.GetDatabaseId(string(stmt.DatabaseName), nil)
		if err != nil {
			return nil, err
		}
	case tree.PITRLEVELTABLE:
		if !ctx.DatabaseExists(string(stmt.DatabaseName), nil) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "database "+string(stmt.DatabaseName)+" does not exist")
		}
		objRef, tableDef, err := ctx.Resolve(string(stmt.DatabaseName), string(stmt.TableName), nil)
		if err != nil {
			return nil, err
		}
		if objRef == nil || tableDef == nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "table "+string(stmt.DatabaseName)+"."+string(stmt.TableName)+" does not exist")
		}
		tableId = tableDef.TblId
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_PITR,
				Definition: &plan.DataDefinition_CreatePitr{
					CreatePitr: &plan.CreatePitr{
						IfNotExists:       stmt.IfNotExists,
						Name:              string(stmt.Name),
						Level:             int32(stmt.Level),
						AccountName:       accountName,
						DatabaseName:      string(stmt.DatabaseName),
						TableName:         string(stmt.TableName),
						PitrValue:         stmt.PitrValue,
						PitrUnit:          stmt.PitrUnit,
						DatabaseId:        databaseId,
						TableId:           tableId,
						AccountId:         accountId,
						CurrentAccountId:  currentAccountId,
						CurrentAccount:    currentAccount,
						OriginAccountName: len(stmt.AccountName) > 0,
					},
				},
			},
		},
	}, nil
}

func buildDropPitr(stmt *tree.DropPitr, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_DROP_PITR
	// Remove privilege check, no account ID validation

	// Build drop pitr plan
	dropPitr := &plan.DropPitr{
		IfExists: stmt.IfExists,
		Name:     string(stmt.Name),
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: ddlType,
				Definition: &plan.DataDefinition_DropPitr{
					DropPitr: dropPitr,
				},
			},
		},
	}, nil
}

func buildCreateCDC(stmt *tree.CreateCDC, ctx CompilerContext) (*Plan, error) {
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_CDC,
				Definition: &plan.DataDefinition_CreateCdc{
					CreateCdc: &plan.CreateCDC{
						IfNotExists: stmt.IfNotExists,
						TaskName:    string(stmt.TaskName),
						SourceUri:   stmt.SourceUri,
						SinkType:    stmt.SinkType,
						SinkUri:     stmt.SinkUri,
						Tables:      stmt.Tables,
						Option:      stmt.Option,
						UserName:    ctx.GetUserName(),
						AccountName: ctx.GetAccountName(),
						AccountId:   accountId,
					},
				},
			},
		},
	}, nil
}

func buildDropCDC(stmt *tree.DropCDC, ctx CompilerContext) (*Plan, error) {
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_CDC,
				Definition: &plan.DataDefinition_DropCdc{
					DropCdc: &plan.DropCDC{
						IfExists:  stmt.IfExists,
						AccountId: accountId,
						All:       stmt.Option.All,
						TaskName:  string(stmt.Option.TaskName),
					},
				},
			},
		},
	}, nil
}

func constructAddedPartitionDefs(
	ctx CompilerContext,
	tableDef *plan.TableDef,
	clause *tree.AlterPartitionAddPartitionClause,
) ([]*plan.PartitionDef, error) {
	originTableStmt, err := parsers.ParseOneWithSQLMode(
		ctx.GetContext(),
		dialect.MYSQL,
		tableDef.Createsql,
		ctx.GetLowerCaseTableNames(),
		"PIPES_AS_CONCAT",
	)
	if err != nil {
		return nil, err
	}
	defer originTableStmt.Free()

	ct, ok := originTableStmt.(*tree.CreateTable)
	if !ok {
		return nil, moerr.NewNotSupportedNoCtx("unsupported ADD PARTITION not in create table")
	}
	if ct == nil || ct.PartitionOption == nil || ct.PartitionOption.PartBy == nil {
		return nil, moerr.NewNotSupportedNoCtx("Partition management on a not partitioned table is not possible")
	}

	switch ct.PartitionOption.PartBy.PType.(type) {
	case *tree.RangeType, *tree.ListType:
		originParts := ct.PartitionOption.Partitions
		newParts := clause.Partitions
		if len(newParts) == 0 {
			return nil, nil
		}

		merged := make([]*tree.Partition, 0, len(originParts)+len(newParts))
		merged = append(merged, originParts...)
		merged = append(merged, newParts...)

		combined := tree.NewPartitionOption(
			ct.PartitionOption.PartBy,
			ct.PartitionOption.SubPartBy,
			merged,
		)

		partBuilder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
		partBindCtx := NewBindContext(partBuilder, nil)
		nodeID := partBuilder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Stats:       nil,
			ObjRef:      nil,
			TableDef:    tableDef,
			BindingTags: []int32{partBuilder.genNewBindTag()},
		}, partBindCtx)
		if err := partBuilder.addBinding(nodeID, tree.AliasClause{}, partBindCtx); err != nil {
			return nil, err
		}
		partitionBinder := NewPartitionBinder(partBuilder, partBindCtx)
		allDefs, err := partitionBinder.buildPartitionDefs(ctx.GetContext(), combined)
		if err != nil {
			return nil, err
		}
		originCnt := len(originParts)
		if originCnt > len(allDefs.PartitionDefs) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "invalid partition definition state")
		}
		return allDefs.PartitionDefs[originCnt:], nil
	default:
		return nil, moerr.NewNotSupportedNoCtx("unsupported partition method in ADD PARTITION")
	}
}

// validateWriteFilePattern validates the WRITE_FILE_PATTERN option that makes an
// external table writable, plus the column restrictions writability implies.
// No-op for read-only external tables (option absent). tableDef may be nil when
// only the param-level options need checking.
// effectiveWriteCompression mirrors crt.GetCompressType's decision (inlined to
// avoid the plan<-crt import cycle): an explicit non-auto compression wins,
// otherwise the type is auto-detected from any of the given file paths'
// suffixes. Returns the effective type and whether it is compressed.
func effectiveWriteCompression(comp string, paths ...string) (string, bool) {
	comp = strings.ToLower(strings.TrimSpace(comp))
	if comp != "" && comp != tree.AUTO {
		return comp, comp != tree.NOCOMPRESS
	}
	suffixes := []string{".tar.gz", ".tar.gzip", ".tar.bz2", ".tar.bzip2", ".gz", ".gzip", ".bz2", ".bzip2", ".lz4"}
	for _, p := range paths {
		p = strings.ToLower(p)
		for _, suf := range suffixes {
			if strings.HasSuffix(p, suf) {
				return strings.TrimPrefix(suf, "."), true
			}
		}
	}
	return tree.NOCOMPRESS, false
}

func validateWriteFilePattern(ctx context.Context, param *tree.ExternParam, tableDef *TableDef) error {
	pattern, ok := GetWriteFilePattern(param)
	if !ok {
		return nil
	}
	if !strings.HasPrefix(pattern, "stage://") {
		return moerr.NewBadConfigf(ctx, "WRITE_FILE_PATTERN must be a stage:// path, got '%s'", pattern)
	}
	// Duplicate option keys would let validation inspect a different value than
	// the one the read-side init later keeps (it walks the whole slice, last
	// wins) — so a table could validate as csv and run as parquet.
	if err := rejectDuplicateKeys(ctx, param.Option,
		[]string{"format", "jsondata", "compression", "filepath", ExternalWriteFilePatternKey}); err != nil {
		return err
	}
	// The writer streams plain bytes; the read path decompresses based on the
	// COMPRESSION option (or, when unset/auto, the file suffix), so any
	// effective compression — from the option, the read FILEPATH glob, or the
	// write pattern itself — would make the produced files unreadable.
	comp := param.CompressType
	if comp == "" {
		comp = getRawOption(param.Option, "compression")
	}
	if eff, compressed := effectiveWriteCompression(comp, getRawOption(param.Option, "filepath"), pattern); compressed {
		return moerr.NewBadConfigf(ctx, "writable external table does not support compression (effective '%s'); the writer emits uncompressed files", eff)
	}
	format := strings.ToLower(param.Format)
	if format == "" {
		format = strings.ToLower(getRawOption(param.Option, "format"))
	}
	if format != tree.CSV && format != tree.JSONLINE {
		return moerr.NewBadConfigf(ctx, "writable external table only supports csv and jsonline formats, got '%s'", format)
	}
	if format == tree.JSONLINE {
		jsondata := strings.ToLower(param.JsonData)
		if jsondata == "" {
			jsondata = strings.ToLower(getRawOption(param.Option, "jsondata"))
		}
		// The writer emits one JSON object per line; jsondata='array' tables would
		// not be able to read back their own output.
		if jsondata == tree.ARRAY {
			return moerr.NewBadConfig(ctx, "writable external table does not support jsondata 'array', use 'object'")
		}
		// JSON strings have no enclosure mechanism: a printable line terminator
		// occurring inside a value would split the record on readback. \n and
		// \r\n are safe because the JSON encoder \u-escapes control characters.
		if param.Tail != nil && param.Tail.Lines != nil && param.Tail.Lines.TerminatedBy != nil {
			if v := param.Tail.Lines.TerminatedBy.Value; v != "" && v != "\n" && v != "\r\n" {
				return moerr.NewBadConfig(ctx, "writable external table with format 'jsonline' only supports LINES TERMINATED BY '\\n' or '\\r\\n'")
			}
		}
		// COMMENT is unsafe for jsonline: the reader matches the marker against the
		// raw line prefix before LINES STARTING BY is consumed, but every jsonline
		// record deterministically begins with LINES STARTING BY + '{' and JSON has
		// no enclosure mechanism to hide it. A marker such as '{' would skip every
		// row the writer produced, so reject COMMENT on writable jsonline tables.
		if GetCSVComment(param) != "" {
			return moerr.NewBadConfig(ctx, "writable external table with format 'jsonline' does not support the COMMENT option")
		}
	}
	if format == tree.CSV {
		if err := validateWritableComment(ctx, param); err != nil {
			return err
		}
	}
	// Dry-run the pattern against a fixed timestamp to reject bad directives at DDL time.
	if _, err := externalwrite.ExpandFilePattern(pattern, time.Unix(0, 0).UTC()); err != nil {
		return err
	}
	// Every parallel pipeline owns one writer and expands the pattern against the
	// same statement timestamp, so without a per-writer-unique directive all
	// pipelines would open the identical path and clobber each other.
	if !externalwrite.PatternHasUniqueDirective(pattern) {
		return moerr.NewBadConfigf(ctx, "WRITE_FILE_PATTERN must contain a %%U or %%<n>N directive so parallel writers produce distinct files, got '%s'", pattern)
	}
	// Reject FIELDS/LINES combinations the writer cannot make round-trip.
	if param.Tail != nil {
		if err := validateWritableEscape(ctx, param.Tail); err != nil {
			return err
		}
		// The reader skips IGNORE N LINES per file, but the writer emits no
		// header lines, so real data rows would be discarded on readback.
		if param.Tail.IgnoredLines > 0 {
			return moerr.NewBadConfig(ctx, "writable external table does not support IGNORE ... LINES")
		}
	}
	if tableDef != nil {
		for _, col := range tableDef.Cols {
			// Hidden/synthetic columns (e.g. the fake-PK column added to tables
			// without a primary key) are never written to the output file.
			if col.Hidden {
				continue
			}
			// AUTO_INCREMENT values are generated by the PreInsert operator, which
			// the minimal external-insert plan does not run.
			if col.Typ.AutoIncr {
				return moerr.NewBadConfigf(ctx, "writable external table does not support AUTO_INCREMENT column '%s'", col.Name)
			}
			// Generated columns are recomputed (and explicit writes rejected) only
			// by the normal insert/load binders. The external INSERT/LOAD path uses
			// the minimal legacy builder, which neither filters nor recomputes them,
			// so a generated column would store arbitrary or NULL/default values.
			if col.GeneratedCol != nil {
				return moerr.NewBadConfigf(ctx, "writable external table does not support generated column '%s'", col.Name)
			}
			// Binary payloads cannot round-trip through JSON strings: bit bytes
			// >= 0x80 are invalid UTF-8, and binary/varbinary/blob would need a
			// base64 encoding the jsonline READER does not decode.
			if format == tree.JSONLINE {
				switch types.T(col.Typ.Id) {
				case types.T_bit, types.T_binary, types.T_varbinary, types.T_blob:
					return moerr.NewBadConfigf(ctx, "writable external table with format 'jsonline' does not support %s column '%s'",
						strings.ToLower(types.T(col.Typ.Id).String()), col.Name)
				}
			}
		}
	}
	return nil
}

// validateWritableComment rejects CSV COMMENT markers the writer cannot make
// round-trip. The reader skips a line whose RAW prefix (before unquoting)
// matches the marker, so the writer must never produce such a line. The writer
// guards a non-NULL, unenclosed first field by enclosing it (the line then
// begins with the enclosure byte), but three structural cases cannot be guarded
// that way and are rejected here:
//
//  1. LINES STARTING BY: the marker is matched on the raw prefix BEFORE the
//     starting-by prefix is consumed, so a marker contained in that fixed prefix
//     (e.g. COMMENT 'REM' with LINES STARTING BY 'REM:') skips every row. COMMENT
//     and LINES STARTING BY are therefore mutually exclusive for writable tables.
//  2. The enclosure byte: a first field that must be enclosed (or the writer's
//     own guard) makes the line begin with the enclosure byte, so a marker that
//     begins with it would skip those rows. Enclosing cannot help — it is the
//     collision. Reject a marker whose first byte is the enclosure byte.
//  3. The escape byte: the writer escapes the (unenclosed) first field, so a
//     value can be written starting with the escape byte (e.g. a doubled escape),
//     which a marker beginning with that byte would skip. Reject it too.
//  4. The field terminator: an empty first field makes the line begin with the
//     terminator, so a marker beginning with it would skip such rows. Reject it.
//  5. The NULL sentinel: a NULL first column is written verbatim as `\N` (it
//     cannot be enclosed without reading back as the string), so a marker that is
//     a prefix of `\N` (or vice-versa) skips every row with a leading NULL. The
//     sentinel uses a literal backslash regardless of the configured escape, so
//     this is checked independently of rule 3.
func validateWritableComment(ctx context.Context, param *tree.ExternParam) error {
	comment := GetCSVComment(param)
	if comment == "" {
		return nil
	}
	var fields *tree.Fields
	if param.Tail != nil {
		if param.Tail.Lines != nil && param.Tail.Lines.StartingBy != "" {
			return moerr.NewBadConfig(ctx, "writable external table does not support COMMENT together with LINES STARTING BY")
		}
		fields = param.Tail.Fields
	}
	enclosed := byte('"')
	if fields != nil && fields.EnclosedBy != nil && fields.EnclosedBy.Value != 0 {
		enclosed = fields.EnclosedBy.Value
	}
	if comment[0] == enclosed {
		return moerr.NewBadConfigf(ctx, "writable external table COMMENT must not start with the enclosure byte '%c'", enclosed)
	}
	// Escape byte: default '\\'; an explicit empty FIELDS ESCAPED BY disables it.
	escape := byte('\\')
	if fields != nil && fields.EscapedBy != nil {
		escape = fields.EscapedBy.Value // 0 means escaping disabled
	}
	if escape != 0 && comment[0] == escape {
		return moerr.NewBadConfigf(ctx, "writable external table COMMENT must not start with the escape byte '%c'", escape)
	}
	// Field terminator: default ','; an empty first field starts the line with it.
	fieldTerm := ","
	if fields != nil && fields.Terminated != nil && fields.Terminated.Value != "" {
		fieldTerm = fields.Terminated.Value
	}
	if comment[0] == fieldTerm[0] {
		return moerr.NewBadConfigf(ctx, "writable external table COMMENT must not start with the field terminator byte '%c'", fieldTerm[0])
	}
	csvNull := `\N`
	if strings.HasPrefix(comment, csvNull) || strings.HasPrefix(csvNull, comment) {
		return moerr.NewBadConfig(ctx, "writable external table COMMENT must not collide with the NULL sentinel \\N")
	}
	return nil
}

// validateWritableEscape checks that the FIELDS/LINES configuration can
// round-trip through the writer + reader pair.
//
// Escape: the writer escapes by doubling the character, and the reader
// unescapes E-sequences in BOTH quoted and unquoted fields, so a custom escape
// must not collide with bytes the reader treats specially. An empty FIELDS
// ESCAPED BY
// (escaping disabled) is allowed; the writer disables escaping too. Note: with
// any non-'\\' escape (including disabled), a string whose content is exactly
// `\N` reads back as NULL — the reader matches the null sentinel after
// unescaping and only exempts it for the default backslash.
//
// Enclosure: values containing structural bytes are written enclosed
// (OPTIONALLY ENCLOSED semantics), which requires the enclosure byte itself
// to be distinguishable from the terminators — no quoting discipline can fix
// an enclosure byte that also begins a field or record boundary.
func validateWritableEscape(ctx context.Context, tail *tree.TailParameter) error {
	f := tail.Fields

	fieldTerm := ","
	enclosed := byte('"')
	var esc byte = '\\'
	if f != nil {
		if f.Terminated != nil && f.Terminated.Value != "" {
			fieldTerm = f.Terminated.Value
		}
		if f.EnclosedBy != nil && f.EnclosedBy.Value != 0 {
			enclosed = f.EnclosedBy.Value
		}
		if f.EscapedBy != nil {
			esc = f.EscapedBy.Value // 0 = escaping disabled
		}
	}
	lineTerm := "\n"
	startingBy := ""
	if l := tail.Lines; l != nil {
		if l.TerminatedBy != nil && l.TerminatedBy.Value != "" {
			lineTerm = l.TerminatedBy.Value
		}
		startingBy = l.StartingBy
	}

	// The CSV reader rejects a field terminator whose first byte is a quote,
	// CR, LF or NUL (csvparser.validDelim / NewCSVParser), so such a table
	// could be created and written but never read back.
	if b := fieldTerm[0]; b == 0 || b == '"' || b == '\r' || b == '\n' {
		return moerr.NewBadConfig(ctx, "writable external table FIELDS TERMINATED BY cannot start with a quote, CR, LF or NUL byte")
	}

	// The escape/enclosure conflict applies to the DEFAULT backslash escape
	// too: ENCLOSED BY '\\' with the default escape makes the tokenizer's
	// doubled-delimiter collapse and the unescaper both consume the same
	// bytes, corrupting values on readback.
	if esc != 0 && esc == enclosed {
		return moerr.NewBadConfigf(ctx, "writable external table cannot use FIELDS ESCAPED BY '%c': it conflicts with the enclosure character", esc)
	}
	if esc != 0 && esc != '\\' {
		// The reader's unescaper maps E+{0,b,n,r,t,Z} to control characters, so a
		// doubled escape (E E) would decode to a control char instead of E itself.
		if strings.IndexByte("0bnrtZ", esc) >= 0 {
			return moerr.NewBadConfigf(ctx, "writable external table cannot use FIELDS ESCAPED BY '%c': the reader maps '%c'-sequences to control characters", esc, esc)
		}
		// Control bytes as the escape would collide with the writer's own
		// E+'r' CR encoding and the reader's record handling.
		if esc < 0x20 || esc == 0x7f {
			return moerr.NewBadConfig(ctx, "writable external table cannot use a control character as FIELDS ESCAPED BY")
		}
		for _, s := range []string{fieldTerm, lineTerm, startingBy} {
			if strings.IndexByte(s, esc) >= 0 {
				return moerr.NewBadConfigf(ctx, "writable external table cannot use FIELDS ESCAPED BY '%c': it occurs in a field/line terminator or LINES STARTING BY", esc)
			}
		}
	}

	// The writer encloses values containing structural bytes; the enclosure
	// byte must not itself be part of a terminator or the record prefix.
	for _, s := range []string{fieldTerm, lineTerm, startingBy} {
		if strings.IndexByte(s, enclosed) >= 0 {
			return moerr.NewBadConfigf(ctx, "writable external table cannot use ENCLOSED BY '%c': it occurs in a field/line terminator or LINES STARTING BY", enclosed)
		}
	}
	return nil
}

// validateAndSetHivePartitionOptions parses and validates hive_partitioning options from the DDL,
// normalizes partition column names, extracts column types, and strips hive keys from Option[].
func validateAndSetHivePartitionOptions(ctx context.Context, stmt *tree.CreateTable, createTable *plan.CreateTable) error {
	raw := stmt.Param.Option

	if err := rejectDuplicateKeys(ctx, raw, []string{"hive_partitioning", "hive_partition_columns"}); err != nil {
		return err
	}

	hiveEnabled, hiveCols, err := parseHiveOptionsFromRawOptions(ctx, raw)
	if err != nil {
		return err
	}
	if !hiveEnabled {
		return nil
	}

	if err := rejectDuplicateKeys(ctx, raw, []string{"format", "filepath"}); err != nil {
		return err
	}

	rawFormat := strings.ToLower(getRawOption(raw, "format"))
	if rawFormat != "parquet" {
		return moerr.NewBadConfigf(ctx, "hive_partitioning currently only supports format='parquet', got '%s'", rawFormat)
	}

	rawFilepath := getRawOption(raw, "filepath")
	if len(stmt.Param.StageName) != 0 || strings.HasPrefix(rawFilepath, "stage://") {
		return moerr.NewBadConfig(ctx, "hive_partitioning does not support stage external tables")
	}

	if len(hiveCols) == 0 || (len(hiveCols) == 1 && strings.EqualFold(strings.TrimSpace(hiveCols[0]), "auto")) {
		prepareHiveInferenceParam(stmt.Param, raw)
		hiveCols, err = inferHivePartitionColumns(ctx, stmt.Param)
		if err != nil {
			return err
		}
	}

	normalized := make([]string, 0, len(hiveCols))
	colTypes := make([]tree.HivePartColType, 0, len(hiveCols))
	seen := make(map[string]bool)
	for _, pc := range hiveCols {
		col := findColInTableDefCaseInsensitive(createTable.TableDef.Cols, pc)
		if col == nil {
			return moerr.NewBadConfigf(ctx, "partition column '%s' not found in table columns", pc)
		}
		if col.Hidden {
			return moerr.NewBadConfigf(ctx, "partition column '%s' cannot be a hidden column", pc)
		}
		if col.GeneratedCol != nil {
			return moerr.NewBadConfigf(ctx, "partition column '%s' cannot be a generated column", pc)
		}
		typId := types.T(col.Typ.Id)
		if typId.IsArrayRelate() {
			// IsArrayRelate covers all six vector types: a vector can never
			// round-trip through a `col=value` path component, so the rejection
			// applies to the narrow types exactly as it does to vecf32/vecf64.
			return moerr.NewBadConfigf(ctx, "partition column '%s' cannot be a VECTOR type", pc)
		}
		canonical := strings.ToLower(col.Name)
		if seen[canonical] {
			return moerr.NewBadConfigf(ctx, "duplicate partition column '%s'", pc)
		}
		seen[canonical] = true
		normalized = append(normalized, canonical)

		nullable := true
		if col.Default != nil {
			nullable = col.Default.NullAbility
		}
		colTypes = append(colTypes, tree.HivePartColType{
			Id:          col.Typ.Id,
			Width:       col.Typ.Width,
			Scale:       col.Typ.Scale,
			Enumvalues:  col.Typ.Enumvalues,
			Charset:     col.Typ.Charset,
			NullAbility: nullable,
		})
	}

	stmt.Param.HivePartitioning = true
	stmt.Param.HivePartitionCols = normalized
	stmt.Param.HivePartitionColTypes = colTypes
	stmt.Param.Option = stripHiveOptionKeys(stmt.Param.Option)
	return nil
}

func prepareHiveInferenceParam(param *tree.ExternParam, options []string) {
	if param.Filepath == "" {
		param.Filepath = getRawOption(options, "filepath")
	}
	if param.Format == "" {
		param.Format = strings.ToLower(getRawOption(options, "format"))
	}
	if param.ScanType != tree.S3 {
		return
	}
	if param.S3Param == nil {
		param.S3Param = &tree.S3Parameter{}
	}
	if param.S3Param.Endpoint == "" {
		param.S3Param.Endpoint = getRawOption(options, "endpoint")
	}
	if param.S3Param.Region == "" {
		param.S3Param.Region = getRawOption(options, "region")
	}
	if param.S3Param.APIKey == "" {
		param.S3Param.APIKey = getRawOption(options, "access_key_id")
	}
	if param.S3Param.APISecret == "" {
		param.S3Param.APISecret = getRawOption(options, "secret_access_key")
	}
	if param.S3Param.Bucket == "" {
		param.S3Param.Bucket = getRawOption(options, "bucket")
	}
	if param.S3Param.Provider == "" {
		param.S3Param.Provider = getRawOption(options, "provider")
	}
	if param.S3Param.RoleArn == "" {
		param.S3Param.RoleArn = getRawOption(options, "role_arn")
	}
	if param.S3Param.ExternalId == "" {
		param.S3Param.ExternalId = getRawOption(options, "external_id")
	}
}

const (
	hivePartitionInferMaxDepth      = 16
	hivePartitionInferMaxListCalls  = 64
	hivePartitionInferMaxSampleDirs = 64
)

func inferHivePartitionColumns(ctx context.Context, param *tree.ExternParam) ([]string, error) {
	basePath := normalizeHiveInferPath(param.Filepath)
	listDir, err := newHiveInferListDir(param, basePath)
	if err != nil {
		return nil, err
	}

	currentPrefixes := []string{basePath}
	inferred := make([]string, 0)
	listCalls := 0
	for depth := 0; depth < hivePartitionInferMaxDepth && len(currentPrefixes) > 0; depth++ {
		var levelKey string
		nextPrefixes := make([]string, 0)
		for _, prefix := range currentPrefixes {
			listCalls++
			if listCalls > hivePartitionInferMaxListCalls {
				return nil, moerr.NewBadConfigf(ctx,
					"hive partition auto inference exceeded %d List calls; specify hive_partition_columns explicitly",
					hivePartitionInferMaxListCalls)
			}
			for entry, err := range listDir(ctx, prefix) {
				if err != nil {
					return nil, moerr.NewBadConfigf(ctx,
						"hive partition auto inference failed to list '%s': %v; specify hive_partition_columns explicitly",
						prefix, err)
				}
				if entry == nil || !entry.IsDir || isHiveInferHidden(entry.Name) {
					continue
				}
				key, isHive, err := parseHiveInferSegmentKey(entry.Name)
				if err != nil {
					return nil, err
				}
				if !isHive {
					continue
				}
				if levelKey == "" {
					levelKey = key
				} else if levelKey != key {
					return nil, moerr.NewBadConfigf(ctx,
						"hive partition auto inference found mixed keys '%s' and '%s' at the same level; specify hive_partition_columns explicitly",
						levelKey, key)
				}
				if len(nextPrefixes) < hivePartitionInferMaxSampleDirs {
					nextPrefixes = append(nextPrefixes, path.Join(prefix, entry.Name))
				}
			}
			if len(nextPrefixes) >= hivePartitionInferMaxSampleDirs {
				break
			}
		}
		if levelKey == "" {
			break
		}
		inferred = append(inferred, levelKey)
		currentPrefixes = nextPrefixes
	}
	if len(inferred) == 0 {
		return nil, moerr.NewBadConfig(ctx,
			"hive partition auto inference found no hive-style partition directories; specify hive_partition_columns explicitly")
	}
	return inferred, nil
}

type hiveInferListDirFunc func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error]

func newHiveInferListDir(param *tree.ExternParam, basePath string) (hiveInferListDirFunc, error) {
	if param.ScanType == tree.S3 {
		fs, baseReadPath, err := GetForETLWithType(param, basePath)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
			return fs.List(ctx, deriveHiveInferReadPath(basePath, baseReadPath, prefix))
		}, nil
	}
	return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		fs, readPath, err := GetForETLWithType(param, prefix)
		if err != nil {
			return func(yield func(*fileservice.DirEntry, error) bool) {
				yield(nil, err)
			}
		}
		return fs.List(ctx, readPath)
	}, nil
}

func normalizeHiveInferPath(p string) string {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "etl:") {
		return path.Clean(p)
	}
	if strings.Contains(p, fileservice.ServiceNameSeparator) {
		return path.Clean(p)
	}
	return path.Clean("/" + p)
}

func deriveHiveInferReadPath(basePath, baseReadPath, prefix string) string {
	prefix = normalizeHiveInferPath(prefix)
	if prefix == basePath {
		return baseReadPath
	}
	if !strings.HasPrefix(prefix, basePath+"/") {
		return prefix
	}
	rel := strings.TrimPrefix(prefix, basePath+"/")
	if rel == "" {
		return baseReadPath
	}
	if baseReadPath == "" || baseReadPath == "." {
		return rel
	}
	return path.Join(baseReadPath, rel)
}

func parseHiveInferSegmentKey(segment string) (string, bool, error) {
	idx := strings.IndexByte(segment, '=')
	if idx <= 0 {
		return "", false, nil
	}
	key := segment[:idx]
	if key == "." || key == ".." {
		return "", true, moerr.NewBadConfigf(context.Background(),
			"invalid hive partition key '%s' during auto inference", key)
	}
	for _, r := range key {
		if r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return "", true, moerr.NewBadConfigf(context.Background(),
				"invalid hive partition key '%s' during auto inference", key)
		}
	}
	return strings.ToLower(key), true, nil
}

func isHiveInferHidden(name string) bool {
	return len(name) > 0 && (name[0] == '.' || name[0] == '_')
}

func parseHiveOptionsFromRawOptions(ctx context.Context, options []string) (enabled bool, cols []string, err error) {
	var hiveVal string
	var colsVal string
	for i := 0; i < len(options); i += 2 {
		key := strings.ToLower(options[i])
		switch key {
		case "hive_partitioning":
			hiveVal = strings.ToLower(options[i+1])
		case "hive_partition_columns":
			colsVal = options[i+1]
		}
	}
	if hiveVal == "" {
		if strings.TrimSpace(colsVal) != "" {
			return false, nil, moerr.NewBadConfig(ctx, "hive_partition_columns requires hive_partitioning='true'")
		}
		return false, nil, nil
	}
	if hiveVal != "true" && hiveVal != "false" {
		return false, nil, moerr.NewBadConfigf(ctx, "hive_partitioning must be 'true' or 'false', got '%s'", hiveVal)
	}
	if hiveVal == "false" {
		if strings.TrimSpace(colsVal) != "" {
			return false, nil, moerr.NewBadConfig(ctx, "hive_partition_columns requires hive_partitioning='true'")
		}
		return false, nil, nil
	}
	if colsVal == "" {
		return true, nil, nil
	}
	parts := strings.Split(colsVal, ",")
	cols = make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			cols = append(cols, p)
		}
	}
	return true, cols, nil
}

func rejectDuplicateKeys(ctx context.Context, options []string, keys []string) error {
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	seen := make(map[string]bool)
	for i := 0; i < len(options); i += 2 {
		key := strings.ToLower(options[i])
		if !keySet[key] {
			continue
		}
		if seen[key] {
			return moerr.NewBadConfigf(ctx, "duplicate option key '%s'", key)
		}
		seen[key] = true
	}
	return nil
}

func getRawOption(options []string, key string) string {
	for i := 0; i < len(options); i += 2 {
		if strings.ToLower(options[i]) == key {
			return options[i+1]
		}
	}
	return ""
}

func stripHiveOptionKeys(opt []string) []string {
	out := make([]string, 0, len(opt))
	for i := 0; i < len(opt); i += 2 {
		key := strings.ToLower(opt[i])
		if key == "hive_partitioning" || key == "hive_partition_columns" {
			continue
		}
		out = append(out, opt[i], opt[i+1])
	}
	return out
}

func findColInTableDefCaseInsensitive(cols []*plan.ColDef, name string) *plan.ColDef {
	lower := strings.ToLower(name)
	for _, col := range cols {
		if strings.ToLower(col.Name) == lower {
			return col
		}
	}
	return nil
}
