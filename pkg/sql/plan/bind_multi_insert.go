// Copyright 2026 Matrix Origin
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
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// multiInsertSourceAlias is the table name under which the materialized source
// row image is bound in every branch's bind context, so that WHEN conditions
// and VALUES expressions resolve against the source query's output columns.
const multiInsertSourceAlias = "__mo_multi_insert_source"

// bindAndOptimizeMultiInsertQuery plans a Snowflake-style multi-table INSERT:
//
//	INSERT {ALL | FIRST} [WHEN cond THEN] INTO t1 ... [INTO t2 ...] [ELSE INTO ...] SELECT ...
//
// The source query is bound once and materialized in a SINK (step 0). Every
// target *table* then gets its own write step reading that sink:
//
//	SINK_SCAN -> [FILTER on the WHEN condition] -> PROJECT (VALUES exprs)
//	          -> casts/defaults/auto-increment -> lock/dedup -> MULTI_UPDATE
//
// i.e. exactly the single-table modern insert pipeline per table, fanned out
// over the shared sink. When the same table appears in several INTO clauses,
// each clause becomes one SINK_SCAN/FILTER/PROJECT branch and the branches are
// merged with UNION ALL in front of a single write pipeline, so that duplicate
// keys produced by different clauses are rejected exactly like duplicates
// within one INSERT ... SELECT. Regular (B-tree/unique) indexes of every target
// are written by that target's MULTI_UPDATE; IVF/fulltext indexes get their
// usual maintenance steps appended after createQuery.
func bindAndOptimizeMultiInsertQuery(ctx CompilerContext, stmt *tree.MultiInsert, isPrepareStmt bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_INSERT, ctx, isPrepareStmt, true)
	builder.parseOptimizeHints()
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	if err := builder.bindMultiInsert(stmt, bindCtx); err != nil {
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	if err = builder.finishIrregularIndexMaintenance(query, bindCtx); err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, nil
}

// maxMultiInsertTargets bounds the number of INTO clauses in one statement.
// Oracle uses the same limit.
const maxMultiInsertTargets = 127

// multiInsertGroupKey identifies the physical table an INTO clause writes, so
// that clauses naming the same table — bare, schema-qualified, or written in a
// different case — share one write pipeline. The resolved table id is the only
// case-policy-independent identity: keying on the lower-cased name would merge
// two genuinely distinct tables under lower_case_table_names=0, and keying on
// the raw name would fail to merge db.t with t. Catalog objects always carry a
// non-zero id; the name fallback exists only for synthetic table defs (tests),
// where failing to merge is the safe direction.
func multiInsertGroupKey(objRef *plan.ObjectRef, tableDef *plan.TableDef) string {
	if tableDef.TblId != 0 {
		return "id:" + strconv.FormatUint(tableDef.TblId, 10)
	}
	return "name:" + objRef.SchemaName + "." + objRef.ObjName
}

// multiInsertBranch is one INTO clause together with the route decision gating
// it. Conditions are referenced by WHEN index, never by AST: every WHEN is bound
// and evaluated exactly once, above the shared source sink, and materialized as
// one boolean selector column. A branch only names the selectors it consumes, so
// all INTO clauses of one WHEN necessarily observe the same decision — which
// matters for volatile predicates such as rand().
type multiInsertBranch struct {
	target *tree.MultiInsertTarget
	// condIdx is the WHEN whose selector must be true for a source row to reach
	// this target; -1 for an unconditional clause and for ELSE.
	condIdx int
	// isElse marks an ELSE target: it takes the rows no WHEN claimed. A NULL
	// condition counts as "not matched", so a row whose condition evaluated to
	// NULL reaches ELSE.
	isElse bool
	// insertColumns are the target columns this clause writes, in clause
	// order (the explicit column list, or every insertable column).
	insertColumns []string
}

// multiInsertGroup gathers every INTO clause that writes the same table; the
// group owns that table's single write pipeline.
type multiInsertGroup struct {
	dmlCtx   *DMLContext
	tableDef *plan.TableDef
	objRef   *plan.ObjectRef
	branches []*multiInsertBranch
}

// multiInsertBranches flattens the statement into per-clause branches, in
// source order, applying the ALL/FIRST/ELSE routing rules.
func multiInsertBranches(stmt *tree.MultiInsert) []*multiInsertBranch {
	branches := make([]*multiInsertBranch, 0, len(stmt.Targets)+len(stmt.Whens)+len(stmt.Else))
	for _, target := range stmt.Targets {
		branches = append(branches, &multiInsertBranch{target: target, condIdx: -1})
	}
	for i, when := range stmt.Whens {
		for _, target := range when.Targets {
			branches = append(branches, &multiInsertBranch{target: target, condIdx: i})
		}
	}
	for _, target := range stmt.Else {
		branches = append(branches, &multiInsertBranch{target: target, condIdx: -1, isElse: true})
	}
	return branches
}

func (builder *QueryBuilder) bindMultiInsert(stmt *tree.MultiInsert, bindCtx *BindContext) error {
	// Never inherit single-table insert proofs/flags from a previous DML bound
	// on this builder.
	builder.insertInputKeysUnique = false
	builder.isInsertIgnore = false

	if stmt.Source == nil {
		return moerr.NewInternalError(builder.GetContext(), "multi-table insert has no source query")
	}
	branches := multiInsertBranches(stmt)
	if len(branches) == 0 {
		return moerr.NewInternalError(builder.GetContext(), "multi-table insert has no target table")
	}
	// Each INTO clause adds a full write pipeline (its own dedup hash build over
	// the whole source), so the plan's cost is linear in the clause count and the
	// count is user-controlled. Cap it, as Oracle does, instead of letting a
	// pathological statement consume unbounded planner and executor memory.
	if len(branches) > maxMultiInsertTargets {
		return moerr.NewNotSupportedf(builder.GetContext(),
			"multi-table INSERT with more than %d INTO clauses", maxMultiInsertTargets)
	}

	// A statement-level WITH and a WITH on the trailing source SELECT are two
	// different lexical scopes, and both can be present:
	//
	//   WITH outer AS (...)                 -- statement scope
	//   INSERT ALL WHEN EXISTS (... outer)  -- sees the statement scope
	//     THEN INTO t VALUES (...)
	//   WITH local AS (...) SELECT ...      -- source scope, private
	//
	// The statement's CTEs are installed on the statement context, so the
	// source (its child) and the branch contexts (built from it below) both
	// see them, while a source-local WITH is installed by bindSelect on srcCtx
	// alone and stays private to the source query. Moving the statement WITH
	// into the source instead would drop it whenever the source has its own,
	// and would expose the source's private CTEs to the WHEN/VALUES
	// expressions that lexically precede them.
	//
	// The rewrite policy has to be installed FIRST. It rides on the source
	// query (that is where AddRewriteHints attaches it) but governs every read
	// the statement performs -- the source, WHEN, VALUES, and a statement CTE's
	// body alike. preprocessCte snapshots a declaration context per CTE, and
	// that snapshot copies remapOption and then detaches, so a policy assigned
	// afterwards would reach the source and branch contexts but never the CTE
	// bodies, letting them read the unrewritten base table.
	if stmt.Source != nil && stmt.Source.RewriteOption != nil {
		bindCtx.remapOption = stmt.Source.RewriteOption
	}
	if stmt.With != nil {
		if err := builder.preprocessCte(&tree.Select{With: stmt.With}, bindCtx); err != nil {
			return err
		}
	}

	// 1. Bind the source query once and materialize it.
	srcCtx := NewBindContext(builder, bindCtx)
	srcID, err := builder.bindSelect(stmt.Source, srcCtx, false)
	if err != nil {
		return err
	}
	srcNode := builder.qry.Nodes[srcID]
	if len(srcCtx.headings) != len(srcNode.ProjectList) {
		return moerr.NewInternalErrorf(builder.GetContext(),
			"multi-table insert source has %d headings but %d projected columns",
			len(srcCtx.headings), len(srcNode.ProjectList))
	}
	srcCols := make([]*plan.ColDef, len(srcCtx.headings))
	for i, name := range srcCtx.headings {
		srcCols[i] = &plan.ColDef{Name: name, Typ: srcNode.ProjectList[i].Typ}
	}

	// 1b. Evaluate every WHEN exactly once, here, above the shared source: one
	// boolean selector column per WHEN, appended to the source columns and
	// materialized by the sink. Targets consume those columns instead of
	// re-binding the predicate, so one WHEN occurrence is one route decision for
	// a row — the property that makes INSERT FIRST a partition and makes all
	// INTO clauses of one WHEN agree, even for volatile predicates like rand().
	selectors, err := builder.appendMultiInsertSelectors(bindCtx, srcID, srcCols, stmt.Whens, stmt.First)
	if err != nil {
		return err
	}

	sinkTag := builder.genNewBindTag()
	sinkID := appendSinkNodeWithTag(builder, bindCtx, selectors.nodeID, sinkTag)
	sourceStep := builder.appendStep(sinkID)

	// 2. Resolve every target and group the clauses by table, in source order.
	var groups []*multiInsertGroup
	byTable := make(map[string]*multiInsertGroup)
	for _, branch := range branches {
		dmlCtx, err := builder.resolveMultiInsertTarget(branch.target)
		if err != nil {
			return err
		}
		tableDef := dmlCtx.tableDefs[0]
		objRef := dmlCtx.objRefs[0]
		if branch.insertColumns, err = builder.getInsertColsFromStmt(branch.target.Columns, tableDef); err != nil {
			return err
		}
		key := multiInsertGroupKey(objRef, tableDef)
		group := byTable[key]
		if group == nil {
			group = &multiInsertGroup{dmlCtx: dmlCtx, tableDef: tableDef, objRef: objRef}
			byTable[key] = group
			groups = append(groups, group)
		}
		group.branches = append(group.branches, branch)
	}

	// 3. One write pipeline per table, every branch reading the sink.
	for _, group := range groups {
		if err = builder.bindMultiInsertGroup(bindCtx, group, sourceStep, srcCols, selectors, len(stmt.Whens)); err != nil {
			return err
		}
	}
	return nil
}

// appendMultiInsertSelectors projects the source columns unchanged and appends
// one boolean column per WHEN, evaluated once per source row. It returns the new
// top node; the selector for WHEN i lives at position len(srcCols)+i.
// multiInsertSelectors describes the materialized route decisions: selector j
// for WHEN j lives at column selectorBase+j of the returned node. For INSERT
// FIRST the selectors are mutually exclusive by construction, so "no selector
// is true" is exactly the ELSE predicate for both statement forms.
// noMultiInsertRoute is the route value of a row no WHEN claimed.
const noMultiInsertRoute int32 = -1

type multiInsertSelectors struct {
	nodeID       int32
	selectorBase int
	// routePos is the column holding the index of the WHEN that claimed the
	// row, or -1 for none, carried forward level by level for INSERT FIRST.
	// It is -1 (absent) for INSERT ALL, where a row can match several WHENs at
	// once and so has no single route.
	routePos int
}

// appendMultiInsertSelectors projects the source columns unchanged and appends
// one boolean column per WHEN, each condition evaluated exactly once.
//
// INSERT ALL evaluates every WHEN for every row by definition, so all selectors
// are computed in a single projection.
//
// INSERT FIRST must not evaluate a later WHEN for a row an earlier WHEN already
// claimed: a projection evaluates each expression over the whole batch, so an
// unreachable later predicate that errors (`cast('bad' as signed)`) or is
// expensive would still run. The selectors are therefore chained, one
// projection per WHEN, each computing
//
//	sel_i     = if(matched_{i-1}, false, cond_i)
//	matched_i = matched_{i-1} or istrue(sel_i)
//
// `if` evaluates a branch only on the rows selected for it (EvalIff compacts
// the parameters and evaluates just those rows), so cond_i runs exactly once
// and only for rows still unclaimed. sel_i is then already "matched here and
// nowhere earlier", so the branch filters need no exclusion terms at all.
func (builder *QueryBuilder) appendMultiInsertSelectors(
	bindCtx *BindContext,
	srcID int32,
	srcCols []*plan.ColDef,
	whens []*tree.MultiInsertWhen,
	first bool,
) (multiInsertSelectors, error) {
	out := multiInsertSelectors{nodeID: srcID, selectorBase: len(srcCols), routePos: -1}
	if len(whens) == 0 {
		return out, nil
	}

	// The conditions are written against the source query's output columns, so
	// bind them in a context where that output is a table. The scope is the
	// STATEMENT's, not the source's: a subquery inside a WHEN is another read
	// by this statement, so it sees the statement's CTEs and obeys its rewrite
	// policy, but it must not see CTEs the source query declared privately, nor
	// the source's row bindings. A declaration context built from the statement
	// context carries exactly that, and the source's columns stay reachable
	// only through the alias added below.
	condCtx := newCTEDeclarationContext(builder, bindCtx)
	if err := builder.addBinding(srcID, tree.AliasClause{Alias: multiInsertSourceAlias}, condCtx); err != nil {
		return out, err
	}
	if len(condCtx.bindings) != 1 {
		return out, moerr.NewInternalError(builder.GetContext(), "multi-table insert failed to bind its source query")
	}
	// addBinding already exposes a derived-table binding's columns unqualified;
	// unlike the SINK_SCAN case below, no manual bindingByCol fixup is needed
	// (and doing it would mark every column ambiguous).
	binding := condCtx.bindings[0]
	condCtx.binder = NewWhereBinder(builder, condCtx)

	srcTag := binding.tag
	lastNodeID := srcID
	conds := make([]*plan.Expr, 0, len(whens))
	for i, when := range whens {
		cond, err := builder.bindMultiInsertConditionExpr(condCtx, when.Cond)
		if err != nil {
			return out, err
		}
		// A subquery is flattened into a JOIN below the selector projections,
		// and a join processes the whole batch: no projection-level mask can
		// stop it running for rows an earlier WHEN already claimed. Masking the
		// column the join produces does not restore first-match semantics, so
		// refuse the shape instead of silently violating it. The first WHEN is
		// fine — it applies to every row by definition — and INSERT ALL has no
		// first-match rule at all.
		if first && i > 0 && exprHasSubquery(cond) {
			return out, moerr.NewNotSupportedf(builder.GetContext(),
				"INSERT FIRST with a subquery in WHEN #%d: it cannot be skipped for rows an earlier WHEN already matched", i+1)
		}
		var nodeID int32
		if nodeID, cond, err = builder.flattenSubqueries(lastNodeID, cond, condCtx); err != nil {
			return out, err
		}
		if cond == nil {
			return out, moerr.NewInternalError(builder.GetContext(), "multi-table insert WHEN condition vanished during binding")
		}
		lastNodeID = nodeID
		conds = append(conds, cond)
	}

	passthrough := func(tag int32, cols []*plan.ColDef) []*plan.Expr {
		list := make([]*plan.Expr, len(cols))
		for i, col := range cols {
			list[i] = &plan.Expr{
				Typ:  col.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: int32(i), Name: col.Name}},
			}
		}
		return list
	}

	if !first {
		projList := append(passthrough(srcTag, srcCols), conds...)
		out.nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeID},
			ProjectList: projList,
			BindingTags: []int32{builder.genNewBindTag()},
		}, condCtx)
		return out, nil
	}

	// INSERT FIRST: one projection, one route column, one expression.
	//
	//   route = if(cond_0, 0, if(cond_1, 1, ... if(cond_{W-1}, W-1, -1)))
	//
	// EvalIff evaluates a false branch only on the rows whose condition was
	// false, and passes that selection down to the child executor, which the
	// next nested if propagates further. So cond_i runs exactly once and only
	// on rows no earlier WHEN claimed: an unreachable later predicate never
	// runs, and never errors, for a row an earlier WHEN already took. A NULL
	// condition takes the false branch, leaving the row for later WHENs and
	// ELSE, as IS TRUE would.
	//
	// Everything lives in one projection above the conditions' own node, so the
	// plan carries M + 1 expressions plus the W conditions -- O(M + W), not the
	// O(W * M) of a per-WHEN chain that re-emits every source column at every
	// level. That matters because Projection.Prepare builds one executor per
	// ProjectList entry and Projection.Call runs them for every batch.
	route := makePlan2Int32ConstExprWithType(noMultiInsertRoute)
	for i := len(conds) - 1; i >= 0; i-- {
		claimed, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "if",
			[]*plan.Expr{conds[i], makePlan2Int32ConstExprWithType(int32(i)), route})
		if err != nil {
			return out, err
		}
		route = claimed
	}
	projList := append(passthrough(srcTag, srcCols), route)
	out.nodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: projList,
		BindingTags: []int32{builder.genNewBindTag()},
	}, condCtx)
	out.selectorBase = -1
	out.routePos = len(srcCols)
	return out, nil
}

// bindMultiInsertConditionExpr binds one WHEN condition against the source
// binding, ANDing its conjuncts back into a single boolean. Subquery flattening
// is deliberately left to the caller, which must first decide whether the shape
// is maskable.
func (builder *QueryBuilder) bindMultiInsertConditionExpr(
	condCtx *BindContext, astCond tree.Expr,
) (*plan.Expr, error) {
	conds, err := splitAndBindCondition(astCond, NoAlias, condCtx)
	if err != nil {
		return nil, err
	}
	cond := conds[0]
	for _, extra := range conds[1:] {
		if cond, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and",
			[]*plan.Expr{cond, extra}); err != nil {
			return nil, err
		}
	}
	return cond, nil
}

// exprHasSubquery reports whether a bound expression still carries a subquery,
// i.e. before flattenSubqueries has turned it into a join.
func exprHasSubquery(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Sub:
		return true
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if exprHasSubquery(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if exprHasSubquery(item) {
				return true
			}
		}
	}
	return false
}

// validateMultiInsertTarget rejects target tables the multi-table INSERT does
// not support (per the feature scope: no foreign keys, no external tables).
func validateMultiInsertTarget(ctx context.Context, tableDef *plan.TableDef) error {
	if len(tableDef.Fkeys) > 0 {
		return moerr.NewNotSupportedf(ctx,
			"multi-table INSERT into table '%s' with foreign key constraints", tableDef.Name)
	}
	switch tableDef.TableType {
	case catalog.SystemExternalRel:
		return moerr.NewNotSupportedf(ctx,
			"multi-table INSERT into external table '%s'", tableDef.Name)
	case catalog.SystemSourceRel:
		return moerr.NewNYIf(ctx, "insert stream %s", tableDef.Name)
	case catalog.SystemClusterRel:
		return moerr.NewNotSupportedf(ctx,
			"multi-table INSERT into cluster table '%s'", tableDef.Name)
	}
	return nil
}

// resolveMultiInsertTarget resolves one INTO clause's table and validates it
// as a multi-table INSERT target.
func (builder *QueryBuilder) resolveMultiInsertTarget(target *tree.MultiInsertTarget) (*DMLContext, error) {
	sysCtx := builder.GetContext()

	// Foreign-key tables are resolved (not rejected by the generic DML
	// resolver) so that validateMultiInsertTarget can report the precise reason.
	dmlCtx := NewDMLContext()
	builder.compCtx.SetContext(context.WithValue(sysCtx, defines.IgnoreForeignKey{}, true))
	// Restore via defer: the marker lives on the shared CompilerContext, so a
	// panic inside ResolveTables must not leave FK rejection disabled for the
	// rest of the session's planning.
	defer builder.compCtx.SetContext(sysCtx)
	err := dmlCtx.ResolveTables(builder.compCtx, tree.TableExprs{target.Table}, nil, nil, true)
	if err != nil {
		return nil, err
	}
	tableDef := dmlCtx.tableDefs[0]

	targetDB := string(target.Table.SchemaName)
	if targetDB == "" {
		targetDB = builder.compCtx.DefaultDatabase()
	}
	targetTable := string(target.Table.ObjectName)
	dmlCtx.targetDBName = targetDB
	dmlCtx.targetTableName = targetTable
	if err = validateInsertColumnQualifiers(
		sysCtx, target.ColumnNames, targetDB, targetTable, builder.compCtx.GetLowerCaseTableNames(),
	); err != nil {
		return nil, err
	}
	if err = validateMultiInsertTarget(sysCtx, tableDef); err != nil {
		return nil, err
	}
	if err = validateTableRegularIndexPrefixMetadata(tableDef); err != nil {
		return nil, err
	}
	return dmlCtx, nil
}

// bindMultiInsertGroup builds the write pipeline of one target table and
// registers it as a step.
func (builder *QueryBuilder) bindMultiInsertGroup(
	bindCtx *BindContext,
	group *multiInsertGroup,
	sourceStep int32,
	srcCols []*plan.ColDef,
	selectors multiInsertSelectors,
	whenCount int,
) error {
	tableDef := group.tableDef
	objRef := group.objRef

	// Capture irregular (IVF/fulltext/master) indexes before the insert helpers
	// strip them from the 1:1 dedup+MULTI_UPDATE plan; their 1:N maintenance is
	// appended after createQuery from the materialized new-row image.
	irregularIndexes := getIrregularIndexes(tableDef)

	// The write pipeline gets its own bind context: the derived-table bindings
	// created by the insert helpers must not clash across tables.
	tCtx := NewBindContext(builder, nil)
	tCtx.snapshot = bindCtx.snapshot

	var (
		lastNodeID    int32
		colName2Idx   map[string]int32
		skipUniqueIdx []bool
		err           error
	)
	if len(group.branches) == 1 {
		// Single clause: bind its row image in clause column order and hand it
		// to the regular insert tail, which casts and fills defaults.
		branch := group.branches[0]
		lastNodeID, err = builder.bindMultiInsertBranchSource(bindCtx, branch, sourceStep, srcCols, nil, tableDef, selectors, whenCount)
		if err != nil {
			return err
		}
		lastNodeID, colName2Idx, skipUniqueIdx, err = builder.appendInsertReplaceSourceCasts(
			tCtx, lastNodeID, branch.insertColumns, objRef, tableDef, false)
		if err != nil {
			return err
		}
	} else {
		// Several clauses write this table: widen every clause to the union of
		// their column lists (defaults for the columns a clause does not set),
		// cast to the target types, and UNION ALL the branches so the table is
		// written by one pipeline with one dedup pass.
		insertColumns := multiInsertUnionColumns(group.branches)
		branchIDs := make([]int32, 0, len(group.branches))
		for _, branch := range group.branches {
			branchID, err := builder.bindMultiInsertBranchSource(bindCtx, branch, sourceStep, srcCols, insertColumns, tableDef, selectors, whenCount)
			if err != nil {
				return err
			}
			branchIDs = append(branchIDs, branchID)
		}
		// Classify from the BOUND value expressions, not the column lists: a
		// clause that lists the column but supplies NULL still gets a generated
		// value from PRE_INSERT.
		if err = builder.validateMergedAutoIncrColumns(tableDef, insertColumns, branchIDs); err != nil {
			return err
		}
		unionID := builder.appendMultiInsertUnionAll(tCtx, branchIDs)
		unionNode := builder.qry.Nodes[unionID]
		unionTag := unionNode.BindingTags[0]
		insertColToExpr := make(map[string]*plan.Expr, len(insertColumns))
		for i, column := range insertColumns {
			insertColToExpr[column] = &plan.Expr{
				Typ: unionNode.ProjectList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: unionTag, ColPos: int32(i)},
				},
			}
		}
		lastNodeID, colName2Idx, skipUniqueIdx, err = builder.appendNodesForInsertStmt(
			tCtx, unionID, tableDef, objRef, insertColToExpr)
		if err != nil {
			return err
		}
	}

	rootID, err := builder.appendDedupAndMultiUpdateNodesForBindInsert(
		tCtx, group.dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, nil, irregularIndexes)
	if err != nil {
		return err
	}
	builder.qry.Steps = append(builder.qry.Steps, rootID)

	// The insert pipeline records irregular-index maintenance for one table in
	// the builder's single-target fields; queue it so every target's
	// maintenance is emitted by finishIrregularIndexMaintenance.
	if len(builder.irregularMaintIndexes) > 0 || len(builder.irregularMaintInsertOnlyIndexes) > 0 {
		builder.irregularUpdateMaints = append(builder.irregularUpdateMaints, irregularUpdateMaintenance{
			sourceStep:              builder.irregularMaintSourceStep,
			deleteStep:              builder.irregularMaintDeleteStep,
			deletePkPos:             builder.irregularMaintDeletePkPos,
			deletePkTyp:             builder.irregularMaintDeletePkTyp,
			indexes:                 builder.irregularMaintIndexes,
			insertOnlySourceStep:    builder.irregularMaintInsertOnlySourceStep,
			insertOnlyIndexes:       builder.irregularMaintInsertOnlyIndexes,
			valueChangedSourceSteps: builder.irregularMaintValueChangedSourceSteps,
			tableDef:                builder.irregularMaintTableDef,
			objRef:                  builder.irregularMaintObjRef,
		})
		builder.irregularMaintIndexes = nil
		builder.irregularMaintInsertOnlyIndexes = nil
		builder.irregularMaintValueChangedSourceSteps = nil
		builder.irregularMaintTableDef = nil
		builder.irregularMaintObjRef = nil
		builder.irregularMaintDeleteStep = -1
		builder.irregularMaintInsertOnlySourceStep = -1
	}
	return nil
}

// autoIncrValueKind classifies what a merged clause supplies for an
// AUTO_INCREMENT column.
type autoIncrValueKind int

const (
	// autoIncrGenerated: the value is certainly produced by PRE_INSERT — the
	// clause omitted the column, or supplied a NULL constant.
	autoIncrGenerated autoIncrValueKind = iota
	// autoIncrExplicit: the value certainly comes from the statement, because
	// the bound expression is provably non-nullable.
	autoIncrExplicit
	// autoIncrUnknown: a nullable expression, which may be either per row.
	autoIncrUnknown
)

// classifyAutoIncrValue decides what a clause supplies for an AUTO_INCREMENT
// column, judging the value that actually reaches PRE_INSERT.
//
// The branch projection holds the value already cast to the column's integer
// type, so the expression is constant-folded first: that is what turns 0.0,
// '0', or a decimal zero into the integer 0 that PRE_INSERT sees.
//
// A zero counts as GENERATED regardless of sql_mode. In the default mode that
// is simply what happens (shouldConvertZeroToNull in the preinsert operator);
// under NO_AUTO_VALUE_ON_ZERO a zero is really explicit, so treating it as
// generated only ever refuses a statement that would have been safe. The
// alternative — reading sql_mode here — would bake a session setting into the
// plan, and PRE_INSERT re-reads that setting at EXECUTE time: a plan prepared
// under one mode and executed under another would classify with a stale bit
// and let the mixed generated/explicit pipeline through. Mode-independence
// removes that whole class of staleness instead of tracking one more prepared
// plan dependency.
//
// Anything that does not fold to a constant is unknown, and unknown is refused
// rather than assumed safe: a non-literal can still evaluate to zero or NULL.
func classifyAutoIncrValue(expr *plan.Expr, proc *process.Process) autoIncrValueKind {
	if expr == nil {
		return autoIncrGenerated
	}
	folded := expr
	if proc != nil {
		if out, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), proc, true, true); err == nil && out != nil {
			folded = out
		}
	}
	lit := folded.GetLit()
	if lit == nil {
		return autoIncrUnknown
	}
	if lit.Isnull {
		return autoIncrGenerated
	}
	switch classifyZeroLiteral(lit) {
	case literalZero:
		return autoIncrGenerated
	case literalNonZero:
		return autoIncrExplicit
	default:
		return autoIncrUnknown
	}
}

// zeroLiteralKind classifies a folded literal as zero, non-zero, or an
// unrecognized representation. Whichever integer width the folder picks for the
// AUTO_INCREMENT column, a zero value means PRE_INSERT generates the key in the
// default sql_mode, so every width must be handled — and a representation this
// does not understand must be reported as such rather than guessed, since
// guessing "zero" silently disables the caller's guard and guessing "non-zero"
// silently admits a generated value.
type zeroLiteralKind int

const (
	literalZero zeroLiteralKind = iota
	literalNonZero
	literalUnrecognized
)

func classifyZeroLiteral(lit *plan.Literal) zeroLiteralKind {
	zero := func(isZero bool) zeroLiteralKind {
		if isZero {
			return literalZero
		}
		return literalNonZero
	}
	switch v := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return zero(v.I8Val == 0)
	case *plan.Literal_I16Val:
		return zero(v.I16Val == 0)
	case *plan.Literal_I32Val:
		return zero(v.I32Val == 0)
	case *plan.Literal_I64Val:
		return zero(v.I64Val == 0)
	case *plan.Literal_U8Val:
		return zero(v.U8Val == 0)
	case *plan.Literal_U16Val:
		return zero(v.U16Val == 0)
	case *plan.Literal_U32Val:
		return zero(v.U32Val == 0)
	case *plan.Literal_U64Val:
		return zero(v.U64Val == 0)
	case *plan.Literal_Fval:
		return zero(v.Fval == 0)
	case *plan.Literal_Dval:
		return zero(v.Dval == 0)
	case *plan.Literal_Bval:
		return zero(!v.Bval)
	case *plan.Literal_Sval:
		text := strings.TrimSpace(v.Sval)
		if value, err := strconv.ParseFloat(text, 64); err == nil {
			return zero(value == 0)
		}
		return literalUnrecognized
	}
	return literalUnrecognized
}

// validateMergedAutoIncrColumns rejects a same-table merge whose clauses do not
// agree on whether an AUTO_INCREMENT column is generated or supplied.
//
// Merged clauses become UNION ALL branches feeding one PRE_INSERT. A branch
// that leaves the column to the engine contributes NULLs that PRE_INSERT fills
// from the table counter, while a branch supplying explicit values runs
// concurrently and has not necessarily advanced that counter yet, so the
// generated values are nondeterministic and can collide with the explicit ones
// (measured: a duplicate key in 1 run of 8, and a silent collision without a
// primary key; with explicit values inside the generated range it fails every
// time). The same race exists for a hand-written INSERT ... SELECT ... UNION
// ALL, but multi-table INSERT synthesizes the union, so ordinary SQL hits it.
//
// Membership in the clause's column list does NOT decide this: a listed column
// holding NULL is still generated. The decision is therefore made on the bound
// value expression, and a nullable expression — which could be either per row —
// is refused rather than assumed safe. Every clause generating, or every clause
// supplying a provably non-null value, both stay supported.
func (builder *QueryBuilder) validateMergedAutoIncrColumns(
	tableDef *plan.TableDef, insertColumns []string, branchIDs []int32,
) error {
	for _, col := range tableDef.Cols {
		if !col.Typ.AutoIncr {
			continue
		}
		pos := -1
		for i, column := range insertColumns {
			if strings.EqualFold(column, col.Name) {
				pos = i
				break
			}
		}
		if pos < 0 {
			// no clause writes it: every row is generated, which is safe
			continue
		}

		var generated, explicit, unknown bool
		for _, id := range branchIDs {
			node := builder.qry.Nodes[id]
			if pos >= len(node.ProjectList) {
				return moerr.NewInternalErrorf(builder.GetContext(),
					"multi-table insert branch is missing column %s", col.Name)
			}
			switch classifyAutoIncrValue(node.ProjectList[pos], builder.compCtx.GetProcess()) {
			case autoIncrGenerated:
				generated = true
			case autoIncrExplicit:
				explicit = true
			default:
				unknown = true
			}
		}
		if unknown || (generated && explicit) {
			return moerr.NewNotSupportedf(builder.GetContext(),
				"multi-table INSERT where INTO clauses disagree on whether auto_increment column '%s' of table '%s' is generated (every clause must omit it or supply a value that cannot be NULL)",
				col.Name, tableDef.Name)
		}
	}
	return nil
}

// multiInsertUnionColumns returns the union of the branches' column lists, in
// first-appearance order.
func multiInsertUnionColumns(branches []*multiInsertBranch) []string {
	var columns []string
	seen := make(map[string]struct{})
	for _, branch := range branches {
		for _, column := range branch.insertColumns {
			if _, ok := seen[column]; ok {
				continue
			}
			seen[column] = struct{}{}
			columns = append(columns, column)
		}
	}
	return columns
}

// bindMultiInsertBranchSource builds one clause's row image:
//
//	SINK_SCAN(source) -> [FILTER] -> PROJECT
//
// With unionColumns == nil the projection yields the clause's VALUES (or the
// source columns) in clause column order, uncast. Otherwise it yields one
// column per entry of unionColumns: the clause's value cast to the target
// column type, or the column's default when the clause does not set it.
func (builder *QueryBuilder) bindMultiInsertBranchSource(
	bindCtx *BindContext,
	branch *multiInsertBranch,
	sourceStep int32,
	srcCols []*plan.ColDef,
	unionColumns []string,
	tableDef *plan.TableDef,
	selectors multiInsertSelectors,
	whenCount int,
) (int32, error) {
	target := branch.target
	sysCtx := builder.GetContext()

	// Every clause binds the source image under the same alias, so each gets
	// its own bind context, built from the STATEMENT's declaration scope: a
	// subquery in this clause's VALUES is another read by this statement, so it
	// sees the statement's CTEs and obeys its rewrite policy, but not the
	// source query's private CTEs or row bindings. The source columns are
	// reachable only through the alias added below.
	bCtx := newCTEDeclarationContext(builder, bindCtx)

	scanTag := builder.genNewBindTag()
	scanCols := make([]*plan.ColDef, len(srcCols))
	for i, col := range srcCols {
		scanCols[i] = &plan.ColDef{Name: col.Name, Typ: col.Typ}
	}
	scanID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_SINK_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: getProjectionByLastNodeWithTag(builder, builder.qry.Steps[sourceStep], scanTag),
		BindingTags: []int32{scanTag},
		TableDef:    &plan.TableDef{Name: multiInsertSourceAlias, Cols: scanCols},
	}, bCtx)
	// This scan projects the shared source sink BY POSITION. createQuery prunes
	// the columns no branch referenced from that sink and records the resulting
	// shift in sinkColRef; only nodes registered here get their ColPos repaired
	// afterwards. Without the registration a branch that reads a strict subset of
	// the source columns keeps pre-prune positions into a narrower batch and the
	// projection panics with an index-out-of-range.
	if builder.positionalSinkScans == nil {
		builder.positionalSinkScans = make(map[int32]struct{})
	}
	builder.positionalSinkScans[scanID] = struct{}{}
	if err := builder.addBinding(scanID, tree.AliasClause{Alias: multiInsertSourceAlias}, bCtx); err != nil {
		return 0, err
	}
	// addBinding registers a SINK_SCAN only under its table name; expose its
	// columns unqualified too, the way users write them in WHEN/VALUES.
	if binding := bCtx.bindingByTag[scanTag]; binding != nil {
		for _, col := range binding.cols {
			if _, ok := bCtx.bindingByCol[col]; ok {
				bCtx.bindingByCol[col] = nil
			} else {
				bCtx.bindingByCol[col] = binding
			}
		}
	}
	lastNodeID := scanID

	// FILTER: read the route decisions straight out of the materialized selector
	// columns. Nothing is re-evaluated here, so every clause of one WHEN sees the
	// identical decision. For INSERT FIRST the selector is already masked by the
	// earlier WHENs, so a WHEN branch needs no exclusion terms and ELSE is simply
	// "nothing claimed this row".
	bCtx.binder = NewWhereBinder(builder, bCtx)
	selector := func(pos int) *plan.Expr {
		return &plan.Expr{
			Typ:  boolType,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: int32(pos)}},
		}
	}
	route := func(want int32) (*plan.Expr, error) {
		col := &plan.Expr{
			Typ:  makePlan2Int32ConstExprWithType(0).Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: int32(selectors.routePos)}},
		}
		return BindFuncExprImplByPlanExpr(builder.GetContext(), "=",
			[]*plan.Expr{col, makePlan2Int32ConstExprWithType(want)})
	}
	var filterList []*plan.Expr
	switch {
	case selectors.routePos >= 0 && branch.condIdx >= 0:
		// INSERT FIRST: the route column already names the winning WHEN, so a
		// branch is one integer test and needs no exclusion terms.
		claimed, err := route(int32(branch.condIdx))
		if err != nil {
			return 0, err
		}
		filterList = append(filterList, claimed)
	case selectors.routePos >= 0 && branch.isElse:
		unclaimed, err := route(noMultiInsertRoute)
		if err != nil {
			return 0, err
		}
		filterList = append(filterList, unclaimed)
	case branch.condIdx >= 0:
		filterList = append(filterList, selector(selectors.selectorBase+branch.condIdx))
	case branch.isElse:
		// INSERT ALL has no first-match rule and so no cumulative flag: a row
		// reaches ELSE only when no WHEN matched it. A NULL selector counts as
		// "not matched", hence IS NOT TRUE rather than NOT.
		for idx := 0; idx < whenCount; idx++ {
			notTrue, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnottrue",
				[]*plan.Expr{selector(selectors.selectorBase + idx)})
			if err != nil {
				return 0, err
			}
			filterList = append(filterList, notTrue)
		}
	}
	if len(filterList) > 0 {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{lastNodeID},
			FilterList: filterList,
		}, bCtx)
	}

	// The clause's values: its VALUES expressions, or every source column.
	var values []*plan.Expr
	if target.Values == nil {
		values = make([]*plan.Expr, len(srcCols))
		for i, col := range srcCols {
			values[i] = &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: scanTag, ColPos: int32(i), Name: col.Name},
				},
			}
		}
	} else {
		values = make([]*plan.Expr, 0, len(target.Values))
		for _, astExpr := range target.Values {
			astExpr, err := bCtx.qualifyColumnNames(astExpr, NoAlias)
			if err != nil {
				return 0, err
			}
			expr, err := bCtx.binder.BindExpr(astExpr, 0, true)
			if err != nil {
				return 0, err
			}
			// A subquery in the VALUES of a CONDITIONAL clause has the same
			// unmaskable-join problem as one in a later WHEN: flattenSubqueries
			// rewrites it into a join whose build side is attached as a PreScope,
			// and MergeRun starts PreScopes even when the filtered probe selects
			// no rows. The subquery would therefore run — and could fail — for a
			// clause the row never reached. The FILTER below cannot prevent it,
			// so refuse the shape rather than evaluate an unreachable value.
			// Unconditional clauses are unaffected: every row reaches them.
			if (branch.condIdx >= 0 || branch.isElse) && exprHasSubquery(expr) {
				return 0, moerr.NewNotSupported(sysCtx,
					"multi-table INSERT with a subquery in the VALUES of a conditional INTO clause: it cannot be skipped for rows the clause does not select")
			}
			lastNodeID, expr, err = builder.flattenSubqueries(lastNodeID, expr, bCtx)
			if err != nil {
				return 0, err
			}
			values = append(values, expr)
		}
	}
	if len(values) != len(branch.insertColumns) {
		return 0, moerr.NewInvalidInput(sysCtx, "insert values does not match the number of columns")
	}

	projList := values
	if unionColumns != nil {
		valueByColumn := make(map[string]*plan.Expr, len(values))
		for i, column := range branch.insertColumns {
			valueByColumn[column] = values[i]
		}
		projList = make([]*plan.Expr, 0, len(unionColumns))
		for _, column := range unionColumns {
			colDef := tableDef.Cols[tableDef.Name2ColIndex[column]]
			expr, ok := valueByColumn[column]
			var err error
			if ok {
				expr, err = builder.castInsertSourceColumn(expr, expr, colDef)
			} else {
				expr, err = getDefaultExpr(sysCtx, colDef)
			}
			if err != nil {
				return 0, err
			}
			projList = append(projList, expr)
		}
	}

	projTag := builder.genNewBindTag()
	return builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: projList,
		BindingTags: []int32{projTag},
	}, bCtx), nil
}

// appendMultiInsertUnionAll chains the branches of one table into a left-deep
// UNION ALL. Every branch projects the same column list, already cast to the
// target column types.
//
// The union must stay EAGER: every branch is a SINK_SCAN of the shared source
// sink, and the sink's spool lets the producer run at most N batches ahead of
// the slowest consumer. If the union's scopes were compiled lazily (see
// LazyPreScopes in compileUnionAll), a later branch would not start consuming
// until an earlier one finished, and the producer would block forever. Today
// laziness requires a Limit and a PROJECT step root, and a multi-insert step
// root is always MULTI_UPDATE, so this holds — but it is a real constraint on
// this plan shape, not an incidental property.
func (builder *QueryBuilder) appendMultiInsertUnionAll(bindCtx *BindContext, branchIDs []int32) int32 {
	lastNodeID := branchIDs[0]
	for _, rightID := range branchIDs[1:] {
		leftNode := builder.qry.Nodes[lastNodeID]
		rightNode := builder.qry.Nodes[rightID]
		leftTag := leftNode.BindingTags[0]
		projList := make([]*plan.Expr, len(leftNode.ProjectList))
		for i, expr := range leftNode.ProjectList {
			projList[i] = &plan.Expr{
				Typ: setOperationOutputType(plan.Node_UNION_ALL, expr.Typ, rightNode.ProjectList[i].Typ),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: leftTag, ColPos: int32(i)},
				},
			}
		}
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_UNION_ALL,
			Children:    []int32{lastNodeID, rightID},
			BindingTags: []int32{builder.genNewBindTag()},
			ProjectList: projList,
		}, bindCtx)
	}
	return lastNodeID
}
