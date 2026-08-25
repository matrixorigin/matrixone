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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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

// multiInsertBranch is one INTO clause together with the conditions gating it.
type multiInsertBranch struct {
	target *tree.MultiInsertTarget
	// cond must be true for a source row to reach the target; nil means
	// unconditional.
	cond tree.Expr
	// excluded are the conditions of earlier WHEN branches that must NOT be
	// true for the row: every earlier WHEN for INSERT FIRST, and all WHENs for
	// the ELSE targets. A NULL condition counts as "not true", so a row whose
	// earlier condition evaluated to NULL is still eligible.
	excluded []tree.Expr
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
		branches = append(branches, &multiInsertBranch{target: target})
	}
	seen := make([]tree.Expr, 0, len(stmt.Whens))
	for _, when := range stmt.Whens {
		var excluded []tree.Expr
		if stmt.First {
			excluded = append([]tree.Expr(nil), seen...)
		}
		for _, target := range when.Targets {
			branches = append(branches, &multiInsertBranch{
				target:   target,
				cond:     when.Cond,
				excluded: excluded,
			})
		}
		seen = append(seen, when.Cond)
	}
	for _, target := range stmt.Else {
		branches = append(branches, &multiInsertBranch{
			target:   target,
			excluded: append([]tree.Expr(nil), seen...),
		})
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

	// Pass WITH clause from the INSERT to the source query if present.
	if stmt.With != nil && stmt.Source.With == nil {
		stmt.Source.With = stmt.With
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

	sinkTag := builder.genNewBindTag()
	sinkID := appendSinkNodeWithTag(builder, bindCtx, srcID, sinkTag)
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
		if err = builder.bindMultiInsertGroup(bindCtx, group, sourceStep, srcCols); err != nil {
			return err
		}
	}
	return nil
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
		lastNodeID, err = builder.bindMultiInsertBranchSource(bindCtx, branch, sourceStep, srcCols, nil, tableDef)
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
		if err = validateMergedAutoIncrColumns(builder.GetContext(), tableDef, group.branches); err != nil {
			return err
		}
		insertColumns := multiInsertUnionColumns(group.branches)
		branchIDs := make([]int32, 0, len(group.branches))
		for _, branch := range group.branches {
			branchID, err := builder.bindMultiInsertBranchSource(bindCtx, branch, sourceStep, srcCols, insertColumns, tableDef)
			if err != nil {
				return err
			}
			branchIDs = append(branchIDs, branchID)
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
	if len(builder.irregularMaintIndexes) > 0 {
		builder.irregularUpdateMaints = append(builder.irregularUpdateMaints, irregularUpdateMaintenance{
			sourceStep:  builder.irregularMaintSourceStep,
			deleteStep:  builder.irregularMaintDeleteStep,
			deletePkPos: builder.irregularMaintDeletePkPos,
			deletePkTyp: builder.irregularMaintDeletePkTyp,
			indexes:     builder.irregularMaintIndexes,
			tableDef:    builder.irregularMaintTableDef,
			objRef:      builder.irregularMaintObjRef,
		})
		builder.irregularMaintIndexes = nil
		builder.irregularMaintTableDef = nil
		builder.irregularMaintObjRef = nil
		builder.irregularMaintDeleteStep = -1
	}
	return nil
}

// validateMergedAutoIncrColumns rejects a same-table merge in which one INTO
// clause supplies an AUTO_INCREMENT column explicitly while another leaves it to
// the engine.
//
// Merged clauses become UNION ALL branches feeding one PRE_INSERT. The branch
// that omits the column contributes NULLs that PRE_INSERT fills from the table
// counter, but the branch supplying explicit values runs concurrently and has
// not necessarily advanced that counter yet, so the generated values are
// nondeterministic and can collide with the explicit ones (observed: 1 run in 8
// produced a duplicate key, and without a primary key the collision is silent).
// The same race exists for a hand-written INSERT ... SELECT ... UNION ALL, but
// multi-table INSERT synthesizes the union, so ordinary SQL would hit it.
// Refuse the combination rather than write nondeterministic keys; every clause
// setting the column, or every clause omitting it, stays supported.
func validateMergedAutoIncrColumns(ctx context.Context, tableDef *plan.TableDef, branches []*multiInsertBranch) error {
	for _, col := range tableDef.Cols {
		if !col.Typ.AutoIncr {
			continue
		}
		set, omitted := false, false
		for _, branch := range branches {
			found := false
			for _, column := range branch.insertColumns {
				if strings.EqualFold(column, col.Name) {
					found = true
					break
				}
			}
			if found {
				set = true
			} else {
				omitted = true
			}
		}
		if set && omitted {
			return moerr.NewNotSupportedf(ctx,
				"multi-table INSERT where some INTO clauses set auto_increment column '%s' of table '%s' and others do not",
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
) (int32, error) {
	target := branch.target
	sysCtx := builder.GetContext()

	// Every clause binds the source image under the same alias, so each gets
	// its own bind context.
	bCtx := NewBindContext(builder, nil)
	bCtx.snapshot = bindCtx.snapshot

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

	// FILTER: the clause's own WHEN condition plus the negated earlier ones.
	bCtx.binder = NewWhereBinder(builder, bCtx)
	filterList := make([]*plan.Expr, 0, len(branch.excluded)+1)
	if branch.cond != nil {
		conds, err := splitAndBindCondition(branch.cond, NoAlias, bCtx)
		if err != nil {
			return 0, err
		}
		filterList = append(filterList, conds...)
	}
	for _, excluded := range branch.excluded {
		notTrue, err := builder.bindMultiInsertNotTrue(excluded, bCtx)
		if err != nil {
			return 0, err
		}
		filterList = append(filterList, notTrue)
	}
	if len(filterList) > 0 {
		flattened := make([]*plan.Expr, 0, len(filterList))
		for _, expr := range filterList {
			var err error
			lastNodeID, expr, err = builder.flattenSubqueries(lastNodeID, expr, bCtx)
			if err != nil {
				return 0, err
			}
			if expr != nil {
				flattened = append(flattened, expr)
			}
		}
		if len(flattened) > 0 {
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{lastNodeID},
				FilterList: flattened,
			}, bCtx)
		}
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

// bindMultiInsertNotTrue binds an earlier WHEN condition as "cond IS NOT TRUE",
// so a row is excluded only when that condition actually held (NULL does not
// exclude), matching INSERT FIRST / ELSE routing.
func (builder *QueryBuilder) bindMultiInsertNotTrue(astCond tree.Expr, bCtx *BindContext) (*plan.Expr, error) {
	conds, err := splitAndBindCondition(astCond, NoAlias, bCtx)
	if err != nil {
		return nil, err
	}
	cond := conds[0]
	for _, c := range conds[1:] {
		cond, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{cond, c})
		if err != nil {
			return nil, err
		}
	}
	return BindFuncExprImplByPlanExpr(builder.GetContext(), "isnottrue", []*plan.Expr{cond})
}
