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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func bindAndOptimizeMergeQuery(ctx CompilerContext, stmt *tree.Merge, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	return buildIcebergMergePlan(stmt, ctx, isPrepareStmt)
}

func buildIcebergMergePlan(stmt *tree.Merge, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	target, err := resolveIcebergMergeTarget(ctx, stmt)
	if err != nil {
		return nil, err
	}
	if err := validateIcebergMergeStatement(ctx, stmt); err != nil {
		return nil, err
	}
	clauses, err := icebergMergePlanClauses(ctx, stmt)
	if err != nil {
		return nil, err
	}
	builder := NewQueryBuilder(planpb.Query_MERGE, ctx, isPrepareStmt, false)
	builder.zeroScalarPolicy = zeroScalarRetainOnly
	// MERGE actions are writes: the matched UPDATE assignments and the
	// not-matched INSERT values bind under the statement's write conversion
	// policy before their expressions hit the scalar cast path. MERGE syntax
	// carries no statement-level ignore bit, so only the write provenance is
	// recorded here: strict sessions keep the typed overflow rejection and
	// loose sessions clip at the cast's own bound with its warning.
	builder.isBindingDMLWrite = true
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	selectExprs, mergeShape, err := icebergMergeSelectExprs(ctx, target, clauses, bindCtx)
	if err != nil {
		return nil, err
	}
	joinType := tree.JOIN_TYPE_INNER
	joinLeft := stmt.Target
	joinRight := stmt.Source
	if clauses.hasNotMatched() {
		joinType = tree.JOIN_TYPE_LEFT
		joinLeft = stmt.Source
		joinRight = stmt.Target
	}
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: selectExprs,
			From: &tree.From{Tables: tree.TableExprs{&tree.JoinTableExpr{
				Left:     joinLeft,
				Right:    joinRight,
				JoinType: joinType,
				Cond:     &tree.OnJoinCond{Expr: stmt.On},
			}}},
		},
		With: stmt.With,
	}
	// SCRATCH glm79 (review #6031303617): register the clause payload slots
	// for the narrow pre-flatten dependent admission. Only these select-list
	// indices are marked; every other SELECT/ON/WHEN shape is untouched.
	payloadSlots := make(map[int]bool)
	for _, proj := range mergeShape.matched {
		if proj.valueStart < 0 {
			continue
		}
		for colIdx := 0; colIdx < mergeShape.tableColumnCount; colIdx++ {
			payloadSlots[proj.valueStart+colIdx] = true
		}
	}
	for _, proj := range mergeShape.notMatched {
		if proj.valueStart < 0 {
			continue
		}
		for colIdx := 0; colIdx < mergeShape.tableColumnCount; colIdx++ {
			payloadSlots[proj.valueStart+colIdx] = true
		}
	}
	bindCtx.icebergMergePayloadSlots = payloadSlots
	bindCtx.icebergMergePayloadActive = false
	lastNodeID, err := builder.bindSelect(selectStmt, bindCtx, false)
	bindCtx.icebergMergePayloadSlots = nil
	bindCtx.icebergMergePayloadActive = false
	if err != nil {
		return nil, err
	}

	insertNode := &planpb.Node{
		NodeType:     planpb.Node_INSERT,
		Children:     []int32{lastNodeID},
		ObjRef:       target.objRef,
		TableDef:     target.tableDef,
		BindingTags:  append([]int32(nil), builder.qry.Nodes[lastNodeID].BindingTags...),
		ExtraOptions: icebergapi.DMLMergePlanExtraOptions,
		InsertCtx: &planpb.InsertCtx{
			Ref:             target.objRef,
			AddAffectedRows: true,
			TableDef:        target.tableDef,
		},
		ProjectList: getProjectionByLastNodeWithTag(builder, lastNodeID, 0),
	}
	lastNodeID = builder.appendNode(insertNode, bindCtx)
	builder.appendStep(lastNodeID)

	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	dmlTableDef, err := appendIcebergDMLMetadataProjection(ctx.GetContext(), query, query.Steps[query.MainOutputStep()], clauses.hasNotMatched(), true)
	if err != nil {
		return nil, err
	}
	if err := rewriteIcebergMergeProjection(ctx, query, clauses, mergeShape, dmlTableDef); err != nil {
		return nil, err
	}
	query.StmtType = planpb.Query_MERGE
	return &Plan{Plan: &planpb.Plan_Query{Query: query}}, nil
}

func resolveIcebergMergeTarget(ctx CompilerContext, stmt *tree.Merge) (icebergDeleteTarget, error) {
	if stmt == nil {
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE requires a statement")
	}
	tbl := stmt.Target
	target := icebergDeleteTarget{}
	if aliasTbl, ok := tbl.(*tree.AliasedTableExpr); ok {
		target.alias = string(aliasTbl.As.Alias)
		tbl = aliasTbl.Expr
	}
	for {
		if paren, ok := tbl.(*tree.ParenTableExpr); ok {
			tbl = paren.Expr
			continue
		}
		break
	}
	tableName, ok := tbl.(*tree.TableName)
	if !ok {
		return icebergDeleteTarget{}, moerr.NewNotSupported(ctx.GetContext(), "Iceberg MERGE requires a base table target")
	}
	dbName := string(tableName.SchemaName)
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}
	target.tableName = string(tableName.ObjectName)
	if strings.TrimSpace(target.tableName) == "" {
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE requires a table name")
	}
	objRef, tableDef, err := ctx.Resolve(dbName, target.tableName, nil, nil)
	if err != nil {
		return icebergDeleteTarget{}, err
	}
	if tableDef == nil {
		return icebergDeleteTarget{}, moerr.NewNoSuchTable(ctx.GetContext(), dbName, target.tableName)
	}
	isIceberg, err := IsIcebergTableDef(ctx.GetContext(), tableDef)
	if err != nil {
		return icebergDeleteTarget{}, err
	}
	if !isIceberg {
		return icebergDeleteTarget{}, moerr.NewNotSupported(ctx.GetContext(), "MERGE INTO currently supports only Iceberg table mappings")
	}
	target.objRef = objRef
	target.tableDef = tableDef
	return target, nil
}

func validateIcebergMergeStatement(ctx CompilerContext, stmt *tree.Merge) error {
	if stmt.Source == nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE requires a source relation")
	}
	if stmt.On == nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE requires an ON condition")
	}
	if len(stmt.Clauses) == 0 {
		return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE requires at least one action clause")
	}
	for _, clause := range stmt.Clauses {
		if clause == nil {
			return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE action clause is empty")
		}
		switch clause.Action {
		case tree.MergeActionUpdate:
			if !clause.Matched {
				return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE UPDATE action requires MATCHED")
			}
			if len(clause.UpdateExprs) == 0 {
				return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE UPDATE action requires assignments")
			}
		case tree.MergeActionDelete:
			if !clause.Matched {
				return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE DELETE action requires MATCHED")
			}
		case tree.MergeActionInsert:
			if clause.Matched {
				return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE INSERT action requires NOT MATCHED")
			}
			if len(clause.InsertValues) == 0 {
				return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE INSERT action requires values")
			}
		default:
			return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE action is unsupported")
		}
	}
	return nil
}

type icebergMergePlanClauseSet struct {
	matched    []*tree.MergeClause
	notMatched []*tree.MergeClause
}

func (s icebergMergePlanClauseSet) hasNotMatched() bool {
	return len(s.notMatched) > 0
}

type icebergMergeProjectionShape struct {
	tableColumnCount    int
	preMetadataCount    int
	matchedDefaultStart int
	matched             []icebergMergeClauseProjection
	notMatched          []icebergMergeClauseProjection
}

type icebergMergeClauseProjection struct {
	clause       *tree.MergeClause
	valueStart   int
	conditionPos int
}

func icebergMergePlanClauses(ctx CompilerContext, stmt *tree.Merge) (icebergMergePlanClauseSet, error) {
	var out icebergMergePlanClauseSet
	if len(stmt.Clauses) == 0 {
		return out, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE requires an action clause")
	}
	for _, clause := range stmt.Clauses {
		if clause == nil {
			return out, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE action clause is empty")
		}
		if clause.Matched {
			if clause.Action != tree.MergeActionUpdate && clause.Action != tree.MergeActionDelete {
				return out, moerr.NewNotSupported(ctx.GetContext(), "Iceberg MERGE currently supports MATCHED UPDATE or DELETE")
			}
			out.matched = append(out.matched, clause)
			continue
		}
		if clause.Action != tree.MergeActionInsert {
			return out, moerr.NewNotSupported(ctx.GetContext(), "Iceberg MERGE currently supports NOT MATCHED INSERT only")
		}
		out.notMatched = append(out.notMatched, clause)
	}
	if len(out.matched) == 0 && len(out.notMatched) == 0 {
		return out, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE requires an action clause")
	}
	return out, nil
}

func icebergMergeSelectExprs(ctx CompilerContext, target icebergDeleteTarget, clauses icebergMergePlanClauseSet, bindCtx *BindContext) ([]tree.SelectExpr, icebergMergeProjectionShape, error) {
	shape := icebergMergeProjectionShape{
		matchedDefaultStart: -1,
	}
	defaultExprs := icebergDeleteSelectExprs(target, bindCtx)
	shape.tableColumnCount = len(defaultExprs)
	shape.matchedDefaultStart = 0
	out := append([]tree.SelectExpr(nil), defaultExprs...)

	for _, clause := range clauses.matched {
		proj := icebergMergeClauseProjection{
			clause:     clause,
			valueStart: -1,
		}
		if clause.Action == tree.MergeActionUpdate {
			matchedExprs, err := icebergMergeMatchedSelectExprs(ctx, target, clause, bindCtx)
			if err != nil {
				return nil, shape, err
			}
			if len(matchedExprs) != shape.tableColumnCount {
				return nil, shape, moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE UPDATE produced %d values for %d target columns", len(matchedExprs), shape.tableColumnCount)
			}
			proj.valueStart = len(out)
			out = append(out, matchedExprs...)
		}
		proj.conditionPos = len(out)
		out = append(out, tree.SelectExpr{Expr: icebergMergeClauseConditionOrTrue(clause)})
		shape.matched = append(shape.matched, proj)
	}

	for _, clause := range clauses.notMatched {
		proj := icebergMergeClauseProjection{
			clause: clause,
		}
		insertExprs, err := icebergMergeInsertSelectExprs(ctx, target, clause)
		if err != nil {
			return nil, shape, err
		}
		if len(insertExprs) != shape.tableColumnCount {
			return nil, shape, moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE INSERT produced %d values for %d target columns", len(insertExprs), shape.tableColumnCount)
		}
		proj.valueStart = len(out)
		out = append(out, insertExprs...)
		proj.conditionPos = len(out)
		out = append(out, tree.SelectExpr{Expr: icebergMergeClauseConditionOrTrue(clause)})
		shape.notMatched = append(shape.notMatched, proj)
	}
	shape.preMetadataCount = len(out)
	return out, shape, nil
}

func icebergMergeMatchedSelectExprs(ctx CompilerContext, target icebergDeleteTarget, clause *tree.MergeClause, bindCtx *BindContext) ([]tree.SelectExpr, error) {
	if clause == nil {
		return icebergDeleteSelectExprs(target, bindCtx), nil
	}
	switch clause.Action {
	case tree.MergeActionDelete:
		return icebergDeleteSelectExprs(target, bindCtx), nil
	case tree.MergeActionUpdate:
		updates, err := icebergMergeUpdateExprMap(ctx, target, clause.UpdateExprs)
		if err != nil {
			return nil, err
		}
		qualifier := target.alias
		if qualifier == "" {
			qualifier = target.tableName
		}
		out := make([]tree.SelectExpr, 0, len(target.tableDef.Cols))
		for _, col := range target.tableDef.Cols {
			if col == nil || col.Hidden || col.Name == catalog.Row_ID || col.Name == catalog.ExternalFilePath {
				continue
			}
			if expr, ok := updates[strings.ToLower(col.Name)]; ok {
				out = append(out, tree.SelectExpr{Expr: expr})
				continue
			}
			out = append(out, tree.SelectExpr{
				Expr: tree.NewUnresolvedName(
					tree.NewCStr(qualifier, bindCtx.lower),
					tree.NewCStr(col.Name, 1),
				),
			})
		}
		return out, nil
	default:
		return nil, moerr.NewNotSupported(ctx.GetContext(), "Iceberg MERGE action is not implemented")
	}
}

func icebergMergeClauseConditionOrTrue(clause *tree.MergeClause) tree.Expr {
	if clause != nil && clause.Condition != nil {
		return clause.Condition
	}
	return tree.NewNumVal(true, "true", false, tree.P_bool)
}

func icebergMergeInsertSelectExprs(ctx CompilerContext, target icebergDeleteTarget, clause *tree.MergeClause) ([]tree.SelectExpr, error) {
	visibleCols := icebergMergeVisibleColumns(target.tableDef)
	if len(visibleCols) == 0 {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE INSERT requires target columns")
	}
	valuesByColumn, err := icebergMergeInsertExprMap(ctx, target, visibleCols, clause)
	if err != nil {
		return nil, err
	}
	out := make([]tree.SelectExpr, 0, len(visibleCols))
	for _, col := range visibleCols {
		out = append(out, tree.SelectExpr{Expr: valuesByColumn[strings.ToLower(col.Name)]})
	}
	return out, nil
}

func icebergMergeVisibleColumns(tableDef *planpb.TableDef) []*planpb.ColDef {
	if tableDef == nil {
		return nil
	}
	out := make([]*planpb.ColDef, 0, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col == nil || col.Hidden || col.Name == catalog.Row_ID || col.Name == catalog.ExternalFilePath {
			continue
		}
		out = append(out, col)
	}
	return out
}

func icebergMergeInsertExprMap(ctx CompilerContext, target icebergDeleteTarget, visibleCols []*planpb.ColDef, clause *tree.MergeClause) (map[string]tree.Expr, error) {
	if clause == nil || clause.Action != tree.MergeActionInsert {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE INSERT action is missing")
	}
	out := make(map[string]tree.Expr, len(visibleCols))
	if len(clause.InsertColumns) == 0 {
		if len(clause.InsertValues) != len(visibleCols) {
			return nil, moerr.NewNotSupportedf(ctx.GetContext(), "Iceberg MERGE INSERT without a column list requires values for every target column: values=%d columns=%d", len(clause.InsertValues), len(visibleCols))
		}
		for idx, col := range visibleCols {
			out[strings.ToLower(col.Name)] = clause.InsertValues[idx]
		}
		return out, nil
	}
	if len(clause.InsertColumns) != len(clause.InsertValues) {
		return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE INSERT column/value count mismatch: columns=%d values=%d", len(clause.InsertColumns), len(clause.InsertValues))
	}
	validCols := make(map[string]string, len(visibleCols))
	for _, col := range visibleCols {
		validCols[strings.ToLower(col.Name)] = col.Name
	}
	for idx, ident := range clause.InsertColumns {
		colKey := strings.ToLower(strings.TrimSpace(string(ident)))
		if colKey == "" {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE INSERT column name is empty")
		}
		if _, ok := validCols[colKey]; !ok {
			return nil, moerr.NewBadFieldError(ctx.GetContext(), string(ident), target.tableName)
		}
		if _, exists := out[colKey]; exists {
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE INSERT column %s appears more than once", ident)
		}
		out[colKey] = clause.InsertValues[idx]
	}
	for _, col := range visibleCols {
		colKey := strings.ToLower(col.Name)
		if _, ok := out[colKey]; ok {
			continue
		}
		out[colKey] = tree.NewDefaultVal(nil)
	}
	return out, nil
}

func icebergMergeUpdateExprMap(ctx CompilerContext, target icebergDeleteTarget, exprs tree.UpdateExprs) (map[string]tree.Expr, error) {
	if len(exprs) == 0 {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE UPDATE action requires assignments")
	}
	qualifier := target.alias
	if qualifier == "" {
		qualifier = target.tableName
	}
	validCols := make(map[string]string, len(target.tableDef.Cols))
	for _, col := range target.tableDef.Cols {
		if col == nil || col.Hidden || col.Name == catalog.Row_ID || col.Name == catalog.ExternalFilePath {
			continue
		}
		validCols[strings.ToLower(col.Name)] = col.Name
	}
	updates := make(map[string]tree.Expr, len(exprs))
	for _, assignment := range exprs {
		if assignment == nil || assignment.Tuple || len(assignment.Names) != 1 {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "Iceberg MERGE UPDATE currently supports one column per assignment")
		}
		name := assignment.Names[0]
		if name == nil {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE UPDATE assignment is missing a column name")
		}
		if tbl := strings.TrimSpace(name.TblName()); tbl != "" && !strings.EqualFold(tbl, qualifier) && !strings.EqualFold(tbl, target.tableName) {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "Iceberg MERGE UPDATE assignment references a different target table")
		}
		colKey := strings.ToLower(strings.TrimSpace(name.ColName()))
		if colKey == "" {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE UPDATE assignment is missing a column name")
		}
		colName, ok := validCols[colKey]
		if !ok {
			return nil, moerr.NewBadFieldError(ctx.GetContext(), name.ColNameOrigin(), target.tableName)
		}
		if _, exists := updates[colKey]; exists {
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE UPDATE assigns column %s more than once", colName)
		}
		updates[colKey] = assignment.Expr
	}
	return updates, nil
}

func rewriteIcebergMergeProjection(ctx CompilerContext, query *planpb.Query, clauses icebergMergePlanClauseSet, shape icebergMergeProjectionShape, dmlTableDef *planpb.TableDef) error {
	if query == nil || len(query.Steps) == 0 {
		return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE plan requires a query root")
	}
	root := query.Nodes[query.Steps[query.MainOutputStep()]]
	if root == nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE plan root is missing")
	}
	tableDef := DeepCopyTableDef(dmlTableDef, true)
	actionCol := &planpb.ColDef{
		Name: icebergapi.DMLMergeActionColumnName,
		Typ: planpb.Type{
			Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Table: tableDef.Name,
			Charset: uint32(types.CharsetUTF8),
		},
	}
	tableDef.Cols = append(tableDef.Cols, actionCol)
	root.TableDef = tableDef
	if root.InsertCtx != nil {
		root.InsertCtx.TableDef = tableDef
	}
	preProjects := clonePlanExprs(root.ProjectList)
	metadataStart := len(preProjects) - 2
	if shape.tableColumnCount <= 0 || metadataStart+1 >= len(preProjects) {
		return moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE projection shape is invalid: %s projects=%d", shape.String(), len(preProjects))
	}
	writeCols := icebergDMLWriteColumns(tableDef)
	if len(writeCols) != shape.tableColumnCount {
		return moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE write column shape is invalid: columns=%d shape=%s", len(writeCols), shape.String())
	}
	// The merge root's projected clause values alias the child projections'
	// clause-value slots, and the reachable normalization below assigns each
	// of those aliased slots its final write-column conversion exactly once.
	// The alias references must therefore describe the aliased slot's
	// post-normalization value: a column reference whose stale declared type
	// still shows the pre-normalization expression (for example a radix write
	// chain still typed at its inner cast's own DECIMAL bound) would
	// otherwise make the root-level forced assignment cast re-rescale the
	// once-converted value by another power of ten. Synchronize only the
	// alias references' declared type here - the value itself keeps its
	// single evaluation in the aliased child slot.
	syncClauseAliasTypes(shape, preProjects, writeCols)
	if err := normalizeIcebergMergeClauseValueProjects(ctx, preProjects, shape, writeCols, dmlTableDef.DbName); err != nil {
		return err
	}
	if err := normalizeIcebergMergeReachableClauseValueProjects(ctx, query, root.Children, shape, writeCols, dmlTableDef.DbName); err != nil {
		return err
	}
	// V1 scratch proof (review #6031303617): make the row select its action
	// before that action's values evaluate. The single-evaluation payload
	// chains live in the select's deepest spine PROJECT above the join; move
	// each clause chain, with its already-assigned write conversion, under a
	// row-local guard built from the physical target-side data-file identity
	// (null exactly under the left-join null extension) and the clause's own
	// bound WHEN slot, then blank the eager lower producer. Unselected
	// clauses evaluate nothing; selected payloads keep today's typed
	// behavior. Scoped: consumer-visible scalar payload slots only.
	wTyp, err := rowSelectIcebergMergeClauseValues(ctx, query, root, shape, preProjects, metadataStart)
	if err != nil {
		return err
	}
	if wTyp == nil {
		return moerr.NewInternalError(ctx.GetContext(), "Iceberg MERGE winner step did not publish its ordinal")
	}
	// Forward the typed ordinal through the preProject as ONE trailing slot:
	// every existing offset (payload aliases, file/ordinal metadata) keeps
	// its position, and the compact writer reads W as a real input ref.
	winnerPos := 0
	if len(root.Children) == 1 && root.Children[0] >= 0 && int(root.Children[0]) < len(query.Nodes) {
		winnerPos = len(query.Nodes[root.Children[0]].ProjectList) - 1
	}
	wForward := &planpb.Expr{
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: int32(winnerPos)}},
		Typ:  *wTyp,
	}
	preProjects = append(preProjects, wForward)
	inputProjects := icebergProjectOutputRefs(preProjects)
	wRef := DeepCopyExpr(inputProjects[len(inputProjects)-1])
	finalProjects := make([]*planpb.Expr, 0, shape.tableColumnCount+2+1)
	for idx := 0; idx < shape.tableColumnCount; idx++ {
		expr, err := icebergMergeReplacementExpr(ctx, inputProjects, shape, wRef, writeCols[idx], idx, dmlTableDef.DbName)
		if err != nil {
			return err
		}
		finalProjects = append(finalProjects, expr)
	}
	finalProjects = append(finalProjects, DeepCopyExpr(inputProjects[metadataStart]), DeepCopyExpr(inputProjects[metadataStart+1]))
	actionExpr, err := icebergMergeActionExpr(ctx, shape, wRef)
	if err != nil {
		return err
	}
	finalProjects = append(finalProjects, actionExpr)
	preProjectID := int32(-1)
	if len(root.Children) == 1 {
		childID := root.Children[0]
		if childID >= 0 && int(childID) < len(query.Nodes) {
			if projectNode := query.Nodes[childID]; projectNode != nil &&
				projectNode.NodeType == planpb.Node_PROJECT &&
				len(projectNode.ProjectList) == len(preProjects) {
				preProjectID = childID
			}
		}
	}
	if preProjectID >= 0 && int(preProjectID) == int(root.Children[0]) {
		// the candidate same-width child is the guarded producer itself:
		// never overwrite its guarded slot list; create the wrapper instead
		preProjectID = -1
	}
	if preProjectID < 0 {
		preProjectNode := &planpb.Node{
			NodeType:    planpb.Node_PROJECT,
			Children:    append([]int32(nil), root.Children...),
			ProjectList: preProjects,
			Stats:       DefaultStats(),
		}
		preProjectID = int32(len(query.Nodes))
		query.Nodes = append(query.Nodes, preProjectNode)
	} else {
		query.Nodes[preProjectID].ProjectList = preProjects
	}
	// finalProjects are built against the full MERGE select output, so the
	// child PROJECT must first materialize preProjects before the compact
	// Iceberg DML output is computed.
	projectNode := &planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{preProjectID},
		ProjectList: finalProjects,
		Stats:       DefaultStats(),
	}
	projectID := int32(len(query.Nodes))
	query.Nodes = append(query.Nodes, projectNode)
	root.Children = []int32{projectID}
	root.ProjectList = finalProjects
	return nil
}

func icebergProjectOutputRefs(projects []*planpb.Expr) []*planpb.Expr {
	out := make([]*planpb.Expr, len(projects))
	for idx, expr := range projects {
		typ := planpb.Type{}
		if expr != nil {
			typ = expr.Typ
		}
		out[idx] = &planpb.Expr{
			Typ: typ,
			Expr: &planpb.Expr_Col{
				Col: &planpb.ColRef{
					RelPos: 0,
					ColPos: int32(idx),
				},
			},
		}
	}
	return out
}

func normalizeIcebergMergeReachableClauseValueProjects(ctx CompilerContext, query *planpb.Query, roots []int32, shape icebergMergeProjectionShape, targetCols []*planpb.ColDef, ownerDb string) error {
	seen := make(map[int32]struct{})
	var visit func(int32) error
	visit = func(nodeID int32) error {
		if nodeID < 0 || query == nil || int(nodeID) >= len(query.Nodes) {
			return nil
		}
		if _, ok := seen[nodeID]; ok {
			return nil
		}
		seen[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node == nil {
			return nil
		}
		if node.NodeType == planpb.Node_PROJECT && len(node.ProjectList) >= shape.preMetadataCount {
			if err := normalizeIcebergMergeClauseValueProjects(ctx, node.ProjectList, shape, targetCols, ownerDb); err != nil {
				return err
			}
		}
		for _, childID := range node.Children {
			if err := visit(childID); err != nil {
				return err
			}
		}
		return nil
	}
	for _, nodeID := range roots {
		if err := visit(nodeID); err != nil {
			return err
		}
	}
	return nil
}

// syncClauseAliasTypes re-types the clause-value project slots that are
// plain column references to the aliased child clause-value slots. The
// reachable normalization supplies each aliased slot exactly one write-column
// assignment conversion, so the reference's declared type must reflect the
// converted value, not the pre-normalization expression it was bound from.
// Non-reference expressions keep their own binding and receive the ordinary
// forced conversion in the normalize pass.
func syncClauseAliasTypes(shape icebergMergeProjectionShape, projects []*planpb.Expr, targetCols []*planpb.ColDef) {
	syncClauseValueRange := func(valueStart int, clauseCount int) {
		if valueStart < 0 {
			return
		}
		for colIdx := 0; colIdx < clauseCount; colIdx++ {
			pos := valueStart + colIdx
			if pos < 0 || pos >= len(projects) || colIdx >= len(targetCols) {
				continue
			}
			ref := projects[pos]
			if ref == nil || ref.GetCol() == nil || ref.GetCol().GetColPos() != int32(pos) {
				// Not the merge spine's position-aligned alias to its own
				// clause-value producer slot: a raw source column reference or
				// a projected expression keeps its own binding and receives the
				// ordinary forced conversion below.
				continue
			}
			// Retyping eligibility is restricted to the proven same-decimal
			// stale-scale case: the alias must already carry the write
			// column's own decimal identity and only its width/scale went
			// stale against the downstream normalized bound. Any other
			// aligned value - including a text expression whose coherent
			// conversion the ordinary forced assignment path owns - keeps its
			// own binding; its runtime materialization boundary is evidenced
			// pre-existing on the no-alias-sync baseline and is deliberately
			// not touched here.
			target := targetCols[colIdx].Typ
			if target.Id != int32(types.T_decimal64) && target.Id != int32(types.T_decimal128) {
				continue
			}
			if ref.Typ.Id != target.Id {
				continue
			}
			// The position-aligned alias references exactly the producer
			// slot the reachable clause normalization converts once. Its
			// declared type must be that converted value's own coherent
			// physical type - identity, width, scale and the numeric
			// family's metadata come from the converted bound, so a text
			// source cannot strand its string metadata (collation) on a
			// non-string identity and the runtime reads the aliased bytes
			// with the layout the converted producer actually materialized.
			// The expression-level nullability stays with the referenced
			// value: the assignment conversion derives it from its real
			// input, and copying the destination schema flag here would
			// defeat constraint handling.
			synced := targetCols[colIdx].Typ
			synced.NotNullable = ref.Typ.NotNullable
			ref.Typ = synced
		}
	}
	for _, proj := range shape.matched {
		if proj.clause == nil || proj.clause.Action != tree.MergeActionUpdate || proj.valueStart < 0 {
			continue
		}
		syncClauseValueRange(proj.valueStart, int(shape.tableColumnCount))
	}
	for _, proj := range shape.notMatched {
		if proj.valueStart < 0 {
			continue
		}
		syncClauseValueRange(proj.valueStart, int(shape.tableColumnCount))
	}
}

func normalizeIcebergMergeClauseValueProjects(ctx CompilerContext, projects []*planpb.Expr, shape icebergMergeProjectionShape, targetCols []*planpb.ColDef, ownerDb string) error {
	for _, proj := range shape.matched {
		if proj.clause == nil || proj.clause.Action != tree.MergeActionUpdate || proj.valueStart < 0 {
			continue
		}
		for colIdx, col := range targetCols {
			pos := proj.valueStart + colIdx
			if pos < 0 || pos >= len(projects) {
				return moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE UPDATE value index is invalid: pos=%d projects=%d", pos, len(projects))
			}
			value, err := icebergMergeAssignmentValueExpr(ctx, projects[pos], col, ownerDb)
			if err != nil {
				return err
			}
			projects[pos] = value
		}
	}
	for _, proj := range shape.notMatched {
		if proj.valueStart < 0 {
			continue
		}
		for colIdx, col := range targetCols {
			pos := proj.valueStart + colIdx
			if pos < 0 || pos >= len(projects) {
				return moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE INSERT value index is invalid: pos=%d projects=%d", pos, len(projects))
			}
			value, err := icebergMergeAssignmentValueExpr(ctx, projects[pos], col, ownerDb)
			if err != nil {
				return err
			}
			projects[pos] = value
		}
	}
	return nil
}

func icebergMergeReplacementExpr(ctx CompilerContext, projects []*planpb.Expr, shape icebergMergeProjectionShape, wRef *planpb.Expr, targetCol *planpb.ColDef, colIdx int, ownerDb string) (*planpb.Expr, error) {
	// ONE writer decision: every replacement reads the winner's ordinal,
	// using exactly the clause order the winner arbitrated. A DELETE (or a
	// clause without a payload) keeps the ORIGINAL row value - it never
	// evaluates any later UPDATE payload - and the tail keeps the original
	// for the genuine no-op row.
	base := DeepCopyExpr(projects[shape.matchedDefaultStart+colIdx])
	value := base
	ordered := icebergMergeOrderedClauses(shape)
	for idx := len(ordered) - 1; idx >= 0; idx-- {
		wc := ordered[idx]
		var arm *planpb.Expr
		var err error
		if wc.valueStart >= 0 && (wc.clause.Action == tree.MergeActionUpdate || wc.clause.Action == tree.MergeActionInsert) {
			arm, err = icebergMergeAssignmentValueExpr(ctx, projects[wc.valueStart+colIdx], targetCol, ownerDb)
			if err != nil {
				return nil, err
			}
		} else {
			arm = DeepCopyExpr(base)
		}
		kExpr := makePlan2Int64ConstExprWithType(int64(wc.ordinal))
		eqExpr, err2 := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{DeepCopyExpr(wRef), kExpr})
		if err2 != nil {
			return nil, err2
		}
		value, err2 = BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{eqExpr, arm, value})
		if err2 != nil {
			return nil, err2
		}
	}
	return value, nil
}

func icebergMergeAssignmentValueExpr(ctx CompilerContext, expr *planpb.Expr, targetCol *planpb.ColDef, ownerDb string) (*planpb.Expr, error) {
	if targetCol == nil {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE assignment target column is missing")
	}
	value := DeepCopyExpr(expr)
	var err error
	if isDefaultValExpr(value) {
		value, err = getDefaultExprForAssignment(ctx.GetContext(), targetCol, ownerDb, ctx.GetProcess(), false)
		if err != nil {
			return nil, err
		}
	}
	return forceAssignmentCastExprWithProcess(ctx.GetContext(), value, targetCol.Typ, false, ctx.GetProcess())
}

func icebergMergeActionExpr(ctx CompilerContext, shape icebergMergeProjectionShape, wRef *planpb.Expr) (*planpb.Expr, error) {
	// ONE writer decision: the action tag reads the same winner ordinal,
	// in the same clause order, so the tag and every replacement value share
	// one arbitration. The tail is the genuine no-op.
	actionExpr := makePlan2StringConstExprWithType(icebergapi.DMLMergeActionNoop)
	ordered := icebergMergeOrderedClauses(shape)
	for idx := len(ordered) - 1; idx >= 0; idx-- {
		wc := ordered[idx]
		kExpr := makePlan2Int64ConstExprWithType(int64(wc.ordinal))
		eqExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{DeepCopyExpr(wRef), kExpr})
		if err != nil {
			return nil, err
		}
		actionExpr, err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{
			eqExpr,
			makePlan2StringConstExprWithType(icebergMergeActionForClause(wc.clause)),
			actionExpr,
		})
		if err != nil {
			return nil, err
		}
	}
	return actionExpr, nil
}

func icebergMergeActionForClause(clause *tree.MergeClause) string {
	if clause == nil {
		return icebergapi.DMLMergeActionNoop
	}
	// The winner rework routes EVERY clause's tag through this helper, so the
	// not-matched INSERT class must map to the INSERT action: the pre-winner
	// builder hard-coded DMLMergeActionInsert in its notMatched arm, and the
	// fall-through to UPDATE silently relabeled the unmatched rows as
	// phantom update writes (measured: run2's id=3 row tagged "update" with
	// the correct 5060 payload and no physical row identity).
	if clause.Action == tree.MergeActionDelete {
		return icebergapi.DMLMergeActionDelete
	}
	if clause.Action == tree.MergeActionInsert {
		return icebergapi.DMLMergeActionInsert
	}
	return icebergapi.DMLMergeActionUpdate
}

func (shape icebergMergeProjectionShape) String() string {
	return fmt.Sprintf("table_columns=%d pre_metadata=%d matched_default_start=%d matched_clauses=%d not_matched_clauses=%d", shape.tableColumnCount, shape.preMetadataCount, shape.matchedDefaultStart, len(shape.matched), len(shape.notMatched))
}

// icebergMergeNullForSlot returns a same-typed NULL literal for a producer
// slot blanked by the row-local guard.
func icebergMergeNullForSlot(typ planpb.Type) *planpb.Expr {
	nullExpr := makePlan2NullConstExprWithType()
	nullExpr.Typ = DeepCopyExpr(&planpb.Expr{Typ: typ}).Typ
	return nullExpr
}

// rowSelectIcebergMergeClauseValues guards and blanks each clause's
// consumer-visible payload producer so the payload evaluates only on rows
// whose class and WHEN condition select that clause. V1 scratch scope.
// icebergMergeOrderedClause mirrors the winner's arbitration order exactly:
// every clause consumes ONE ordinal - matched clauses (UPDATE and DELETE
// alike) in their slice order, then the not-matched INSERT clauses. The two
// classes are row-exclusive, so on-disk interleaving cannot change a row's
// arbitration.
type icebergMergeOrderedClause struct {
	ordinal    int
	clause     *tree.MergeClause
	valueStart int
}

// icebergMergeOrderedClauses enumerates the clauses in winner order. The
// returned exprs are resolved independently per use: the winner node's own
// inputs and the writer arms both read materialized refs, not each other.
func icebergMergeOrderedClauses(shape icebergMergeProjectionShape) []icebergMergeOrderedClause {
	out := make([]icebergMergeOrderedClause, 0, len(shape.matched)+len(shape.notMatched))
	for _, proj := range shape.matched {
		if proj.clause == nil {
			continue
		}
		out = append(out, icebergMergeOrderedClause{
			ordinal:    len(out) + 1,
			clause:     proj.clause,
			valueStart: proj.valueStart,
		})
	}
	for _, proj := range shape.notMatched {
		if proj.clause == nil {
			continue
		}
		out = append(out, icebergMergeOrderedClause{
			ordinal:    len(out) + 1,
			clause:     proj.clause,
			valueStart: proj.valueStart,
		})
	}
	return out
}

func rowSelectIcebergMergeClauseValues(ctx CompilerContext, query *planpb.Query, root *planpb.Node, shape icebergMergeProjectionShape, preProjects []*planpb.Expr, metadataStart int) (*planpb.Type, error) {
	// V1..V2 scratch proof, review #6031303617. SCRATCH-ONLY; NOT production
	// accepted. This revision replaces the per-arm "class AND own predicate"
	// skeleton with ONE distinct winner producer: a grafted PROJECT step below
	// the merge select computes the row's first-applicable clause ordinal W
	// (ordered clauses, class-gated, NULL/false WHEN = nonmatch, DELETE
	// clauses consume ordinals), and every consumer-visible payload slot is
	// masked as if(W = k, <chain as bound>, same-typed NULL) - so a row that
	// an earlier WHEN already won never evaluates a later clause's payload.
	// Marked boundaries: (1) WHEN conditions are supported only when they
	// trace to a literal/folded expression or a producer-input column
	// reference - anything else is an explicit error, never a silent skip;
	// (2) this masks only consumer-visible scalar payload slots: upstream
	// producers hoisted below the join (GLM65 uncorrelated / GLM69
	// equality-correlated families) are NOT suppressed here and stay open;
	// (3) no arbitration of duplicate NodeIds or width guessing is done -
	// positions are taken from the verified single-child chain and traced
	// references only.
	if root == nil || len(root.Children) != 1 {
		return nil, nil
	}
	producer := query.Nodes[root.Children[0]]
	if producer == nil || producer.NodeType != planpb.Node_PROJECT || len(producer.Children) != 1 {
		return nil, nil
	}
	joinNode := query.Nodes[producer.Children[0]]
	if joinNode == nil || len(joinNode.ProjectList) < 1 {
		return nil, nil
	}
	// trace resolves a root-level projected expression through the verified
	// edges (root's child = the producer; the producer's input) and returns
	// either the folded producer-slot expression (valid where the guard
	// runs), or the column reference that slot passes through from the
	// producer's input, plus that column's actual declared type. No
	// arbitrary fallback typing is applied anywhere.
	trace := func(e *planpb.Expr) (*planpb.Expr, *planpb.ColRef, planpb.Type) {
		if e == nil {
			return nil, nil, planpb.Type{}
		}
		col := e.GetCol()
		if col == nil {
			c := DeepCopyExpr(e)
			return c, nil, c.Typ
		}
		if int(col.ColPos) < 0 || int(col.ColPos) >= len(producer.ProjectList) {
			return nil, nil, planpb.Type{}
		}
		slotExpr := producer.ProjectList[col.ColPos]
		if slotExpr == nil {
			return nil, nil, planpb.Type{}
		}
		if col2 := slotExpr.GetCol(); col2 != nil {
			// the slot passes its input column straight through: the
			// reference applies in the producer's input scope with the
			// referenced output slot's real type.
			child := query.Nodes[producer.Children[0]]
			if child == nil || int(col2.ColPos) < 0 || int(col2.ColPos) >= len(child.ProjectList) || child.ProjectList[col2.ColPos] == nil {
				return nil, nil, planpb.Type{}
			}
			return nil, col2, child.ProjectList[col2.ColPos].Typ
		}
		c := DeepCopyExpr(slotExpr)
		return c, nil, c.Typ
	}
	// Class discriminator: the target side's physical data-file identity,
	// traced from the root's metadata slot; null exactly under the left-join
	// null extension, so a genuine matched row never reads as null.
	_, fileCol, fileTyp := trace(preProjects[metadataStart])
	if fileCol == nil {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "Iceberg MERGE winner step requires a traceable target data-file identity")
	}
	classRef := &planpb.Expr{
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: fileCol.RelPos, ColPos: fileCol.ColPos}},
		Typ:  fileTyp,
	}
	unmatchedClass, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "isnull", []*planpb.Expr{classRef})
	if err != nil {
		return nil, err
	}
	matchedClass, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "not", []*planpb.Expr{DeepCopyExpr(unmatchedClass)})
	if err != nil {
		return nil, err
	}
	// Ordered clause list: every clause consumes an ordinal, including
	// DELETE clauses (a winning DELETE suppresses later clause payloads).
	// matched and not-matched classes are row-exclusive, so their statement
	// interleaving cannot change per-row arbitration.
	type winnerClause struct {
		ordinal      int
		class        *planpb.Expr
		cond         *planpb.Expr
		valueStart   int
		conditionPos int
	}
	var ordered []winnerClause
	appendClause := func(class *planpb.Expr, valueStart, conditionPos int) error {
		wc := winnerClause{ordinal: len(ordered) + 1, class: DeepCopyExpr(class), valueStart: valueStart, conditionPos: conditionPos}
		if conditionPos >= len(preProjects) || (conditionPos >= 0 && preProjects[conditionPos] == nil) {
			return moerr.NewInternalErrorf(ctx.GetContext(), "Iceberg MERGE winner step cannot read clause %d WHEN slot", wc.ordinal)
		}
		if conditionPos >= 0 {
			condExpr, condCol, condTyp := trace(preProjects[conditionPos])
			if condExpr == nil && condCol == nil {
				return moerr.NewInternalErrorf(ctx.GetContext(), "Iceberg MERGE winner step cannot trace clause %d WHEN to a producer input", wc.ordinal)
			}
			if condCol != nil {
				wc.cond = &planpb.Expr{
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: condCol.RelPos, ColPos: condCol.ColPos}},
					Typ:  condTyp,
				}
			} else {
				wc.cond = condExpr
			}
		}
		ordered = append(ordered, wc)
		return nil
	}
	for _, proj := range shape.matched {
		if proj.clause == nil {
			continue
		}
		if err := appendClause(matchedClass, proj.valueStart, proj.conditionPos); err != nil {
			return nil, err
		}
	}
	for _, proj := range shape.notMatched {
		if proj.clause == nil {
			continue
		}
		if err := appendClause(unmatchedClass, proj.valueStart, proj.conditionPos); err != nil {
			return nil, err
		}
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	// W = the first-applicable ordinal as a nested if chain (false and NULL
	// both fall through) ending in the no-op ordinal 0.
	wExpr := makePlan2Int64ConstExprWithType(0)
	for idx := len(ordered) - 1; idx >= 0; idx-- {
		wc := ordered[idx]
		active := DeepCopyExpr(wc.class)
		if wc.cond != nil {
			andExpr, err2 := BindFuncExprImplByPlanExpr(ctx.GetContext(), "and", []*planpb.Expr{active, DeepCopyExpr(wc.cond)})
			if err2 != nil {
				return nil, err2
			}
			active = andExpr
		}
		kExpr := makePlan2Int64ConstExprWithType(int64(wc.ordinal))
		branch, err2 := BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{active, kExpr, wExpr})
		if err2 != nil {
			return nil, err2
		}
		wExpr = branch
	}
	// The distinct winner producer: a PROJECT step between the join and the
	// merge select, passing the entire join row through positionally and
	// publishing W as one appended slot, so every upper reference keeps its
	// coordinate and W is read as a real input column (no sibling reads).
	joinWidth := len(joinNode.ProjectList)
	pass := make([]*planpb.Expr, joinWidth, joinWidth+1)
	for i := 0; i < joinWidth; i++ {
		src := joinNode.ProjectList[i]
		srcRef := &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: int32(i)}}
		out := &planpb.Expr{Expr: srcRef}
		if src != nil {
			out.Typ = src.Typ
		}
		pass[i] = out
	}
	pass = append(pass, DeepCopyExpr(wExpr))
	winnerNode := &planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{producer.Children[0]},
		ProjectList: pass,
		Stats:       DefaultStats(),
	}
	winnerID := int32(len(query.Nodes))
	query.Nodes = append(query.Nodes, winnerNode)
	producer.Children[0] = winnerID
	// Each consumer-visible payload slot reads the published W at position
	// joinWidth and evaluates only when the row selected that exact clause.
	wTyp := DeepCopyExpr(wExpr).Typ
	wRef := &planpb.Expr{
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: int32(joinWidth)}},
		Typ:  wTyp,
	}
	payloadAtProducer := make(map[int]bool)
	for _, wc := range ordered {
		if wc.valueStart < 0 {
			continue
		}
		for colIdx := 0; colIdx < shape.tableColumnCount; colIdx++ {
			slot := wc.valueStart + colIdx
			payloadAtProducer[slot] = true
			if slot < 0 || slot >= len(producer.ProjectList) || producer.ProjectList[slot] == nil {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "Iceberg MERGE winner step cannot mask admitted clause %d payload slot %d", wc.ordinal, slot)
			}
			chain := producer.ProjectList[slot]
			kExpr := makePlan2Int64ConstExprWithType(int64(wc.ordinal))
			eqExpr, err2 := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{DeepCopyExpr(wRef), kExpr})
			if err2 != nil {
				return nil, err2
			}
			guarded, err2 := BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{eqExpr, chain, icebergMergeNullForSlot(chain.Typ)})
			if err2 != nil {
				return nil, err2
			}
			// Plan-owned guarded-payload admission stamp (review
			// #6031303617): this rewrite is the SOLE authority for
			// guarded-payload memoization membership. Mark every
			// capture-free SCALAR dependent call this writer's winner
			// guards actually coordinate, so the dependent runtime owns an
			// exact planner-authored membership from the finalized plan
			// instead of re-inferring it from expression shapes. The
			// correlation finalization only reassigns
			// OccurrenceId/StageId, so the stamp rides the call through
			// copy, remap and serialization intact; every unstamped call
			// keeps per-row semantics.
			_ = planpb.VisitExprTree(chain, func(e *planpb.Expr) error {
				if call := e.GetDependent(); call != nil &&
					call.Reducer == planpb.SubqueryRef_SCALAR && call.Comparison == nil &&
					len(call.Inputs) == 0 && len(call.Lhs) == 0 {
					call.GuardedMergePayload = true
				}
				return nil
			})
			producer.ProjectList[slot] = guarded
		}
	}
	// ONE condition evaluation where owned: the winner computes each WHEN
	// once per row for arbitration. The clause's prior projection slot only
	// fed that arbitration, so a COMPUTED WHEN (a folded comparison, for
	// example) must not still run from the now-dead producer slot. The slot
	// position and declared type stay (upstream passthrough refs do not
	// shift); only the expression becomes a same-typed NULL.
	for _, wc := range ordered {
		if wc.conditionPos < 0 || wc.conditionPos >= len(preProjects) || preProjects[wc.conditionPos] == nil {
			continue
		}
		col := preProjects[wc.conditionPos].GetCol()
		if col == nil || int(col.ColPos) < 0 || int(col.ColPos) >= len(producer.ProjectList) {
			continue
		}
		condSlot := int(col.ColPos)
		if payloadAtProducer[condSlot] {
			continue
		}
		if slotExpr := producer.ProjectList[condSlot]; slotExpr != nil {
			producer.ProjectList[condSlot] = icebergMergeNullForSlot(slotExpr.Typ)
		}
	}
	// Forward the ONE appended typed ordinal through the producer as a
	// single trailing output slot, preserving every existing offset (the
	// file and row-ordinal metadata included). The writer reads this slot
	// through the preProject passthrough below.
	producer.ProjectList = append(producer.ProjectList, &planpb.Expr{
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: int32(joinWidth)}},
		Typ:  wTyp,
	})
	return &wTyp, nil
}
