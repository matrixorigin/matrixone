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
	lastNodeID, err := builder.bindSelect(selectStmt, bindCtx, false)
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
	dmlTableDef, err := appendIcebergDMLMetadataProjection(ctx.GetContext(), query, query.Steps[len(query.Steps)-1], clauses.hasNotMatched(), true)
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
	objRef, tableDef, err := ctx.Resolve(dbName, target.tableName, nil)
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
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	if root == nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE plan root is missing")
	}
	tableDef := DeepCopyTableDef(dmlTableDef, true)
	actionCol := &planpb.ColDef{
		Name: icebergapi.DMLMergeActionColumnName,
		Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Table: tableDef.Name},
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
	if err := normalizeIcebergMergeClauseValueProjects(ctx, preProjects, shape, writeCols); err != nil {
		return err
	}
	if err := normalizeIcebergMergeReachableClauseValueProjects(ctx, query, root.Children, shape, writeCols); err != nil {
		return err
	}
	inputProjects := icebergProjectOutputRefs(preProjects)
	dataFilePathExpr := DeepCopyExpr(inputProjects[metadataStart])
	isUnmatchedExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "isnull", []*planpb.Expr{dataFilePathExpr})
	if err != nil {
		return err
	}
	finalProjects := make([]*planpb.Expr, 0, shape.tableColumnCount+2+1)
	for idx := 0; idx < shape.tableColumnCount; idx++ {
		expr, err := icebergMergeReplacementExpr(ctx, inputProjects, shape, isUnmatchedExpr, writeCols[idx], idx)
		if err != nil {
			return err
		}
		finalProjects = append(finalProjects, expr)
	}
	finalProjects = append(finalProjects, DeepCopyExpr(inputProjects[metadataStart]), DeepCopyExpr(inputProjects[metadataStart+1]))
	actionExpr, err := icebergMergeActionExpr(ctx, inputProjects, shape, isUnmatchedExpr)
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

func normalizeIcebergMergeReachableClauseValueProjects(ctx CompilerContext, query *planpb.Query, roots []int32, shape icebergMergeProjectionShape, targetCols []*planpb.ColDef) error {
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
			if err := normalizeIcebergMergeClauseValueProjects(ctx, node.ProjectList, shape, targetCols); err != nil {
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

func normalizeIcebergMergeClauseValueProjects(ctx CompilerContext, projects []*planpb.Expr, shape icebergMergeProjectionShape, targetCols []*planpb.ColDef) error {
	for _, proj := range shape.matched {
		if proj.clause == nil || proj.clause.Action != tree.MergeActionUpdate || proj.valueStart < 0 {
			continue
		}
		for colIdx, col := range targetCols {
			pos := proj.valueStart + colIdx
			if pos < 0 || pos >= len(projects) {
				return moerr.NewInvalidInputf(ctx.GetContext(), "Iceberg MERGE UPDATE value index is invalid: pos=%d projects=%d", pos, len(projects))
			}
			value, err := icebergMergeAssignmentValueExpr(ctx, projects[pos], col)
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
			value, err := icebergMergeAssignmentValueExpr(ctx, projects[pos], col)
			if err != nil {
				return err
			}
			projects[pos] = value
		}
	}
	return nil
}

func icebergMergeReplacementExpr(ctx CompilerContext, projects []*planpb.Expr, shape icebergMergeProjectionShape, isUnmatchedExpr *planpb.Expr, targetCol *planpb.ColDef, colIdx int) (*planpb.Expr, error) {
	matchedValue := DeepCopyExpr(projects[shape.matchedDefaultStart+colIdx])
	for idx := len(shape.matched) - 1; idx >= 0; idx-- {
		proj := shape.matched[idx]
		if proj.clause == nil || proj.clause.Action != tree.MergeActionUpdate || proj.valueStart < 0 {
			continue
		}
		active, err := icebergMergeMatchedActiveExpr(ctx, isUnmatchedExpr, projects[proj.conditionPos])
		if err != nil {
			return nil, err
		}
		updateValue, err := icebergMergeAssignmentValueExpr(ctx, projects[proj.valueStart+colIdx], targetCol)
		if err != nil {
			return nil, err
		}
		matchedValue, err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{
			active,
			updateValue,
			matchedValue,
		})
		if err != nil {
			return nil, err
		}
	}
	if len(shape.notMatched) == 0 {
		return matchedValue, nil
	}
	insertValue := DeepCopyExpr(projects[shape.matchedDefaultStart+colIdx])
	for idx := len(shape.notMatched) - 1; idx >= 0; idx-- {
		proj := shape.notMatched[idx]
		active, err := icebergMergeNotMatchedActiveExpr(ctx, isUnmatchedExpr, projects[proj.conditionPos])
		if err != nil {
			return nil, err
		}
		clauseInsertValue, err := icebergMergeAssignmentValueExpr(ctx, projects[proj.valueStart+colIdx], targetCol)
		if err != nil {
			return nil, err
		}
		insertValue, err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{
			active,
			clauseInsertValue,
			insertValue,
		})
		if err != nil {
			return nil, err
		}
	}
	return BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{
		DeepCopyExpr(isUnmatchedExpr),
		insertValue,
		matchedValue,
	})
}

func icebergMergeAssignmentValueExpr(ctx CompilerContext, expr *planpb.Expr, targetCol *planpb.ColDef) (*planpb.Expr, error) {
	if targetCol == nil {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg MERGE assignment target column is missing")
	}
	value := DeepCopyExpr(expr)
	var err error
	if isDefaultValExpr(value) {
		value, err = getDefaultExpr(ctx.GetContext(), targetCol)
		if err != nil {
			return nil, err
		}
	}
	return forceAssignmentCastExprWithProcess(ctx.GetContext(), value, targetCol.Typ, false, ctx.GetProcess())
}

func icebergMergeActionExpr(ctx CompilerContext, projects []*planpb.Expr, shape icebergMergeProjectionShape, isUnmatchedExpr *planpb.Expr) (*planpb.Expr, error) {
	actionExpr := makePlan2StringConstExprWithType(icebergapi.DMLMergeActionNoop)
	for idx := len(shape.notMatched) - 1; idx >= 0; idx-- {
		proj := shape.notMatched[idx]
		active, err := icebergMergeNotMatchedActiveExpr(ctx, isUnmatchedExpr, projects[proj.conditionPos])
		if err != nil {
			return nil, err
		}
		actionExpr, err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{
			active,
			makePlan2StringConstExprWithType(icebergapi.DMLMergeActionInsert),
			actionExpr,
		})
		if err != nil {
			return nil, err
		}
	}
	for idx := len(shape.matched) - 1; idx >= 0; idx-- {
		proj := shape.matched[idx]
		active, err := icebergMergeMatchedActiveExpr(ctx, isUnmatchedExpr, projects[proj.conditionPos])
		if err != nil {
			return nil, err
		}
		actionExpr, err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*planpb.Expr{
			active,
			makePlan2StringConstExprWithType(icebergMergeActionForClause(proj.clause)),
			actionExpr,
		})
		if err != nil {
			return nil, err
		}
	}
	return actionExpr, nil
}

func icebergMergeMatchedActiveExpr(ctx CompilerContext, isUnmatchedExpr, conditionExpr *planpb.Expr) (*planpb.Expr, error) {
	isMatchedExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "not", []*planpb.Expr{DeepCopyExpr(isUnmatchedExpr)})
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(ctx.GetContext(), "and", []*planpb.Expr{
		isMatchedExpr,
		DeepCopyExpr(conditionExpr),
	})
}

func icebergMergeNotMatchedActiveExpr(ctx CompilerContext, isUnmatchedExpr, conditionExpr *planpb.Expr) (*planpb.Expr, error) {
	return BindFuncExprImplByPlanExpr(ctx.GetContext(), "and", []*planpb.Expr{
		DeepCopyExpr(isUnmatchedExpr),
		DeepCopyExpr(conditionExpr),
	})
}

func icebergMergeActionForClause(clause *tree.MergeClause) string {
	if clause == nil {
		return icebergapi.DMLMergeActionNoop
	}
	if clause.Action == tree.MergeActionDelete {
		return icebergapi.DMLMergeActionDelete
	}
	return icebergapi.DMLMergeActionUpdate
}

func (shape icebergMergeProjectionShape) String() string {
	return fmt.Sprintf("table_columns=%d pre_metadata=%d matched_default_start=%d matched_clauses=%d not_matched_clauses=%d", shape.tableColumnCount, shape.preMetadataCount, shape.matchedDefaultStart, len(shape.matched), len(shape.notMatched))
}
