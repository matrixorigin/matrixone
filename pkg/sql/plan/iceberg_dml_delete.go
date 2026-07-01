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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type icebergDeleteTarget struct {
	objRef    *plan.ObjectRef
	tableDef  *plan.TableDef
	tableName string
	alias     string
}

func buildIcebergUpdatePlan(stmt *tree.Update, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	target, err := resolveIcebergUpdateTarget(ctx, stmt)
	if err != nil {
		return nil, err
	}

	builder := NewQueryBuilder(plan.Query_UPDATE, ctx, isPrepareStmt, false)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	selectExprs, err := icebergUpdateSelectExprs(ctx.GetContext(), target, stmt, bindCtx)
	if err != nil {
		return nil, err
	}
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Distinct: false,
			Exprs:    selectExprs,
			From:     &tree.From{Tables: stmt.Tables},
			Where:    stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}
	lastNodeID, err := builder.bindSelect(selectStmt, bindCtx, false)
	if err != nil {
		return nil, err
	}

	insertNode := &plan.Node{
		NodeType:     plan.Node_INSERT,
		Children:     []int32{lastNodeID},
		ObjRef:       target.objRef,
		TableDef:     target.tableDef,
		ExtraOptions: icebergapi.DMLUpdatePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			Ref:             target.objRef,
			AddAffectedRows: true,
			TableDef:        target.tableDef,
		},
		ProjectList: getProjectionByLastNode(builder, lastNodeID),
	}
	lastNodeID = builder.appendNode(insertNode, bindCtx)
	builder.appendStep(lastNodeID)

	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	dmlTableDef, err := appendIcebergUpdateMetadataProjection(ctx.GetContext(), query, query.Steps[len(query.Steps)-1])
	if err != nil {
		return nil, err
	}
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	root.TableDef = dmlTableDef
	if root.InsertCtx != nil {
		root.InsertCtx.TableDef = dmlTableDef
	}
	if err := rewriteIcebergUpdateProjection(ctx.GetContext(), query, query.Steps[len(query.Steps)-1]); err != nil {
		return nil, err
	}
	query.StmtType = plan.Query_UPDATE
	return &Plan{Plan: &plan.Plan_Query{Query: query}}, nil
}

func buildIcebergDeletePlan(stmt *tree.Delete, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	target, err := resolveIcebergDeleteTarget(ctx, stmt)
	if err != nil {
		return nil, err
	}

	builder := NewQueryBuilder(plan.Query_DELETE, ctx, isPrepareStmt, false)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	selectExprs := icebergDeleteSelectExprs(target, bindCtx)
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Distinct: false,
			Exprs:    selectExprs,
			From:     &tree.From{Tables: stmt.Tables},
			Where:    stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}
	lastNodeID, err := builder.bindSelect(selectStmt, bindCtx, false)
	if err != nil {
		return nil, err
	}

	insertNode := &plan.Node{
		NodeType:     plan.Node_INSERT,
		Children:     []int32{lastNodeID},
		ObjRef:       target.objRef,
		TableDef:     target.tableDef,
		ExtraOptions: icebergapi.DMLDeletePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			Ref:             target.objRef,
			AddAffectedRows: true,
			TableDef:        target.tableDef,
		},
		ProjectList: getProjectionByLastNode(builder, lastNodeID),
	}
	lastNodeID = builder.appendNode(insertNode, bindCtx)
	builder.appendStep(lastNodeID)

	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	dmlTableDef, err := appendIcebergDeleteMetadataProjection(ctx.GetContext(), query, query.Steps[len(query.Steps)-1])
	if err != nil {
		return nil, err
	}
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	root.TableDef = dmlTableDef
	if root.InsertCtx != nil {
		root.InsertCtx.TableDef = dmlTableDef
	}
	query.StmtType = plan.Query_DELETE
	return &Plan{Plan: &plan.Plan_Query{Query: query}}, nil
}

func resolveIcebergUpdateTarget(ctx CompilerContext, stmt *tree.Update) (icebergDeleteTarget, error) {
	if stmt == nil {
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg UPDATE requires a statement")
	}
	if stmt.From != nil || len(stmt.Tables) != 1 {
		return icebergDeleteTarget{}, moerr.NewNotSupported(ctx.GetContext(), "Iceberg UPDATE currently supports a single target table without FROM")
	}
	tbl := stmt.Tables[0]
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
		return icebergDeleteTarget{}, moerr.NewNotSupported(ctx.GetContext(), "Iceberg UPDATE requires a base table target")
	}
	dbName := string(tableName.SchemaName)
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}
	target.tableName = string(tableName.ObjectName)
	if strings.TrimSpace(target.tableName) == "" {
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg UPDATE requires a table name")
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
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg UPDATE requires an Iceberg table mapping")
	}
	target.objRef = objRef
	target.tableDef = tableDef
	return target, nil
}

func icebergUpdateSelectExprs(ctx context.Context, target icebergDeleteTarget, stmt *tree.Update, bindCtx *BindContext) ([]tree.SelectExpr, error) {
	updates, err := icebergUpdateExprMap(ctx, target, stmt)
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
}

func icebergUpdateExprMap(ctx context.Context, target icebergDeleteTarget, stmt *tree.Update) (map[string]tree.Expr, error) {
	if stmt == nil || len(stmt.Exprs) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg UPDATE requires assignments")
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
	updates := make(map[string]tree.Expr, len(stmt.Exprs))
	for _, assignment := range stmt.Exprs {
		if assignment == nil || assignment.Tuple || len(assignment.Names) != 1 {
			return nil, moerr.NewNotSupported(ctx, "Iceberg UPDATE currently supports one column per assignment")
		}
		name := assignment.Names[0]
		if name == nil {
			return nil, moerr.NewInvalidInput(ctx, "Iceberg UPDATE assignment is missing a column name")
		}
		if tbl := strings.TrimSpace(name.TblName()); tbl != "" && !strings.EqualFold(tbl, qualifier) && !strings.EqualFold(tbl, target.tableName) {
			return nil, moerr.NewNotSupported(ctx, "Iceberg UPDATE assignment references a different target table")
		}
		colKey := strings.ToLower(strings.TrimSpace(name.ColName()))
		if colKey == "" {
			return nil, moerr.NewInvalidInput(ctx, "Iceberg UPDATE assignment is missing a column name")
		}
		colName, ok := validCols[colKey]
		if !ok {
			return nil, moerr.NewBadFieldError(ctx, name.ColNameOrigin(), target.tableName)
		}
		if _, exists := updates[colKey]; exists {
			return nil, moerr.NewInvalidInputf(ctx, "Iceberg UPDATE assigns column %s more than once", colName)
		}
		updates[colKey] = assignment.Expr
	}
	return updates, nil
}

func resolveIcebergDeleteTarget(ctx CompilerContext, stmt *tree.Delete) (icebergDeleteTarget, error) {
	if stmt == nil {
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg DELETE requires a statement")
	}
	if stmt.TableRefs != nil || len(stmt.Tables) != 1 {
		return icebergDeleteTarget{}, moerr.NewNotSupported(ctx.GetContext(), "Iceberg DELETE currently supports a single target table")
	}
	if len(stmt.PartitionNames) > 0 {
		return icebergDeleteTarget{}, moerr.NewNotSupported(ctx.GetContext(), "Iceberg DELETE does not support partition-name syntax")
	}

	tbl := stmt.Tables[0]
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
		return icebergDeleteTarget{}, moerr.NewNotSupported(ctx.GetContext(), "Iceberg DELETE requires a base table target")
	}
	dbName := string(tableName.SchemaName)
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}
	target.tableName = string(tableName.ObjectName)
	if strings.TrimSpace(target.tableName) == "" {
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg DELETE requires a table name")
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
		return icebergDeleteTarget{}, moerr.NewInvalidInput(ctx.GetContext(), "Iceberg DELETE requires an Iceberg table mapping")
	}
	target.objRef = objRef
	target.tableDef = tableDef
	return target, nil
}

func icebergDeleteSelectExprs(target icebergDeleteTarget, bindCtx *BindContext) []tree.SelectExpr {
	qualifier := target.alias
	if qualifier == "" {
		qualifier = target.tableName
	}
	out := make([]tree.SelectExpr, 0, len(target.tableDef.Cols))
	for _, col := range target.tableDef.Cols {
		if col == nil || col.Hidden || col.Name == catalog.Row_ID || col.Name == catalog.ExternalFilePath {
			continue
		}
		out = append(out, tree.SelectExpr{
			Expr: tree.NewUnresolvedName(
				tree.NewCStr(qualifier, bindCtx.lower),
				tree.NewCStr(col.Name, 1),
			),
		})
	}
	return out
}

func appendIcebergDeleteMetadataProjection(ctx context.Context, query *plan.Query, rootID int32) (*plan.TableDef, error) {
	return appendIcebergDMLMetadataProjection(ctx, query, rootID, false, false)
}

func appendIcebergUpdateMetadataProjection(ctx context.Context, query *plan.Query, rootID int32) (*plan.TableDef, error) {
	return appendIcebergDMLMetadataProjection(ctx, query, rootID, false, true)
}

func appendIcebergDMLMetadataProjection(ctx context.Context, query *plan.Query, rootID int32, allowJoinRight bool, preserveTableColumns bool) (*plan.TableDef, error) {
	if query == nil {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg DELETE plan requires a query")
	}
	if rootID < 0 || int(rootID) >= len(query.Nodes) {
		return nil, moerr.NewInvalidInputf(ctx, "Iceberg DELETE plan references invalid root node %d", rootID)
	}
	root := query.Nodes[rootID]
	scanID, err := findSingleIcebergDMLScan(ctx, query, rootID)
	if err != nil {
		return nil, err
	}
	scan := query.Nodes[scanID]
	if scan.TableDef == nil || scan.ExternScan == nil {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg DELETE scan is malformed")
	}
	if preserveTableColumns {
		ensureIcebergDMLScanProjectsTableColumns(scan, icebergDMLRootTargetTableDef(root))
	}
	projection, err := BuildIcebergDMLMetadataProjection(ctx, scan.TableDef, int32(len(scan.TableDef.Cols)), true)
	if err != nil {
		return nil, err
	}
	for _, col := range projection.Cols {
		scan.TableDef.Cols = append(scan.TableDef.Cols, col)
		if scan.ExternScan.TbColToDataCol == nil {
			scan.ExternScan.TbColToDataCol = make(map[string]int32)
		}
		scan.ExternScan.TbColToDataCol[col.Name] = int32(len(scan.ExternScan.TbColToDataCol))
	}

	if _, found, err := appendIcebergDMLColumnsOnPath(ctx, query, rootID, scanID, projection.Cols, allowJoinRight); err != nil {
		return nil, err
	} else if !found {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg DELETE scan is not reachable from plan root")
	}
	return scan.TableDef, nil
}

func icebergDMLRootTargetTableDef(root *plan.Node) *plan.TableDef {
	if root == nil {
		return nil
	}
	if root.GetInsertCtx() != nil && root.GetInsertCtx().GetTableDef() != nil {
		return root.GetInsertCtx().GetTableDef()
	}
	return root.GetTableDef()
}

func ensureIcebergDMLScanProjectsTableColumns(scan *plan.Node, targetTableDef *plan.TableDef) {
	if scan == nil || scan.TableDef == nil {
		return
	}
	if targetTableDef == nil {
		targetTableDef = scan.TableDef
	}
	realCols := make([]*plan.ColDef, 0, len(targetTableDef.GetCols()))
	tbColToDataCol := make(map[string]int32, len(targetTableDef.GetCols()))
	scan.ProjectList = scan.ProjectList[:0]
	for _, col := range targetTableDef.GetCols() {
		if col == nil || col.Hidden || col.Name == catalog.Row_ID || col.Name == catalog.ExternalFilePath ||
			strings.EqualFold(col.Name, icebergapi.DMLDataFilePathColumnName) ||
			strings.EqualFold(col.Name, icebergapi.DMLRowOrdinalColumnName) ||
			strings.EqualFold(col.Name, icebergapi.DMLMergeActionColumnName) {
			continue
		}
		copied := DeepCopyColDef(col)
		pos := int32(len(realCols))
		realCols = append(realCols, copied)
		tbColToDataCol[copied.Name] = pos
		scan.ProjectList = append(scan.ProjectList, icebergDMLPassthroughExpr(copied, pos))
	}
	if len(realCols) == 0 {
		return
	}
	scan.TableDef.Cols = realCols
	scan.TableDef.Name2ColIndex = make(map[string]int32, len(realCols))
	for idx, col := range realCols {
		scan.TableDef.Name2ColIndex[col.Name] = int32(idx)
	}
	if scan.ExternScan.TbColToDataCol == nil {
		scan.ExternScan.TbColToDataCol = make(map[string]int32, len(tbColToDataCol))
	}
	for name := range scan.ExternScan.TbColToDataCol {
		if strings.EqualFold(name, icebergapi.DMLDataFilePathColumnName) ||
			strings.EqualFold(name, icebergapi.DMLRowOrdinalColumnName) ||
			strings.EqualFold(name, icebergapi.DMLMergeActionColumnName) {
			continue
		}
		delete(scan.ExternScan.TbColToDataCol, name)
	}
	for name, pos := range tbColToDataCol {
		scan.ExternScan.TbColToDataCol[name] = pos
	}
}

func findSingleIcebergDMLScan(ctx context.Context, query *plan.Query, rootID int32) (int32, error) {
	var found []int32
	var walk func(int32) error
	walk = func(nodeID int32) error {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return moerr.NewInvalidInputf(ctx, "Iceberg DELETE plan references invalid node %d", nodeID)
		}
		node := query.Nodes[nodeID]
		if node.GetExternScan() != nil && node.GetExternScan().GetType() == int32(plan.ExternType_ICEBERG_TB) {
			found = append(found, nodeID)
		}
		for _, child := range node.Children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(rootID); err != nil {
		return 0, err
	}
	if len(found) > 1 {
		if targetRef := icebergDMLTargetRefFromRoot(query, rootID); targetRef != nil {
			var targetScans []int32
			for _, scanID := range found {
				if icebergDMLSameObjectRef(targetRef, query.Nodes[scanID].GetObjRef()) {
					targetScans = append(targetScans, scanID)
				}
			}
			if len(targetScans) == 1 {
				return targetScans[0], nil
			}
		}
	}
	if len(found) != 1 {
		return 0, moerr.NewNotSupportedf(ctx, "Iceberg DELETE requires exactly one Iceberg scan, found %d", len(found))
	}
	return found[0], nil
}

func icebergDMLTargetRefFromRoot(query *plan.Query, rootID int32) *plan.ObjectRef {
	if query == nil || rootID < 0 || int(rootID) >= len(query.Nodes) {
		return nil
	}
	root := query.Nodes[rootID]
	if root.GetInsertCtx() != nil && root.GetInsertCtx().GetRef() != nil {
		return root.GetInsertCtx().GetRef()
	}
	return root.GetObjRef()
}

func icebergDMLSameObjectRef(a, b *plan.ObjectRef) bool {
	if a == nil || b == nil {
		return false
	}
	if a.GetObj() != 0 && b.GetObj() != 0 {
		return a.GetObj() == b.GetObj()
	}
	return strings.EqualFold(a.GetSchemaName(), b.GetSchemaName()) &&
		strings.EqualFold(a.GetObjName(), b.GetObjName())
}

func appendIcebergDMLColumnsOnPath(ctx context.Context, query *plan.Query, nodeID, scanID int32, cols []*plan.ColDef, allowJoinRight bool) ([]int32, bool, error) {
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return nil, false, moerr.NewInvalidInputf(ctx, "Iceberg DELETE plan references invalid node %d", nodeID)
	}
	node := query.Nodes[nodeID]
	if nodeID == scanID {
		positions := make([]int32, 0, len(cols))
		for _, col := range cols {
			pos := int32(len(node.ProjectList))
			node.ProjectList = append(node.ProjectList, icebergDMLPassthroughExpr(col, pos))
			positions = append(positions, pos)
		}
		return positions, true, nil
	}
	foundChild := false
	var childPositions []int32
	childRelPos := int32(0)
	for childIdx, childID := range node.Children {
		positions, found, err := appendIcebergDMLColumnsOnPath(ctx, query, childID, scanID, cols, allowJoinRight)
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}
		if foundChild || (childIdx != 0 && !(allowJoinRight && node.NodeType == plan.Node_JOIN)) {
			return nil, false, moerr.NewNotSupported(ctx, "Iceberg DELETE DML metadata projection requires a single-input plan")
		}
		foundChild = true
		childPositions = positions
		if node.NodeType == plan.Node_JOIN {
			childRelPos = int32(childIdx)
		}
	}
	if !foundChild {
		return nil, false, nil
	}
	positions := make([]int32, 0, len(cols))
	for i, col := range cols {
		pos := int32(len(node.ProjectList))
		node.ProjectList = append(node.ProjectList, icebergDMLPassthroughExprWithRel(col, childRelPos, childPositions[i]))
		positions = append(positions, pos)
	}
	return positions, true, nil
}

func icebergDMLPassthroughExpr(col *plan.ColDef, childPos int32) *plan.Expr {
	return icebergDMLPassthroughExprWithRel(col, 0, childPos)
}

func icebergDMLPassthroughExprWithRel(col *plan.ColDef, relPos, childPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: col.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: childPos,
				Name:   col.Name,
			},
		},
	}
}

func rewriteIcebergUpdateProjection(ctx context.Context, query *plan.Query, rootID int32) error {
	if query == nil || rootID < 0 || int(rootID) >= len(query.Nodes) {
		return moerr.NewInvalidInput(ctx, "Iceberg UPDATE plan requires a valid query root")
	}
	root := query.Nodes[rootID]
	if root == nil {
		return moerr.NewInvalidInput(ctx, "Iceberg UPDATE plan root is missing")
	}
	metadataCount := icebergDMLMetadataColumnCount(root.TableDef)
	if metadataCount == 0 {
		return nil
	}
	if len(root.ProjectList) < metadataCount {
		return moerr.NewInvalidInputf(ctx, "Iceberg UPDATE projection is missing DML metadata columns: projects=%d metadata=%d", len(root.ProjectList), metadataCount)
	}
	replacementCount := len(root.ProjectList) - metadataCount
	if len(root.Children) == 1 {
		childID := root.Children[0]
		if childID >= 0 && int(childID) < len(query.Nodes) {
			if projectNode := query.Nodes[childID]; projectNode != nil && projectNode.NodeType == plan.Node_PROJECT {
				if len(projectNode.ProjectList) < metadataCount {
					return moerr.NewInvalidInputf(ctx, "Iceberg UPDATE child projection is missing DML metadata columns: projects=%d metadata=%d", len(projectNode.ProjectList), metadataCount)
				}
				finalProjects := make([]*plan.Expr, 0, len(root.ProjectList))
				for _, expr := range root.ProjectList[:replacementCount] {
					finalProjects = append(finalProjects, DeepCopyExpr(expr))
				}
				for _, expr := range projectNode.ProjectList[len(projectNode.ProjectList)-metadataCount:] {
					finalProjects = append(finalProjects, DeepCopyExpr(expr))
				}
				projectNode.ProjectList = finalProjects
				if projectNode.Stats == nil {
					projectNode.Stats = DefaultStats()
				}
				root.ProjectList = finalProjects
				return nil
			}
		}
	}
	projectNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    append([]int32(nil), root.Children...),
		ProjectList: clonePlanExprs(root.ProjectList),
		Stats:       DefaultStats(),
	}
	projectID := int32(len(query.Nodes))
	query.Nodes = append(query.Nodes, projectNode)
	root.Children = []int32{projectID}
	return nil
}

func icebergDMLMetadataColumnCount(tableDef *plan.TableDef) int {
	if tableDef == nil {
		return 0
	}
	count := 0
	for _, col := range tableDef.GetCols() {
		if col == nil {
			continue
		}
		if strings.EqualFold(col.Name, icebergapi.DMLDataFilePathColumnName) ||
			strings.EqualFold(col.Name, icebergapi.DMLRowOrdinalColumnName) {
			count++
		}
	}
	return count
}

func clonePlanExprs(exprs []*plan.Expr) []*plan.Expr {
	out := make([]*plan.Expr, 0, len(exprs))
	for _, expr := range exprs {
		out = append(out, DeepCopyExpr(expr))
	}
	return out
}
