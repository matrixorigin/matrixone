// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func returningNotSupported(builder *QueryBuilder, feature string) error {
	return moerr.NewNotSupportedf(builder.GetContext(), "DML RETURNING does not support %s", feature)
}

func returningFallbackFeature(err error, fallback string) string {
	if err == nil {
		return ""
	}
	if err.Error() == icebergRowLevelDMLUnsupportedMsg {
		return "Iceberg table"
	}
	if err.Error() == externalTableUnsupportedDMLMsg ||
		strings.Contains(err.Error(), "cannot insert/update/delete from external table") {
		return "external table"
	}
	if moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML) {
		return fallback
	}
	return ""
}

func returningUpdatePlannerFeature(err error) string {
	route, reason, _ := classifyUpdatePlannerError(err)
	if route == updatePlannerRejected && reason == updateRouteReasonIrregularIndex {
		return "primary-key UPDATE on synchronous full-text/vector index"
	}
	return ""
}

func returningBaseTableExpr(expr tree.TableExpr) bool {
	switch table := expr.(type) {
	case *tree.TableName:
		return true
	case *tree.AliasedTableExpr:
		_, ok := table.Expr.(*tree.TableName)
		return ok
	case *tree.ParenTableExpr:
		return returningBaseTableExpr(table.Expr)
	default:
		return false
	}
}

// validateReturningSyntax rejects every statement shape that could otherwise
// fall back to a legacy/specialized DML planner. The modern binders perform the
// target-kind checks after name resolution.
func validateReturningSyntax(builder *QueryBuilder, stmt tree.Statement) error {
	switch s := stmt.(type) {
	case *tree.Insert:
		if !s.HasReturning() {
			return nil
		}
		if len(s.OnDuplicateUpdate) == 1 && s.OnDuplicateUpdate[0] == nil {
			return returningNotSupported(builder, "INSERT IGNORE")
		}
		if s.Overwrite {
			return returningNotSupported(builder, "INSERT OVERWRITE")
		}
		if len(s.OnDuplicateUpdate) > 0 {
			return returningNotSupported(builder, "INSERT ON DUPLICATE KEY UPDATE")
		}
		if s.With != nil {
			return returningNotSupported(builder, "WITH DML")
		}
		if len(s.PartitionNames) > 0 || len(s.PartitionValues) > 0 {
			return returningNotSupported(builder, "explicit PARTITION DML")
		}
	case *tree.Update:
		if !s.HasReturning() {
			return nil
		}
		if s.Priority != "" {
			return returningNotSupported(builder, strings.ToUpper(s.Priority)+" UPDATE")
		}
		if s.Ignore {
			return returningNotSupported(builder, "UPDATE IGNORE")
		}
		if s.MultiTable || len(s.Tables) != 1 {
			return returningNotSupported(builder, "multi-table UPDATE")
		}
		if !returningBaseTableExpr(s.Tables[0]) {
			return returningNotSupported(builder, "joined UPDATE")
		}
		if s.From != nil && len(s.From.Tables) > 0 {
			return returningNotSupported(builder, "UPDATE FROM")
		}
		if s.With != nil {
			return returningNotSupported(builder, "WITH DML")
		}
	case *tree.Delete:
		if !s.HasReturning() {
			return nil
		}
		if s.Priority != "" {
			return returningNotSupported(builder, strings.ToUpper(s.Priority)+" DELETE")
		}
		if s.Quick {
			return returningNotSupported(builder, "DELETE QUICK")
		}
		if s.Ignore {
			return returningNotSupported(builder, "DELETE IGNORE")
		}
		if len(s.Tables) != 1 {
			return returningNotSupported(builder, "multi-table DELETE")
		}
		if len(s.TableRefs) > 0 {
			return returningNotSupported(builder, "DELETE USING")
		}
		if s.With != nil {
			return returningNotSupported(builder, "WITH DML")
		}
		if len(s.PartitionNames) > 0 {
			return returningNotSupported(builder, "explicit PARTITION DML")
		}
	}
	return nil
}

func validateReturningTarget(builder *QueryBuilder, tableDef *planpb.TableDef, objRef *planpb.ObjectRef) error {
	if tableDef == nil {
		return returningNotSupported(builder, "non-table target")
	}
	if tableDef.IsTemporary || tableDef.TableType == catalog.SystemTemporaryTable {
		return returningNotSupported(builder, "temporary table")
	}
	if tableDef.TableType == catalog.SystemExternalRel {
		return returningNotSupported(builder, "external table")
	}
	if tableDef.TableType != catalog.SystemOrdinaryRel {
		return returningNotSupported(builder, "internal table")
	}
	if objRef != nil && strings.EqualFold(objRef.SchemaName, catalog.MO_CATALOG) {
		return returningNotSupported(builder, "system table")
	}
	return nil
}

func makeReturningColPos(tableDef *planpb.TableDef, positions map[string]int32) map[string]int32 {
	ret := make(map[string]int32, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if pos, ok := positions[strings.ToLower(col.Name)]; ok {
			ret[strings.ToLower(col.Name)] = pos
		}
	}
	return ret
}

func (builder *QueryBuilder) recordReturningSource(
	step int32,
	tableDef *planpb.TableDef,
	objRef *planpb.ObjectRef,
	tableName string,
	alias string,
	colPos map[string]int32,
) {
	builder.returningSourceStep = step
	builder.returningTableDef = tableDef
	builder.returningObjRef = objRef
	builder.returningTableName = strings.ToLower(tableName)
	builder.returningAlias = strings.ToLower(alias)
	builder.returningColPos = makeReturningColPos(tableDef, colPos)
}

func (builder *QueryBuilder) recordReturningIrregularMaintenance(
	indexes []*planpb.IndexDef,
	tableDef *planpb.TableDef,
	objRef *planpb.ObjectRef,
	pkPos int32,
	skipInsert bool,
) error {
	if len(indexes) == 0 {
		return nil
	}
	if tableDef.Pkey == nil {
		return moerr.NewInternalError(builder.GetContext(), "DML RETURNING irregular maintenance requires a primary key")
	}
	pkColIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok || pkColIdx < 0 || int(pkColIdx) >= len(tableDef.Cols) {
		return moerr.NewInternalError(builder.GetContext(), "DML RETURNING cannot locate irregular maintenance primary key")
	}
	maintTableDef := *tableDef
	maintTableDef.Indexes = indexes
	builder.irregularMaintSourceStep = builder.returningSourceStep
	builder.irregularMaintDeleteStep = builder.returningSourceStep
	builder.irregularMaintDeletePkPos = pkPos
	builder.irregularMaintDeletePkTyp = tableDef.Cols[pkColIdx].Typ
	builder.irregularMaintIndexes = indexes
	builder.irregularMaintTableDef = &maintTableDef
	builder.irregularMaintObjRef = objRef
	builder.irregularMaintSkipInsert = skipInsert
	return nil
}

// materializeReturningSource gives the mutation and RETURNING projection
// independent readers of exactly the same final row-image generation.
func (builder *QueryBuilder) materializeReturningSource(
	bindCtx *BindContext,
	inputNodeID int32,
	inputTag int32,
	tableDef *planpb.TableDef,
	objRef *planpb.ObjectRef,
	tableName string,
	alias string,
	colPos map[string]int32,
) int32 {
	sinkID := appendSinkNodeWithTag(builder, bindCtx, inputNodeID, inputTag)
	builder.preserveReturningSinkProjection(sinkID)
	step := builder.appendStep(sinkID)
	builder.recordReturningSource(step, tableDef, objRef, tableName, alias, colPos)
	return builder.appendTaggedSinkScan(bindCtx, step, inputTag)
}

func (builder *QueryBuilder) preserveReturningSinkProjection(nodeID int32) {
	if builder.preserveSinkProjection == nil {
		builder.preserveSinkProjection = make(map[int32]struct{})
	}
	builder.preserveSinkProjection[nodeID] = struct{}{}
}

func returningExprForbidden(expr *planpb.Expr) string {
	if expr == nil {
		return "invalid RETURNING expression"
	}
	switch e := expr.Expr.(type) {
	case *planpb.Expr_Sub:
		return "subquery in RETURNING expression"
	case *planpb.Expr_W:
		return "window function in RETURNING expression"
	case *planpb.Expr_V:
		return "variable in RETURNING expression"
	case *planpb.Expr_F:
		if e.F != nil && e.F.Func != nil && function.GetFunctionIsVolatileOrRealTimeRelatedByName(e.F.Func.ObjName) {
			return "volatile function in RETURNING expression"
		}
		for _, arg := range e.F.Args {
			if feature := returningExprForbidden(arg); feature != "" {
				return feature
			}
		}
	case *planpb.Expr_List:
		for _, arg := range e.List.List {
			if feature := returningExprForbidden(arg); feature != "" {
				return feature
			}
		}
	}
	return ""
}

type returningQualifierVisitor struct {
	tableName string
	alias     string
	feature   string
}

func (v *returningQualifierVisitor) Enter(expr tree.Expr) (tree.Expr, bool) {
	name, ok := expr.(*tree.UnresolvedName)
	if !ok || name.NumParts < 2 || v.feature != "" {
		return expr, v.feature != ""
	}
	qualifier := strings.ToLower(name.TblName())
	if qualifier == "old" || qualifier == "new" {
		v.feature = "old/new pseudo namespace"
	} else if name.NumParts > 2 || qualifier != v.tableName && qualifier != v.alias {
		v.feature = "non-target source in RETURNING expression"
	}
	return expr, v.feature != ""
}

func (v *returningQualifierVisitor) Exit(expr tree.Expr) (tree.Expr, bool) {
	return expr, v.feature == ""
}

// Some parser expression nodes intentionally do not implement Accept (notably
// Subquery). Qualifier validation is only an early, deterministic diagnostic;
// the projection binder remains authoritative for those nodes.
func returningQualifierFeature(expr tree.Expr, tableName string, alias string) (feature string) {
	defer func() {
		if recover() != nil {
			feature = ""
		}
	}()
	visitor := &returningQualifierVisitor{tableName: tableName, alias: alias}
	switch expr := expr.(type) {
	case tree.UnqualifiedStar, *tree.UnqualifiedStar, nil:
		return ""
	default:
		_, _ = expr.Accept(visitor)
		return visitor.feature
	}
}

func (builder *QueryBuilder) appendReturningProjection(exprs tree.SelectExprs, bindCtx *BindContext) error {
	if len(exprs) == 0 {
		return nil
	}
	if builder.returningSourceStep < 0 || builder.returningTableDef == nil {
		return moerr.NewInternalError(builder.GetContext(), "DML RETURNING row image was not materialized")
	}
	for _, selectExpr := range exprs {
		if feature := returningQualifierFeature(
			selectExpr.Expr, builder.returningTableName, builder.returningAlias,
		); feature != "" {
			return returningNotSupported(builder, feature)
		}
	}

	sourceSinkID := builder.qry.Steps[builder.returningSourceStep]
	sourceSink := builder.qry.Nodes[sourceSinkID]
	if len(sourceSink.BindingTags) != 1 {
		return moerr.NewInternalError(builder.GetContext(), "DML RETURNING source sink has invalid binding")
	}

	returnCtx := NewBindContext(builder, bindCtx)
	scanTag := builder.genNewBindTag()
	visibleCols := make([]*planpb.ColDef, 0, len(builder.returningTableDef.Cols))
	scanProjects := make([]*planpb.Expr, 0, len(builder.returningTableDef.Cols))
	for _, col := range builder.returningTableDef.Cols {
		if col.Hidden || col.Name == catalog.Row_ID {
			continue
		}
		pos, ok := builder.returningColPos[strings.ToLower(col.Name)]
		if !ok || pos < 0 || int(pos) >= len(sourceSink.ProjectList) {
			return moerr.NewInternalErrorf(builder.GetContext(), "DML RETURNING cannot locate final image column %s", col.Name)
		}
		visibleCols = append(visibleCols, &planpb.ColDef{Name: col.Name, Typ: col.Typ})
		scanProjects = append(scanProjects, &planpb.Expr{
			Typ: col.Typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: sourceSink.BindingTags[0],
				ColPos: pos,
				Name:   col.Name,
			}},
		})
	}
	scanID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_SINK_SCAN,
		SourceStep:  []int32{builder.returningSourceStep},
		ProjectList: scanProjects,
		BindingTags: []int32{scanTag},
		TableDef: &planpb.TableDef{
			Name:         builder.returningTableDef.Name,
			OriginalName: builder.returningTableDef.OriginalName,
			DbName:       builder.returningTableDef.DbName,
			TblId:        builder.returningTableDef.TblId,
			Cols:         visibleCols,
		},
	}, returnCtx)

	bindName := builder.returningAlias
	if bindName == "" {
		bindName = builder.returningTableName
	}
	if err := builder.addBinding(scanID, tree.AliasClause{Alias: tree.Identifier(bindName)}, returnCtx); err != nil {
		return err
	}
	if len(returnCtx.bindings) != 1 {
		return moerr.NewInternalError(builder.GetContext(), "DML RETURNING failed to bind target image")
	}
	binding := returnCtx.bindings[0]
	returnCtx.bindingByTable[builder.returningTableName] = binding
	if builder.returningAlias != "" {
		returnCtx.bindingByTable[builder.returningAlias] = binding
	}
	for _, col := range binding.cols {
		returnCtx.bindingByCol[col] = binding
	}

	selectList, err := appendSelectList(builder, returnCtx, nil, exprs...)
	if err != nil {
		return err
	}
	beforeNodes := len(builder.qry.Nodes)
	projectionBinder := NewProjectionBinder(builder, returnCtx, NewHavingBinder(builder, returnCtx))
	if _, _, err = builder.bindProjection(returnCtx, projectionBinder, selectList, false); err != nil {
		return err
	}
	if len(builder.qry.Nodes) != beforeNodes {
		return returningNotSupported(builder, "subquery in RETURNING expression")
	}
	if len(returnCtx.aggregates) > 0 {
		return returningNotSupported(builder, "aggregate in RETURNING expression")
	}
	if len(returnCtx.windows) > 0 {
		return returningNotSupported(builder, "window function in RETURNING expression")
	}
	for _, expr := range returnCtx.projects {
		if feature := returningExprForbidden(expr); feature != "" {
			return returningNotSupported(builder, feature)
		}
	}

	projectTag := builder.genNewBindTag()
	projectID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{scanID},
		ProjectList: returnCtx.projects,
		BindingTags: []int32{projectTag},
	}, returnCtx)
	builder.qry.HasReturning = true
	builder.qry.ReturningStep = builder.appendStep(projectID)
	builder.qry.Headings = append(builder.qry.Headings, returnCtx.headings...)
	return nil
}
