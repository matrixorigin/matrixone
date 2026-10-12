// Copyright 2022 Matrix Origin
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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// insertRowAliasBinding is the planner-local mapping from the names exposed by
// INSERT ... VALUES/SET AS row_alias to target table column positions. It is
// deliberately separate from catalog bindings so the alias cannot leak into
// the INSERT input or another statement.
type insertRowAliasBinding struct {
	name string
	cols map[string]insertRowAliasColumn
}

type insertRowAliasColumn struct {
	targetIdx   int
	incomingPos int
}

func lookupInsertTableColumn(tableDef *plan.TableDef, name string, lowerCaseTableNames int64) (int32, bool) {
	if tableDef == nil {
		return 0, false
	}
	// Column identifiers use the parser's column-name normalization regardless
	// of lower_case_table_names. The latter controls table/row-alias matching;
	// using it for columns makes an alias declared as `X` invisible to the
	// parser's normalized lookup key `x` when the setting is 0.
	key := normalizeInsertColumnName(name)
	if tableDef.Name2ColIndex != nil {
		if idx, ok := tableDef.Name2ColIndex[name]; ok {
			if idx >= 0 && int(idx) < len(tableDef.Cols) && tableDef.Cols[idx] != nil {
				return idx, true
			}
		}
		for candidate, idx := range tableDef.Name2ColIndex {
			if normalizeInsertColumnName(candidate) == key &&
				idx >= 0 && int(idx) < len(tableDef.Cols) && tableDef.Cols[idx] != nil {
				return idx, true
			}
		}
	}
	for i, col := range tableDef.Cols {
		if col != nil && normalizeInsertColumnName(col.Name) == key {
			return int32(i), true
		}
	}
	return 0, false
}

func normalizeInsertColumnName(name string) string {
	return tree.NewCStr(name, 1).Compare()
}

// validateInsertRowAlias checks the row alias after the target table and the
// effective INSERT column order are known. The effective order is the explicit
// INSERT list or SET list, or the visible implicit source sequence returned by
// getInsertColsForRowAlias (which retains represented generated columns).
func validateInsertRowAlias(
	ctx context.Context,
	rowAlias *tree.AliasClause,
	insertColumns []string,
	tableDef *plan.TableDef,
	targetDBName, targetTableName string,
	lowerCaseTableNames int64,
) (*insertRowAliasBinding, error) {
	if rowAlias == nil {
		return nil, nil
	}
	aliasName := tree.NewCStr(string(rowAlias.Alias), lowerCaseTableNames).Compare()
	if aliasName == "" {
		return nil, moerr.NewInvalidInput(ctx, "INSERT row alias cannot be empty")
	}
	targetName := tree.NewCStr(targetTableName, lowerCaseTableNames).Compare()
	if aliasName == targetName {
		return nil, moerr.NewInvalidInputf(ctx,
			"row alias '%s' conflicts with target table '%s'", rowAlias.Alias, targetTableName)
	}
	if tableDef == nil {
		return nil, moerr.NewInvalidInput(ctx, "INSERT row alias has no target table")
	}
	if len(rowAlias.Cols) > 0 && len(rowAlias.Cols) != len(insertColumns) {
		return nil, moerr.NewInvalidInputf(ctx,
			"INSERT row alias column list has %d entries, but the INSERT has %d columns",
			len(rowAlias.Cols), len(insertColumns))
	}

	binding := &insertRowAliasBinding{
		name: aliasName,
		cols: make(map[string]insertRowAliasColumn, len(insertColumns)),
	}
	for i, targetColumn := range insertColumns {
		if targetColumn == "" {
			return nil, moerr.NewInvalidInput(ctx, "INSERT row alias contains an empty target column")
		}
		targetKey := normalizeInsertColumnName(targetColumn)
		key := targetKey
		if len(rowAlias.Cols) > 0 {
			key = normalizeInsertColumnName(string(rowAlias.Cols[i]))
		}
		if key == "" {
			return nil, moerr.NewInvalidInput(ctx, "INSERT row alias column cannot be empty")
		}
		if _, duplicate := binding.cols[key]; duplicate {
			return nil, moerr.NewErrDupFieldName(ctx, key)
		}
		idx, ok := lookupInsertTableColumn(tableDef, targetColumn, lowerCaseTableNames)
		if !ok {
			return nil, moerr.NewBadFieldErrorf(ctx,
				"invalid input: column '%s' does not exist", targetColumn)
		}
		binding.cols[key] = insertRowAliasColumn{
			targetIdx:   int(idx),
			incomingPos: int(idx),
		}
	}

	// A row alias is never database-qualified. Database qualification is
	// validated while each RHS reference is bound; keep this argument here so
	// syntax validation has one shared call site for modern and fallback plans.
	_ = targetDBName
	return binding, nil
}

func validateOndupUpdateTargets(
	ctx context.Context,
	updates tree.UpdateExprs,
	tableDef *plan.TableDef,
	targetDBName, targetTableName string,
	lowerCaseTableNames int64,
) error {
	if tableDef == nil {
		return moerr.NewInvalidInput(ctx, "ON DUPLICATE KEY UPDATE has no target table")
	}
	for _, update := range updates {
		if update == nil {
			continue
		}
		if len(update.Names) == 0 || update.Names[0] == nil {
			return moerr.NewInvalidInput(ctx, "ON DUPLICATE KEY UPDATE has no target column")
		}
		if err := validateInsertColumnQualifiers(
			ctx, update.Names, targetDBName, targetTableName, lowerCaseTableNames,
		); err != nil {
			return err
		}
		colIdx, ok := lookupInsertTableColumn(tableDef, update.Names[0].ColName(), lowerCaseTableNames)
		if !ok {
			return moerr.NewBadFieldErrorf(ctx,
				"invalid input: column '%s' does not exist", update.Names[0].ColNameOrigin())
		}
		if tableDef.Cols[colIdx].GeneratedCol != nil {
			if _, isDefault := update.Expr.(*tree.DefaultVal); !isDefault {
				return moerr.NewInvalidInputf(ctx,
					"the value specified for generated column '%s' in table '%s' is not allowed",
					tableDef.Cols[colIdx].Name, tableDef.Name)
			}
		}
	}
	return nil
}

const odkuRowAliasSubqueryCause = "row-alias subqueries in on duplicate key update cannot be evaluated before duplicate-key action"

// This is an AST-only gate. Invalid names/types in a rejected RHS subquery
// must not replace the deterministic error, and unreachable CASE branches and
// no-key UPDATE arms must be checked without binding or executing them.
func validateRowAliasUpdateSubqueries(ctx context.Context, updates tree.UpdateExprs) error {
	for _, update := range updates {
		if update != nil && astExprContainsSubquery(update.Expr) {
			return moerr.NewUnsupportedDML(ctx, odkuRowAliasSubqueryCause)
		}
	}
	return nil
}

func (binding *insertRowAliasBinding) remapIncomingPositions(ctx context.Context, tableDef *plan.TableDef, colName2Idx map[string]int32) error {
	if binding == nil {
		return nil
	}
	for name, column := range binding.cols {
		if column.targetIdx < 0 || column.targetIdx >= len(tableDef.Cols) {
			return moerr.NewInvalidInputf(ctx, "row alias column '%s' has no target position", name)
		}
		targetName := tableDef.Cols[column.targetIdx].Name
		position, ok := colName2Idx[tableDef.Name+"."+targetName]
		if !ok {
			return moerr.NewBadFieldErrorf(ctx,
				"invalid input: column '%s' does not exist", targetName)
		}
		column.incomingPos = int(position)
		binding.cols[name] = column
	}
	return nil
}

// use for on duplicate key update clause:  eg: insert into t1 values(1,1),(2,2) on duplicate key update a = a + abs(b), b = values(b)-2
func NewOndupUpdateBinder(
	sysCtx context.Context,
	builder *QueryBuilder,
	ctx *BindContext,
	scanTag, selectTag int32,
	tableDef *plan.TableDef,
	targetDBName, targetTableName string,
	lowerCaseTableNames int64,
	rowAliases ...*insertRowAliasBinding,
) *OndupUpdateBinder {
	b := &OndupUpdateBinder{
		scanTag:             scanTag,
		selectTag:           selectTag,
		tableDef:            tableDef,
		targetDBName:        targetDBName,
		targetTableName:     targetTableName,
		lowerCaseTableNames: lowerCaseTableNames,
	}
	if len(rowAliases) > 0 {
		b.rowAlias = rowAliases[0]
	}
	b.sysCtx = sysCtx
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *OndupUpdateBinder) SetTargetCorrelationTag(tag int32) {
	b.targetCorrelationTag = tag
}

func (b *OndupUpdateBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	if funcExpr, ok := astExpr.(*tree.FuncExpr); ok {
		funcRef, ok := funcExpr.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return nil, moerr.NewNYIf(b.GetContext(), "function expr '%v'", astExpr)
		}

		if funcRef.ColName() == "values" {

			if len(funcExpr.Exprs) != 1 {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", funcExpr.Exprs)
			}

			col, ok := funcExpr.Exprs[0].(*tree.UnresolvedName)
			if !ok {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", funcExpr.Exprs[0])
			}
			if err := validateInsertColumnQualifiers(
				b.GetContext(), []*tree.UnresolvedName{col}, b.targetDBName, b.targetTableName, b.lowerCaseTableNames,
			); err != nil {
				return nil, err
			}

			colName := col.ColName()
			idx, ok := lookupInsertTableColumn(b.tableDef, colName, b.lowerCaseTableNames)
			if !ok || (b.rowAlias != nil && b.tableDef.Cols[idx].Hidden) {
				return nil, moerr.NewBadFieldErrorf(b.GetContext(), "invalid input: column '%s' does not exist", col.ColNameOrigin())
			}

			return &plan.Expr{
				Typ: b.tableDef.Cols[idx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.selectTag,
						ColPos: int32(idx),
						Name:   colName,
					},
				},
			}, nil
		}
	}

	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *OndupUpdateBinder) BindAssignmentExpr(astExpr tree.Expr, target Type) (*plan.Expr, error) {
	if err := b.validateSubqueryAdmission(astExpr); err != nil {
		return nil, err
	}
	if !useNumericAssignmentContext(target, b.builder.isInsertIgnore) {
		return b.BindExpr(astExpr, 0, true)
	}
	if isPreparedAssignmentParam(b.builder, astExpr) {
		return b.BindExpr(astExpr, 0, true)
	}
	if subquery, ok := scalarSubqueryExpr(astExpr); ok && !subquery.Exists {
		previousSubqueryTarget := b.numericSubqueryTarget
		b.numericSubqueryTarget = &target
		defer func() { b.numericSubqueryTarget = previousSubqueryTarget }()
		return b.baseBindExpr(astExpr, 0, true)
	}
	return b.bindNumericExprWithContext(astExpr, 0, &target)
}

func scalarSubqueryExpr(astExpr tree.Expr) (*tree.Subquery, bool) {
	for {
		switch expr := astExpr.(type) {
		case *tree.ParenExpr:
			astExpr = expr.Expr
		case *tree.Subquery:
			return expr, true
		default:
			return nil, false
		}
	}
}

func (b *OndupUpdateBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	colName := astExpr.ColName()
	tableName := astExpr.TblName()
	dbName := astExpr.DbName()
	targetTableName := tree.NewCStr(b.targetTableName, b.lowerCaseTableNames).Compare()
	targetDBName := tree.NewCStr(b.targetDBName, b.lowerCaseTableNames).Compare()
	normalizedTableName := tree.NewCStr(tableName, b.lowerCaseTableNames).Compare()

	if b.rowAlias != nil && normalizedTableName == b.rowAlias.name {
		if dbName != "" {
			return nil, moerr.NewInvalidInputf(b.GetContext(),
				"row alias '%s' cannot be database-qualified", astExpr.TblNameOrigin())
		}
		column, ok := b.rowAlias.cols[normalizeInsertColumnName(colName)]
		if !ok {
			return nil, moerr.NewBadFieldErrorf(b.GetContext(),
				"invalid input: column '%s' does not exist", astExpr.ColNameOrigin())
		}
		return b.makeColRef(column.targetIdx, column.incomingPos, depth, colName, b.selectTag), nil
	}

	if tableName != "" {
		if tableName != targetTableName || (dbName != "" && dbName != targetDBName) {
			// At a correlated depth, let the ordinary parent chain resolve a
			// local/outer subquery table before reporting this ODKU scope as
			// missing. At depth zero no other table is legal in this scope.
			if depth > 0 {
				return b.baseBindColRef(astExpr, depth, isRoot)
			}
			return nil, moerr.NewInvalidInputf(b.GetContext(),
				"missing FROM-clause entry for table '%s'", astExpr.TblNameOrigin())
		}
		idx, ok := lookupInsertTableColumn(b.tableDef, colName, b.lowerCaseTableNames)
		if !ok {
			return nil, moerr.NewBadFieldErrorf(b.GetContext(),
				"invalid input: column '%s' does not exist", astExpr.ColNameOrigin())
		}
		relPos := b.scanTag
		if depth > 0 && b.targetCorrelationTag != 0 {
			relPos = b.targetCorrelationTag
		}
		return b.makeColRef(int(idx), int(idx), depth, colName, relPos), nil
	}

	if b.rowAlias != nil {
		aliasKey := normalizeInsertColumnName(colName)
		_, incoming := b.rowAlias.cols[aliasKey]
		_, target := lookupInsertTableColumn(b.tableDef, colName, b.lowerCaseTableNames)
		if incoming && target {
			return nil, moerr.NewInvalidInputf(b.GetContext(),
				"ambiguous column reference '%s'", astExpr.ColNameOrigin())
		}
		if incoming {
			column := b.rowAlias.cols[aliasKey]
			return b.makeColRef(column.targetIdx, column.incomingPos, depth, colName, b.selectTag), nil
		}
	}

	if idx, ok := lookupInsertTableColumn(b.tableDef, colName, b.lowerCaseTableNames); ok {
		relPos := b.scanTag
		if depth > 0 && b.targetCorrelationTag != 0 {
			relPos = b.targetCorrelationTag
		}
		return b.makeColRef(int(idx), int(idx), depth, colName, relPos), nil
	}
	if depth > 0 {
		return b.baseBindColRef(astExpr, depth, isRoot)
	}

	return nil, moerr.NewBadFieldErrorf(b.GetContext(),
		"invalid input: column '%s' does not exist", astExpr.ColNameOrigin())
}

func (b *OndupUpdateBinder) makeColRef(targetIdx, colPos int, depth int32, name string, relPos int32) *plan.Expr {
	if depth > 0 && b.builder != nil && b.builder.odkuAdmission != nil {
		b.builder.odkuAdmission.hasOuterReference = true
	}
	expr := &plan.Expr{Typ: b.tableDef.Cols[targetIdx].Typ}
	if depth == 0 {
		expr.Expr = &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: relPos,
			ColPos: int32(colPos),
			Name:   name,
		}}
	} else {
		expr.Expr = &plan.Expr_Corr{Corr: &plan.CorrColRef{
			RelPos: relPos,
			ColPos: int32(colPos),
			Depth:  depth,
		}}
	}
	return expr
}

func (b *OndupUpdateBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind agregate functions '%s'", funcName)
}

func (b *OndupUpdateBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind window functions '%s'", funcName)
}

// odkuAdmissionState belongs exclusively to a validation-only builder. A
// resolved outer reference survives a later sibling error independently of
// whether the complete subquery expression can be built.
type odkuAdmissionState struct {
	hasOuterReference bool
	// Recovery only needs to visit operands that ordinary binding did not
	// reach. Keep visits scoped to their canonical query block and depth;
	// the same AST in another scope must still resolve its own provenance.
	visited map[odkuAdmissionExprKey]struct{}
}

type odkuAdmissionExprKey struct {
	expr  tree.Expr
	ctx   *BindContext
	depth int32
}

// validateSubqueryAdmission classifies the complete legacy assignment before
// ordinary binding can stop on an unrelated earlier operand or type error.
func (b *OndupUpdateBinder) validateSubqueryAdmission(astExpr tree.Expr) error {
	if b.rowAlias == nil && b.builder != nil && b.builder.odkuAdmission == nil && b.ctx != nil && astExprContainsSubquery(astExpr) {
		hasOuterReference := func() bool {
			probe := NewQueryBuilder(plan.Query_SELECT, b.builder.compCtx, b.builder.isPrepareStatement, true)
			probe.nextBindTag = b.builder.nextBindTag
			probe.odkuAdmission = &odkuAdmissionState{}
			probeCtx := cloneBindContextForWindowValidation(b.ctx)
			probeCtx.odkuAdmission = probe.odkuAdmission
			restoreCTEs := snapshotWindowValidationCTEState(probeCtx, true)
			defer restoreCTEs()
			binder := NewOndupUpdateBinder(b.GetContext(), probe, probeCtx,
				b.scanTag, b.selectTag, b.tableDef, b.targetDBName, b.targetTableName,
				b.lowerCaseTableNames, b.rowAlias)
			binder.SetTargetCorrelationTag(b.targetCorrelationTag)
			probeCtx.binder = binder
			_, _ = binder.BindExpr(cloneTreeExpr(astExpr), 0, true)
			return probe.odkuAdmission.hasOuterReference
		}()
		if hasOuterReference {
			return moerr.NewUnsupportedDML(b.GetContext(), odkuTargetCorrelatedSubqueryCause)
		}
	}
	return nil
}

func (b *OndupUpdateBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}

func (b *OndupUpdateBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
