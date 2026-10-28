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
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// virtualTimestampColumn reports whether the value of a virtual column may
// change when a reader uses a different session time zone from its writer.
// Stored generated columns are physical leaves. Classification memoizes each
// column, so a generated-column DAG costs linear work rather than repeatedly
// traversing shared dependencies. A malformed cycle is unsafe for index use.
func virtualTimestampColumn(table *plan.TableDef, pos int32) bool {
	if table == nil || pos < 0 || int(pos) >= len(table.Cols) ||
		(table.Cols[pos] != nil && (table.Cols[pos].GeneratedCol == nil || table.Cols[pos].GeneratedCol.IsStored)) {
		return false
	}
	return virtualTimestampColumnState(table, pos, make([]uint8, len(table.Cols)))
}

// bindVirtualTimestampRead is shared by ordinary references and JOIN USING
// operands. It also binds sequential UPDATE RHS references against the current
// projection, while the separate OLD projection retains the physical value.
func (bc *BindContext) bindVirtualTimestampRead(binding *Binding, pos, depth int32) (*plan.Expr, bool, error) {
	if bc == nil || binding == nil || bc.builder == nil {
		return nil, false, nil
	}
	table := binding.virtualTimestampTable
	if table == nil && bc.builder.qry != nil && binding.nodeId >= 0 && int(binding.nodeId) < len(bc.builder.qry.Nodes) {
		node := bc.builder.qry.Nodes[binding.nodeId]
		if node != nil && node.NodeType == plan.Node_TABLE_SCAN && len(node.BindingTags) == 1 &&
			node.BindingTags[0] == binding.tag {
			table = node.TableDef
		}
	}
	if table == nil || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil ||
		table.Cols[pos].GeneratedCol == nil || table.Cols[pos].GeneratedCol.IsStored {
		return nil, false, nil
	}
	if len(binding.virtualTimestampClass) != len(table.Cols) {
		binding.virtualTimestampClass = make([]uint8, len(table.Cols))
	}
	if !virtualTimestampColumnState(table, pos, binding.virtualTimestampClass) {
		return nil, false, nil
	}
	computed := expandVirtualTimestampColumnClassified(table, pos, binding.tag, depth,
		make(map[int32]bool), binding.virtualTimestampClass)
	if computed == nil {
		return nil, true, moerr.NewNotSupportedf(bc.builder.GetContext(),
			"virtual generated column '%s' needs %d read-expression nodes (limit %d); run the virtual timestamp preflight and migrate this table before upgrade",
			table.Cols[pos].Name, virtualTimestampReadCost(table, pos), maxVirtualTimestampExpansionNodes)
	}
	// A sequential UPDATE projection has already converted physical ENUM/SET
	// columns to display strings. Catalog generated expressions were bound to
	// their storage values, so recover those inputs before evaluating them.
	if binding.virtualTimestampTable != nil {
		var err error
		computed, err = adaptVirtualTimestampProjectionInputs(bc.builder.GetContext(), computed, table, binding)
		if err != nil {
			return nil, true, err
		}
	}
	// Sequential UPDATE binds its RHS to a PROJECT whose ENUM/SET columns
	// already contain display strings. Match that projection's representation;
	// ordinary table scans still expose the storage value to baseBinder, which
	// owns their display conversion.
	if binding.virtualTimestampTable != nil && int(pos) < len(binding.types) &&
		binding.types[pos] != nil && types.T(binding.types[pos].Id).IsMySQLString() &&
		isEnumOrSetPlanType(&computed.Typ) {
		var err error
		computed, err = makeEnumOrSetDisplayValue(bc.builder.GetContext(), computed)
		if err != nil {
			return nil, true, err
		}
	}
	if binding.virtualTimestampTable != nil {
		var nodes int
		if err := plan.VisitExprTree(computed, func(*plan.Expr) error {
			nodes++
			if nodes > maxVirtualTimestampExpansionNodes {
				return moerr.NewNotSupportedf(bc.builder.GetContext(),
					"virtual generated column '%s' needs more than %d read-expression nodes after projection conversion",
					table.Cols[pos].Name, maxVirtualTimestampExpansionNodes)
			}
			return nil
		}); err != nil {
			return nil, true, err
		}
	}
	for scope := bc; scope != nil; scope = scope.parent {
		if !scope.nullExtendedTags[binding.tag] {
			continue
		}
		rowPos, ok := table.Name2ColIndex[catalog.Row_ID]
		if !ok || rowPos < 0 || int(rowPos) >= len(binding.types) {
			return nil, true, moerr.NewNotSupportedf(bc.builder.GetContext(),
				"virtual generated column '%s' on a nullable join side requires a row identity", table.Cols[pos].Name)
		}
		rowID := &plan.Expr{Typ: *binding.types[rowPos]}
		rowID.Typ.NotNullable = false
		if depth == 0 {
			rowID.Expr = &plan.Expr_Col{Col: &plan.ColRef{RelPos: binding.tag, ColPos: rowPos}}
		} else {
			rowID.Expr = &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: binding.tag, ColPos: rowPos, Depth: depth}}
		}
		missing, err := BindFuncExprImplByPlanExpr(bc.builder.GetContext(), "isnull", []*plan.Expr{rowID})
		if err != nil {
			return nil, true, err
		}
		nullValue := makePlan2NullConstExprWithType()
		nullValue.Typ = computed.Typ
		nullValue.Typ.NotNullable = false
		computed, err = BindFuncExprImplByPlanExpr(bc.builder.GetContext(), "if", []*plan.Expr{missing, nullValue, computed})
		return computed, true, err
	}
	return computed, true, nil
}

func adaptVirtualTimestampProjectionInputs(ctx context.Context, expr *plan.Expr, table *plan.TableDef, binding *Binding) (*plan.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	var pos int32 = -1
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col != nil {
			pos = e.Col.ColPos
		}
	case *plan.Expr_Corr:
		if e.Corr != nil {
			pos = e.Corr.ColPos
		}
	case *plan.Expr_F:
		if e.F != nil {
			for i, arg := range e.F.Args {
				var err error
				e.F.Args[i], err = adaptVirtualTimestampProjectionInputs(ctx, arg, table, binding)
				if err != nil {
					return nil, err
				}
			}
		}
	case *plan.Expr_List:
		if e.List != nil {
			for i, arg := range e.List.List {
				var err error
				e.List.List[i], err = adaptVirtualTimestampProjectionInputs(ctx, arg, table, binding)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if pos < 0 || int(pos) >= len(table.Cols) || int(pos) >= len(binding.types) || binding.types[pos] == nil ||
		table.Cols[pos] == nil || !isEnumOrSetPlanType(&table.Cols[pos].Typ) ||
		!types.T(binding.types[pos].Id).IsMySQLString() {
		return expr, nil
	}
	storage := &table.Cols[pos].Typ
	if !mysqlSpecialNumericTypeReversible(storage) {
		return nil, moerr.NewNotSupportedf(ctx, "virtual generated column '%s' depends on a non-reversible ENUM/SET projection", table.Cols[pos].Name)
	}
	display := DeepCopyExpr(expr)
	display.Typ = *DeepCopyType(binding.types[pos])
	return makeMySQLSpecialNumericValue(ctx, display, storage)
}

// state: 0 unseen, 1 visiting, 2 safe, 3 affected.
func virtualTimestampColumnState(table *plan.TableDef, pos int32, state []uint8) bool {
	if table == nil || pos < 0 || int(pos) >= len(table.Cols) {
		return false
	}
	switch state[pos] {
	case 1, 3:
		return true
	case 2:
		return false
	}
	col := table.Cols[pos]
	if col == nil {
		state[pos] = 3
		return true
	}
	if col.GeneratedCol == nil || col.GeneratedCol.IsStored {
		state[pos] = 2
		return false
	}
	if col.GeneratedCol.Expr == nil {
		state[pos] = 3
		return true
	}
	state[pos] = 1
	if virtualExprUsesTimestamp(table, col.GeneratedCol.Expr, state) {
		state[pos] = 3
		return true
	}
	state[pos] = 2
	return false
}

func virtualExprUsesTimestamp(table *plan.TableDef, expr *plan.Expr, state []uint8) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(table.Cols) {
			return true
		}
		col := table.Cols[e.Col.ColPos]
		return col == nil || types.T(col.Typ.Id) == types.T_timestamp || virtualTimestampColumnState(table, e.Col.ColPos, state)
	case *plan.Expr_F:
		if e.F == nil {
			return true
		}
		for _, arg := range e.F.Args {
			if virtualExprUsesTimestamp(table, arg, state) {
				return true
			}
		}
	case *plan.Expr_List:
		if e.List == nil {
			return true
		}
		for _, arg := range e.List.List {
			if virtualExprUsesTimestamp(table, arg, state) {
				return true
			}
		}
	case *plan.Expr_Lit, *plan.Expr_T:
		return false
	default:
		// An unfamiliar catalog expression must not make a persisted index
		// appear safe when we cannot inspect all of its inputs.
		return true
	}
	return false
}

// expandVirtualTimestampColumn substitutes read-time expressions for affected
// virtual dependencies. It only receives catalog expressions whose column
// references are local (RelPos 0); the copy is safe for each query binding.
const maxVirtualTimestampExpansionNodes = 65536

// VirtualTimestampExpansionFinding identifies a generated column whose
// read-time expression cannot be expanded within the planner's resource bound.
// Cost is saturated at maxVirtualTimestampExpansionNodes+1.
type VirtualTimestampExpansionFinding struct {
	Column string
	Cost   int
}

// VirtualTimestampExpansionFindings is shared by DDL admission and the
// catalog preflight command. It examines the complete table, including hidden
// generated columns, so an ALTER of an upstream dependency cannot evade it.
func VirtualTimestampExpansionFindings(table *plan.TableDef) []VirtualTimestampExpansionFinding {
	if table == nil {
		return nil
	}
	classified := make([]uint8, len(table.Cols))
	state := make([]uint8, len(table.Cols))
	costs := make([]int, len(table.Cols))
	var findings []VirtualTimestampExpansionFinding
	for pos, col := range table.Cols {
		if col == nil || !virtualTimestampColumnState(table, int32(pos), classified) {
			continue
		}
		cost := virtualTimestampColumnCost(table, int32(pos), classified, state, costs)
		if cost > maxVirtualTimestampExpansionNodes {
			findings = append(findings, VirtualTimestampExpansionFinding{Column: col.Name, Cost: cost})
		}
	}
	return findings
}

func validateVirtualTimestampExpansion(ctx context.Context, table *plan.TableDef) error {
	for _, finding := range VirtualTimestampExpansionFindings(table) {
		return moerr.NewNotSupportedf(ctx,
			"table '%s' virtual generated column '%s' needs more than %d read-expression nodes",
			table.Name, finding.Column, maxVirtualTimestampExpansionNodes)
	}
	return nil
}

func virtualTimestampReadCost(table *plan.TableDef, pos int32) int {
	if table == nil {
		return maxVirtualTimestampExpansionNodes + 1
	}
	n := len(table.Cols)
	return virtualTimestampColumnCost(table, pos, make([]uint8, n), make([]uint8, n), make([]int, n))
}

func virtualTimestampColumnCost(table *plan.TableDef, pos int32, classified, state []uint8, costs []int) int {
	if pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || table.Cols[pos].GeneratedCol == nil {
		return maxVirtualTimestampExpansionNodes + 1
	}
	switch state[pos] {
	case 1:
		return maxVirtualTimestampExpansionNodes + 1
	case 2:
		return costs[pos]
	}
	state[pos] = 1
	cost := virtualTimestampExprCost(table, table.Cols[pos].GeneratedCol.Expr, classified, state, costs)
	state[pos], costs[pos] = 2, cost
	return cost
}

func virtualTimestampExprCost(table *plan.TableDef, expr *plan.Expr, classified, state []uint8, costs []int) int {
	if expr == nil {
		return maxVirtualTimestampExpansionNodes + 1
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(table.Cols) {
			return maxVirtualTimestampExpansionNodes + 1
		}
		if virtualTimestampColumnState(table, e.Col.ColPos, classified) {
			return virtualTimestampColumnCost(table, e.Col.ColPos, classified, state, costs)
		}
		return 1
	case *plan.Expr_F:
		if e.F == nil {
			return maxVirtualTimestampExpansionNodes + 1
		}
		cost := 1
		for _, arg := range e.F.Args {
			cost = saturatingVirtualTimestampCost(cost, virtualTimestampExprCost(table, arg, classified, state, costs))
		}
		return cost
	case *plan.Expr_List:
		if e.List == nil {
			return maxVirtualTimestampExpansionNodes + 1
		}
		cost := 1
		for _, arg := range e.List.List {
			cost = saturatingVirtualTimestampCost(cost, virtualTimestampExprCost(table, arg, classified, state, costs))
		}
		return cost
	case *plan.Expr_Lit, *plan.Expr_T:
		return 1
	default:
		return maxVirtualTimestampExpansionNodes + 1
	}
}

func saturatingVirtualTimestampCost(a, b int) int {
	if a > maxVirtualTimestampExpansionNodes || b > maxVirtualTimestampExpansionNodes ||
		a > maxVirtualTimestampExpansionNodes-b {
		return maxVirtualTimestampExpansionNodes + 1
	}
	return a + b
}

func expandVirtualTimestampColumn(table *plan.TableDef, pos, tag, depth int32, visiting map[int32]bool) *plan.Expr {
	if table == nil {
		return nil
	}
	return expandVirtualTimestampColumnClassified(table, pos, tag, depth, visiting, make([]uint8, len(table.Cols)))
}

func expandVirtualTimestampColumnClassified(table *plan.TableDef, pos, tag, depth int32, visiting map[int32]bool, classified []uint8) *plan.Expr {
	remaining := maxVirtualTimestampExpansionNodes
	return expandVirtualTimestampColumnBounded(table, pos, tag, depth, visiting, &remaining, classified)
}

func expandVirtualTimestampColumnBounded(table *plan.TableDef, pos, tag, depth int32, visiting map[int32]bool, remaining *int, classified []uint8) *plan.Expr {
	if pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || table.Cols[pos].GeneratedCol == nil || visiting[pos] {
		return nil
	}
	visiting[pos] = true
	defer delete(visiting, pos)
	return expandVirtualTimestampExpr(table, DeepCopyExpr(table.Cols[pos].GeneratedCol.Expr), tag, depth, visiting, remaining, classified)
}

func expandVirtualTimestampExpr(table *plan.TableDef, expr *plan.Expr, tag, depth int32, visiting map[int32]bool, remaining *int, classified []uint8) *plan.Expr {
	if expr == nil || *remaining == 0 {
		return nil
	}
	// An affected column reference is replaced by its expression. The
	// placeholder does not survive in the plan and must not consume the
	// budget used by virtualTimestampExprCost and DDL admission.
	if col, ok := expr.Expr.(*plan.Expr_Col); ok && col.Col != nil &&
		col.Col.ColPos >= 0 && int(col.Col.ColPos) < len(table.Cols) &&
		virtualTimestampColumnState(table, col.Col.ColPos, classified) {
		return expandVirtualTimestampColumnBounded(table, col.Col.ColPos, tag, depth, visiting, remaining, classified)
	}
	*remaining = *remaining - 1
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(table.Cols) {
			return nil
		}
		pos := e.Col.ColPos
		if depth > 0 {
			expr.Expr = &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: tag, ColPos: pos, Depth: depth}}
		} else {
			e.Col.RelPos = tag
		}
	case *plan.Expr_F:
		if e.F == nil {
			return nil
		}
		for i, arg := range e.F.Args {
			e.F.Args[i] = expandVirtualTimestampExpr(table, arg, tag, depth, visiting, remaining, classified)
			if e.F.Args[i] == nil {
				return nil
			}
		}
	case *plan.Expr_List:
		if e.List == nil {
			return nil
		}
		for i, arg := range e.List.List {
			e.List.List[i] = expandVirtualTimestampExpr(table, arg, tag, depth, visiting, remaining, classified)
			if e.List.List[i] == nil {
				return nil
			}
		}
	case *plan.Expr_Lit, *plan.Expr_T:
		// No row-local references.
	default:
		return nil
	}
	return expr
}

func indexUsesVirtualTimestamp(idx *plan.IndexDef, table *plan.TableDef) bool {
	if idx == nil || table == nil {
		return false
	}
	state := make([]uint8, len(table.Cols))
	for _, part := range idx.Parts {
		if pos, ok := table.Name2ColIndex[catalog.ResolveAlias(part)]; ok &&
			virtualTimestampColumnState(table, pos, state) {
			return true
		}
	}
	return false
}

func indexUnsafeForRead(idx *plan.IndexDef, table *plan.TableDef) bool {
	return indexUsesVirtualTimestamp(idx, table) || indexUsesUnicodeCollation(idx, table)
}
