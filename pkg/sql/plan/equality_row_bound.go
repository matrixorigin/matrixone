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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
)

const equalityRowBoundAttempts = 4

// tightenIndexedEquality uses a snapshot observation for costing. It leaves the
// actual row predicate and runtime filter budgets authoritative.
func (builder *QueryBuilder) tightenIndexedEquality(node *plan.Node, expr *plan.Expr, tableRows float64) float64 {
	selection := expr.Selectivity
	if bounded, found := builder.equalityRowBounds[expr]; found {
		return min(selection, bounded)
	}
	if tableRows < 50000 || selection < 0.1 ||
		len(builder.equalityRowBounds) >= equalityRowBoundAttempts ||
		node.TableDef == nil || len(node.TableDef.Indexes) == 0 ||
		node.ObjRef == nil || node.ObjRef.PubInfo != nil || node.ObjRef.SubscriptionName != "" ||
		node.ObjRef.Obj <= 0 || uint64(node.ObjRef.Obj) != node.TableDef.TblId || node.TableDef.TableType != catalog.SystemOrdinaryRel ||
		node.ScanSnapshot.GetTS() != nil || node.ObjRef.Snapshot.GetTS() != nil ||
		node.TableDef.IsTemporary || node.TableDef.IsDynamic || node.TableDef.Partition != nil ||
		node.TableDef.FeatureFlag&(features.Partitioned|features.Partition) != 0 ||
		catalog.IsSystemTable(node.TableDef.TblId) || len(node.BindingTags) != 1 {
		return selection
	}
	provider, ok := builder.compCtx.(interface {
		BoundEqualityRows(*plan.TableDef, *plan.ColDef, []byte, types.Type) (candidates, total uint64)
	})
	fn := expr.GetF()
	if !ok || builder.isReusablePlan() || fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
		return selection
	}
	left, right := fn.Args[0], fn.Args[1]
	if left.GetCol() == nil {
		left, right = right, left
	}
	col, lit := left.GetCol(), right.GetLit()
	if col == nil || lit == nil || lit.Isnull || col.RelPos != node.BindingTags[0] ||
		col.ColPos < 0 || int(col.ColPos) >= len(node.TableDef.Cols) {
		return selection
	}
	def := node.TableDef.Cols[col.ColPos]
	oid := types.T(left.Typ.Id)
	// These direct comparisons have the same byte/integer ordering as the
	// ordinary reader's equality zonemap checks.
	if def == nil || def.Primary || def.Typ.Id != left.Typ.Id || left.Typ.Id != right.Typ.Id ||
		(!oid.IsInteger() && oid != types.T_varchar && oid != types.T_varbinary) {
		return selection
	}
	if pos, ok := node.TableDef.Name2ColIndex[def.Name]; !ok || pos != col.ColPos {
		return selection
	}
	if (oid == types.T_varchar || oid == types.T_varbinary) && (len(lit.GetSval()) > 256 ||
		types.IsUnicodeCollation(uint8(def.Typ.Charset)) || types.IsUnicodeCollation(uint8(right.Typ.Charset))) {
		return selection
	}
	if !builder.scanHintsForceIndexes(node) && hasOrderedPrimaryKey(node.TableDef) && hasPrimaryKeyEquality(node) {
		return selection
	}
	if !builder.leadingRegularIndexEquality(builder.filterRegularIndexesByScanHints(node, node.TableDef.Indexes), node.TableDef, def.Name, fn) {
		return selection
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil || proc.Base == nil || proc.Mp() == nil {
		return selection
	}
	if builder.equalityRowBounds == nil {
		builder.equalityRowBounds = make(map[*plan.Expr]float64)
	}
	budget := 257 // One scalar plus at most 256 bytes copied from the literal.
	scalar, ok, _ := evaluateIndexCostScalar(builder.GetContext(), proc, right, &budget)
	if !ok || scalar.null || scalar.typ.Oid != oid || types.IsUnicodeCollation(scalar.typ.Charset) {
		builder.equalityRowBounds[expr] = selection
		return selection
	}
	bound, snapshotRows := provider.BoundEqualityRows(node.TableDef, def, scalar.value, scalar.typ)
	if bound < snapshotRows {
		// Cached global statistics can lag this snapshot. A difference in
		// table size alone must never make the predicate look more selective.
		selection = min(selection, float64(max(bound, 1))/min(tableRows, float64(snapshotRows)))
	}
	builder.equalityRowBounds[expr] = selection
	return selection
}

func (builder *QueryBuilder) leadingRegularIndexEquality(indexes []*plan.IndexDef, table *plan.TableDef, name string, fn *plan.Function) bool {
	for _, idx := range indexes {
		if !usableRegularHintIndex(idx) || len(idx.Parts) == 0 || (!idx.Unique && len(idx.Parts) < 2) ||
			catalog.ResolveAlias(idx.Parts[0]) != name || indexUnsafeForRead(idx, table) {
			continue
		}
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idx.IndexAlgoParams)
		if err == nil && prefixLengths[name] == 0 && builder.regularIndexEqualityUsable(idx, table, fn, 0) {
			return true
		}
	}
	return false
}
