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
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// fuseScalarAggregates combines adjacent scalar aggregates over the same bag
// input. Global aggregates without HAVING always emit exactly one row, even on
// empty input. Thus (X LEFT JOIN agg1(R) ON true) LEFT JOIN agg2(R) ON true
// needs only one aggregate carrying both sets of results. The same holds for
// an unconditional INNER join of the two singleton relations.
//
// This is input equality, not semi-join containment: matching only the set of
// keys would lose multiplicity. Keep the first version to identical filtered
// base scans and fixed-state numeric aggregates; do not broaden predicates or
// introduce a materialized producer. No cardinality estimate proves legality.
func (builder *QueryBuilder) fuseScalarAggregates(nodeID int32) (int32, bool) {
	remap := make(map[[2]int32]*planpb.Expr)
	var visit func(int32) int32
	visit = func(id int32) int32 {
		node := builder.qry.Nodes[id]
		for i, child := range node.Children {
			node.Children[i] = visit(child)
		}
		if !scalarAggUnconditionalJoin(node) {
			return id
		}
		leftID := node.Children[0]
		left := builder.qry.Nodes[leftID]
		keep := left
		if scalarAggUnconditionalJoin(left) {
			keep = builder.qry.Nodes[left.Children[1]]
		}
		drop := builder.qry.Nodes[node.Children[1]]
		keepScan, ok := builder.scalarAggInput(keep)
		if !ok {
			return id
		}
		dropScan, ok := builder.scalarAggInput(drop)
		if !ok || keep.BindingTags[1] == drop.BindingTags[1] ||
			!sameScalarAggInput(keepScan, dropScan) {
			return id
		}
		mapping := map[int32]int32{dropScan.BindingTags[0]: keepScan.BindingTags[0]}
		for i, agg := range drop.AggList {
			mapped, _ := remapSemiContainmentExpr(agg, mapping, true)
			pos := int32(len(keep.AggList))
			keep.AggList = append(keep.AggList, mapped)
			remap[[2]int32{drop.BindingTags[1], int32(i)}] =
				GetColExpr(agg.Typ, keep.BindingTags[1], pos)
		}
		return leftID
	}
	root := visit(nodeID)
	if len(remap) == 0 {
		return root, false
	}
	// A previously fused right subtree may itself be fused into a left one.
	// Resolve the complete output map before visiting consumers, so references
	// to the first eliminated aggregate cannot stop at another eliminated tag.
	var resolve func([2]int32) *planpb.Expr
	resolve = func(key [2]int32) *planpb.Expr {
		expr := remap[key]
		col := expr.GetCol()
		next := [2]int32{col.RelPos, col.ColPos}
		if remap[next] != nil {
			expr = resolve(next)
			remap[key] = expr
		}
		return expr
	}
	for key := range remap {
		resolve(key)
	}
	builder.applyEffectlessAggRemap(root, remap)
	return root, true
}

func scalarAggUnconditionalJoin(node *planpb.Node) bool {
	if node.NodeType != planpb.Node_JOIN || len(node.Children) != 2 ||
		(node.JoinType != planpb.Node_INNER && node.JoinType != planpb.Node_LEFT) ||
		!scalarAggPlainNode(node) || len(node.BindingTags) != 0 || len(node.FilterList) != 0 {
		return false
	}
	for _, expr := range node.OnList {
		lit := expr.GetLit()
		if lit == nil || lit.Isnull || !lit.GetBval() || lit.Src != nil ||
			types.T(expr.Typ.Id) != types.T_bool {
			return false
		}
	}
	return true
}

func scalarAggPlainNode(node *planpb.Node) bool {
	return len(node.ProjectList) == 0 && node.Limit == nil && node.Offset == nil &&
		len(node.OrderBy) == 0 && len(node.RuntimeFilterProbeList) == 0 &&
		len(node.RuntimeFilterBuildList) == 0 && len(node.SendMsgList) == 0 &&
		len(node.RecvMsgList) == 0 && !node.IsEnd && !node.NotCacheable &&
		node.SampleFunc == nil && node.ExtraOptions == ""
}

func (builder *QueryBuilder) scalarAggInput(agg *planpb.Node) (*planpb.Node, bool) {
	if agg.NodeType != planpb.Node_AGG || len(agg.Children) != 1 ||
		len(agg.BindingTags) != 2 || len(agg.GroupBy) != 0 || len(agg.GroupingFlag) != 0 ||
		len(agg.FilterList) != 0 || len(agg.AggList) == 0 || !scalarAggPlainNode(agg) {
		return nil, false
	}
	scan := builder.qry.Nodes[agg.Children[0]]
	if scan.NodeType != planpb.Node_TABLE_SCAN || len(scan.Children) != 0 ||
		len(scan.BindingTags) != 1 || scan.ObjRef == nil || scan.TableDef == nil ||
		!scalarAggPlainNode(scan) || scan.ParentObjRef != nil ||
		scan.IndexReaderParam != nil || scan.ClusterTable != nil || scan.ExternScan != nil ||
		(scan.TableDef.TableType != "" && scan.TableDef.TableType != catalog.SystemOrdinaryRel) ||
		scan.ObjRef.SchemaName == catalog.MO_CATALOG {
		return nil, false
	}
	for _, filter := range scan.FilterList {
		if !isTruncationSafePredicateExpr(filter) ||
			!scalarAggLocalExpr(filter, scan.BindingTags[0]) {
			return nil, false
		}
	}
	for _, expr := range agg.AggList {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || uint64(fn.Func.Obj)&function.Distinct != 0 || len(fn.Args) != 1 {
			return nil, false
		}
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		switch id {
		case function.COUNT, function.STARCOUNT, function.SUM, function.AVG, function.MIN, function.MAX:
		default:
			return nil, false
		}
		arg := fn.Args[0]
		typ := types.T(arg.Typ.Id)
		if (!typ.IsInteger() && !typ.IsFloat() && !typ.IsDecimal()) ||
			(arg.GetCol() == nil && arg.GetLit() == nil) ||
			!scalarAggLocalExpr(arg, scan.BindingTags[0]) {
			return nil, false
		}
	}
	return scan, true
}

// Require every column to belong to the admitted base scan. In particular an
// equal-looking correlated expression or a prepared literal with executable
// provenance must not become an input-equality proof.
func scalarAggLocalExpr(expr *planpb.Expr, tag int32) bool {
	if expr == nil || expr.AuxId < 0 {
		return false
	}
	switch v := expr.Expr.(type) {
	case *planpb.Expr_Col:
		return v.Col != nil && v.Col.RelPos == tag
	case *planpb.Expr_Lit:
		return v.Lit != nil && v.Lit.Src == nil
	case *planpb.Expr_F:
		for _, arg := range v.F.Args {
			if !scalarAggLocalExpr(arg, tag) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func sameScalarAggInput(left, right *planpb.Node) bool {
	if !sameSemiContainmentScan(left, right) || !proto.Equal(left.TableDef, right.TableDef) ||
		!proto.Equal(&left.IndexScanInfo, &right.IndexScanInfo) ||
		len(left.FilterList) != len(right.FilterList) {
		return false
	}
	mapping := map[int32]int32{right.BindingTags[0]: left.BindingTags[0]}
	for i, filter := range right.FilterList {
		mapped, ok := remapSemiContainmentExpr(filter, mapping, true)
		if !ok || !scalarAggExprEqual(left.FilterList[i], mapped) {
			return false
		}
	}
	return true
}

func scalarAggExprEqual(left, right *planpb.Expr) bool {
	if !sameSemiContainmentType(left.Typ, right.Typ) || !exprStructuralEqual(left, right) {
		return false
	}
	if fn := left.GetF(); fn != nil {
		for i, arg := range fn.Args {
			if !scalarAggExprEqual(arg, right.GetF().Args[i]) {
				return false
			}
		}
	}
	return true
}
