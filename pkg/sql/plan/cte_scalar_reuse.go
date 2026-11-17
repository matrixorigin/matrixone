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
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// cteScalarReuseSafe admits a single materialized scalar aggregate underneath
// inline CTEs. It does not admit arbitrary nested materialization: producer
// trees must contain only scans, filters, projections and aggregates. Every
// occurrence must execute, and every published column must be demanded by at
// least one unconditionally evaluated consumer expression in the original plan.
func (builder *QueryBuilder) cteScalarReuseSafe(rootID int32, occurrences []cteOccurrence) bool {
	if len(occurrences) < 2 {
		return false
	}
	first := occurrences[0]
	identity, ok := builder.cteScalarIdentity(first.rootID)
	if !ok {
		return false
	}
	byRoot := make(map[int32]cteOccurrence, len(occurrences))
	for i, occurrence := range occurrences {
		if i != 0 {
			other, valid := builder.cteScalarIdentity(occurrence.rootID)
			if !valid || len(identity) != len(other) {
				return false
			}
			for j := range identity {
				if !proto.Equal(identity[j], other[j]) {
					return false
				}
			}
		}
		byRoot[occurrence.rootID] = occurrence
	}
	if len(byRoot) != len(occurrences) {
		return false
	}

	published := make([]bool, len(first.types))
	visited := make(map[int32]bool)
	found := 0
	// Demand is keyed by binding identity, not by textual column names. Only
	// live projection positions are followed; dead expressions cannot justify
	// eagerly evaluating a producer column.
	var visit func(int32, map[[2]int32]int) (uint64, bool)
	visit = func(id int32, demand map[[2]int32]int) (uint64, bool) {
		if id < 0 || int(id) >= len(builder.qry.Nodes) || visited[id] {
			return 0, false
		}
		visited[id] = true
		node := builder.qry.Nodes[id]
		if node == nil || node.Offset != nil || len(node.FilterList) != 0 {
			return 0, false
		}
		if occurrence, exists := byRoot[id]; exists {
			if len(demand) == 0 {
				return 0, false
			}
			for ref := range demand {
				if ref[0] != occurrence.rootTag || ref[1] < 0 || int(ref[1]) >= len(published) {
					return 0, false
				}
				published[ref[1]] = true
			}
			found++
			return 1, true
		}
		childDemand := make(map[[2]int32]int)
		var bound uint64
		var valid bool
		switch node.NodeType {
		case planpb.Node_PROJECT, planpb.Node_UNION_ALL:
			if len(node.BindingTags) != 1 || len(node.Children) != 1 && node.NodeType == planpb.Node_PROJECT ||
				len(node.Children) != 2 && node.NodeType == planpb.Node_UNION_ALL {
				return 0, false
			}
			for _, expr := range node.ProjectList {
				if expr == nil || !exprCanRemoveProject(expr) {
					return 0, false
				}
			}
			for ref := range demand {
				if ref[0] != node.BindingTags[0] || ref[1] < 0 || int(ref[1]) >= len(node.ProjectList) {
					return 0, false
				}
				if !cteScalarDemand(node.ProjectList[ref[1]], childDemand) {
					return 0, false
				}
			}
			bound, valid = visit(node.Children[0], childDemand)
			if !valid {
				return 0, false
			}
			if node.NodeType == planpb.Node_UNION_ALL {
				if node.Children[1] < 0 || int(node.Children[1]) >= len(builder.qry.Nodes) {
					return 0, false
				}
				right := builder.qry.Nodes[node.Children[1]]
				if right == nil || len(right.BindingTags) != 1 || len(right.ProjectList) != len(node.ProjectList) {
					return 0, false
				}
				rightDemand := make(map[[2]int32]int)
				for ref := range demand {
					rightDemand[[2]int32{right.BindingTags[0], ref[1]}] = 1
				}
				n, ok := visit(node.Children[1], rightDemand)
				if !ok || n > math.MaxUint64-bound {
					return 0, false
				}
				bound += n
			}
		case planpb.Node_SORT, planpb.Node_WINDOW:
			if len(node.Children) != 1 {
				return 0, false
			}
			for ref := range demand {
				childDemand[ref] = 1
			}
			if node.NodeType == planpb.Node_WINDOW {
				// Only the ordinary, unpartitioned row number is transparent in
				// this local proof. Other windows keep the existing inline path.
				if len(node.BindingTags) != 1 || len(node.WinSpecList) != 1 {
					return 0, false
				}
				window := node.WinSpecList[0].GetW()
				ref := [2]int32{node.BindingTags[0], node.GetWindowIdx()}
				if window == nil || window.WindowFunc.GetF().GetFunc().GetObjName() != "row_number" ||
					len(window.PartitionBy) != 0 || len(window.OrderBy) != 0 || childDemand[ref] == 0 {
					return 0, false
				}
				delete(childDemand, ref)
			}
			for _, order := range node.OrderBy {
				if order == nil || !cteScalarDemand(order.Expr, childDemand) {
					return 0, false
				}
			}
			bound, valid = visit(node.Children[0], childDemand)
			if !valid {
				return 0, false
			}
		default:
			return 0, false
		}
		return bound, cteScalarLimitCovers(node.Limit, bound)
	}
	if rootID < 0 || int(rootID) >= len(builder.qry.Nodes) {
		return false
	}
	root := builder.qry.Nodes[rootID]
	if root == nil || len(root.BindingTags) != 1 {
		return false
	}
	demand := make(map[[2]int32]int)
	for i := range root.ProjectList {
		demand[[2]int32{root.BindingTags[0], int32(i)}] = 1
	}
	if _, ok := visit(rootID, demand); !ok || found != len(occurrences) {
		return false
	}
	for _, needed := range published {
		if !needed {
			return false
		}
	}
	return len(published) > 0
}

func cteScalarLimitCovers(limit *planpb.Expr, bound uint64) bool {
	if limit == nil {
		return true
	}
	literal := limit.GetLit()
	if literal == nil || literal.Isnull {
		return false
	}
	switch value := literal.Value.(type) {
	case *planpb.Literal_U64Val:
		return value.U64Val > 0 && value.U64Val >= bound
	case *planpb.Literal_I64Val:
		return value.I64Val > 0 && uint64(value.I64Val) >= bound
	default:
		return false
	}
}

// cteScalarDemand collects only unconditional dependencies. Conditional arms
// and unknown function evaluation contracts do not prove a column is needed.
// In particular, a syntactic reference in IF(false, bad_column, 0) is not a
// reason to evaluate bad_column in a materialized producer.
func cteScalarDemand(expr *planpb.Expr, demand map[[2]int32]int) bool {
	if expr == nil || !exprCanRemoveProject(expr) {
		return false
	}
	switch e := expr.Expr.(type) {
	case *planpb.Expr_Col:
		demand[[2]int32{e.Col.RelPos, e.Col.ColPos}] = 1
	case *planpb.Expr_Lit, *planpb.Expr_T:
	case *planpb.Expr_F:
		if e.F.Func == nil {
			return false
		}
		args := e.F.Args
		switch e.F.Func.ObjName {
		case "case":
			// The binder lowers NULLIF(column, literal) to this form. The
			// tested column is necessary regardless of which arm is selected.
			// Other conditionals may fold away a syntactic column reference.
			if len(args) != 3 || args[1].GetLit() == nil || !args[1].GetLit().Isnull {
				return true
			}
			condition := args[0].GetF()
			if condition.GetFunc().GetObjName() != "=" || len(condition.Args) != 2 ||
				condition.Args[0].GetCol() == nil || condition.Args[1].GetLit() == nil ||
				condition.Args[1].GetLit().Isnull || !exprStructuralEqual(condition.Args[0], args[2]) {
				return true
			}
			return cteScalarDemand(condition.Args[0], demand)
		case "isnull", "is_null", "isnotnull", "is_not_null":
			if len(args) != 1 || args[0] == nil {
				return false
			}
			if args[0].Typ.NotNullable {
				return true
			}
		case "+", "-", "*", "/", "=", "<>", "<", ">", "<=", ">=", "cast", "abs", "round", "format", "concat":
			for _, arg := range args {
				if arg == nil {
					return false
				}
				if literal := arg.GetLit(); literal != nil && literal.Isnull {
					return true
				}
			}
		default:
			return true
		}
		for _, arg := range args {
			refs := make(map[[2]int32]int)
			if !cteScalarDemand(arg, refs) || arg.GetF() != nil && len(refs) == 0 && !cteScalarNonNullLiteralCast(arg) {
				// A constant/conditional function argument can fold to NULL
				// and erase its siblings. Do not evaluate it during this proof.
				return false
			}
			for ref := range refs {
				demand[ref] = 1
			}
		}
	default:
		return false
	}
	return true
}

// FORMAT's precision argument is bound as CAST(integer_literal AS VARCHAR).
// This conversion cannot produce NULL. Do not generalize to arbitrary constant
// expressions or casts: a failed temporal conversion, for example, can be NULL.
func cteScalarNonNullLiteralCast(expr *planpb.Expr) bool {
	fn := expr.GetF()
	if fn.GetFunc().GetObjName() != "cast" || len(fn.Args) != 2 || expr.Typ.Id != int32(types.T_varchar) {
		return false
	}
	arg := fn.Args[0]
	return arg.GetLit() != nil && !arg.GetLit().Isnull && makeTypeByPlan2Type(arg.Typ).IsNumeric()
}

// cteScalarIdentity normalizes only binding tags and node ids. All semantic
// fields (including internal expression types, filters, grouping and scan
// snapshots) must match. Matching just the final output types is insufficient
// when different consumers provide different numeric binding contexts.
func (builder *QueryBuilder) cteScalarIdentity(rootID int32) ([]*planpb.Node, bool) {
	nodes := make([]*planpb.Node, 0)
	tags := make(map[int32]int32)
	seen := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(id int32) bool {
		if id < 0 || int(id) >= len(builder.qry.Nodes) || seen[id] {
			return false
		}
		seen[id] = true
		node := builder.qry.Nodes[id]
		if node == nil || node.Limit != nil || node.Offset != nil {
			return false
		}
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			if len(node.Children) != 0 {
				return false
			}
		case planpb.Node_PROJECT, planpb.Node_FILTER, planpb.Node_AGG:
			if len(node.Children) != 1 {
				return false
			}
		default:
			return false
		}
		for _, tag := range node.BindingTags {
			if _, exists := tags[tag]; exists {
				return false
			}
			tags[tag] = int32(len(tags))
		}
		nodes = append(nodes, node)
		for _, child := range node.Children {
			if !visit(child) {
				return false
			}
		}
		return true
	}
	if !visit(rootID) {
		return nil, false
	}
	// Require a scalar aggregate behind only direct-column projections. A
	// computed outer projection remains inline, with its original error domain.
	scalar := builder.qry.Nodes[rootID]
	for scalar.NodeType == planpb.Node_PROJECT {
		if len(scalar.FilterList) != 0 {
			return nil, false
		}
		for _, expr := range scalar.ProjectList {
			if expr.GetCol() == nil {
				return nil, false
			}
		}
		scalar = builder.qry.Nodes[scalar.Children[0]]
	}
	if scalar.NodeType != planpb.Node_AGG || len(scalar.GroupBy) != 0 || len(scalar.FilterList) != 0 {
		return nil, false
	}
	result := make([]*planpb.Node, 0, len(nodes))
	for _, original := range nodes {
		// proto.Clone intentionally preserves fields not handled by the older
		// DeepCopyNode helper, notably ScanSnapshot.
		node := proto.Clone(original).(*planpb.Node)
		node.NodeId, node.Children, node.Stats, node.AnalyzeInfo = 0, nil, nil, nil
		for i, tag := range node.BindingTags {
			node.BindingTags[i] = tags[tag]
		}
		for _, list := range [][]*planpb.Expr{node.ProjectList, node.FilterList, node.GroupBy, node.AggList, node.BlockFilterList} {
			for _, expr := range list {
				if !cteScalarNormalizeExpr(expr, tags) {
					return nil, false
				}
			}
		}
		result = append(result, node)
	}
	return result, true
}

func cteScalarNormalizeExpr(expr *planpb.Expr, tags map[int32]int32) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *planpb.Expr_Col:
		tag, ok := tags[e.Col.RelPos]
		if !ok {
			return false
		}
		e.Col.RelPos = tag
	case *planpb.Expr_F:
		for _, arg := range e.F.Args {
			if !cteScalarNormalizeExpr(arg, tags) {
				return false
			}
		}
	case *planpb.Expr_List:
		for _, arg := range e.List.List {
			if !cteScalarNormalizeExpr(arg, tags) {
				return false
			}
		}
	case *planpb.Expr_Lit, *planpb.Expr_T, *planpb.Expr_P:
	default:
		return false
	}
	return true
}
