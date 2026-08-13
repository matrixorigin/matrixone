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

package rule

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxPartitionTopN = uint64(1024)

// PartitionTopN annotates the dedicated PARTITION child of a ROW_NUMBER
// window when the post-window predicate proves a small, literal upper bound.
// Prepared plans deliberately skip this rule: their parameter values are not
// part of the reusable logical contract.
type PartitionTopN struct {
	prepared bool
}

func NewPartitionTopN(prepared bool) *PartitionTopN {
	return &PartitionTopN{prepared: prepared}
}

func (r *PartitionTopN) Match(node *plan.Node) bool {
	return !r.prepared && node.NodeType == plan.Node_WINDOW
}

func (r *PartitionTopN) Apply(node *plan.Node, qry *plan.Query, _ *process.Process) {
	if qry.StmtType != plan.Query_SELECT || len(node.WinSpecList) != 1 ||
		len(node.Children) != 1 || len(node.BindingTags) != 1 || len(node.FilterList) == 0 {
		return
	}

	window := node.WinSpecList[0].GetW()
	if window == nil || window.WindowFunc.GetF() == nil || window.WindowFunc.GetF().Func == nil ||
		len(window.PartitionBy) == 0 || len(window.OrderBy) == 0 ||
		hasUnsafeExpr(node.WinSpecList[0]) {
		return
	}
	windowFunctionID, _ := function.DecodeOverloadID(window.WindowFunc.GetF().Func.Obj)
	if windowFunctionID != function.ROW_NUMBER {
		return
	}
	for _, expr := range window.PartitionBy {
		if !partitionTopNHashCompatible(types.T(expr.Typ.Id)) {
			return
		}
	}

	child := qry.Nodes[node.Children[0]]
	if child.NodeType != plan.Node_PARTITION || len(child.OrderBy) != len(window.PartitionBy) || child.Limit != nil {
		return
	}

	windowTag, windowIdx := node.BindingTags[0], node.WindowIdx
	bound, found := uint64(0), false
	for _, filter := range node.FilterList {
		if hasUnsafeExpr(filter) || containsFunction(filter, function.OR) {
			return
		}
		candidate, recognized, referencesWindow := rankUpperBound(filter, windowTag, windowIdx)
		if referencesWindow && !recognized && !rankLowerBoundResidual(filter, windowTag, windowIdx) {
			return
		}
		if recognized && (!found || candidate < bound) {
			bound, found = candidate, true
		}
	}
	if !found || bound > maxPartitionTopN {
		return
	}

	// The generic planner intentionally shares PARTITION and Window PartitionBy
	// expressions. The bounded path remaps both at different physical layers,
	// so detach only the optimized child's copies before either remap runs.
	for _, spec := range child.OrderBy {
		spec.Expr = proto.Clone(spec.Expr).(*plan.Expr)
	}
	child.PartitionByCount = int32(len(window.PartitionBy))
	for _, spec := range window.OrderBy {
		child.OrderBy = append(child.OrderBy, proto.Clone(spec).(*plan.OrderBySpec))
	}
	child.Limit = &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_uint64), NotNullable: true},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_U64Val{U64Val: bound},
		}},
	}
}

func partitionTopNHashCompatible(typ types.T) bool {
	// Hash grouping canonicalizes floating NaNs and JSON values, while the
	// legacy PARTITION comparator does not use the same equality relation.
	switch typ {
	case types.T_float32, types.T_float64, types.T_json,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16:
		return false
	default:
		return true
	}
}

func rankLowerBoundResidual(expr *plan.Expr, tag, idx int32) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
		return false
	}
	fid, _ := function.DecodeOverloadID(fn.Func.Obj)
	leftRank := isRankRef(fn.Args[0], tag, idx)
	rightRank := isRankRef(fn.Args[1], tag, idx)
	if leftRank == rightRank {
		return false
	}
	literal := fn.Args[1]
	if rightRank {
		literal = fn.Args[0]
	}
	if !integerLiteral(literal) {
		return false
	}
	return leftRank && (fid == function.GREAT_EQUAL || fid == function.GREAT_THAN) ||
		rightRank && (fid == function.LESS_EQUAL || fid == function.LESS_THAN)
}

func integerLiteral(expr *plan.Expr) bool {
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return false
	}
	switch lit.Value.(type) {
	case *plan.Literal_I8Val, *plan.Literal_I16Val, *plan.Literal_I32Val, *plan.Literal_I64Val,
		*plan.Literal_U8Val, *plan.Literal_U16Val, *plan.Literal_U32Val, *plan.Literal_U64Val:
		return true
	default:
		return false
	}
}

func rankUpperBound(expr *plan.Expr, tag, idx int32) (uint64, bool, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return 0, false, referencesRank(expr, tag, idx)
	}
	fid, _ := function.DecodeOverloadID(fn.Func.Obj)
	if fid == function.BETWEEN && len(fn.Args) == 3 && isRankRef(fn.Args[0], tag, idx) {
		value, ok := nonNegativeInteger(fn.Args[2])
		return value, ok, true
	}
	if len(fn.Args) != 2 {
		return 0, false, referencesRank(expr, tag, idx)
	}
	leftRank := isRankRef(fn.Args[0], tag, idx)
	rightRank := isRankRef(fn.Args[1], tag, idx)
	if leftRank == rightRank {
		return 0, false, referencesRank(expr, tag, idx)
	}

	literal := fn.Args[1]
	if rightRank {
		literal = fn.Args[0]
	}
	value, ok := nonNegativeInteger(literal)
	if !ok {
		return 0, false, true
	}

	switch {
	case fid == function.EQUAL:
		return value, true, true
	case leftRank && fid == function.LESS_EQUAL,
		rightRank && fid == function.GREAT_EQUAL:
		return value, true, true
	case leftRank && fid == function.LESS_THAN,
		rightRank && fid == function.GREAT_THAN:
		if value == 0 {
			return 0, true, true
		}
		return value - 1, true, true
	default:
		return 0, false, true
	}
}

func nonNegativeInteger(expr *plan.Expr) (uint64, bool) {
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return 0, false
	}
	switch value := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return uint64(value.I8Val), value.I8Val >= 0
	case *plan.Literal_I16Val:
		return uint64(value.I16Val), value.I16Val >= 0
	case *plan.Literal_I32Val:
		return uint64(value.I32Val), value.I32Val >= 0
	case *plan.Literal_I64Val:
		return uint64(value.I64Val), value.I64Val >= 0
	case *plan.Literal_U8Val:
		return uint64(value.U8Val), true
	case *plan.Literal_U16Val:
		return uint64(value.U16Val), true
	case *plan.Literal_U32Val:
		return uint64(value.U32Val), true
	case *plan.Literal_U64Val:
		return value.U64Val, true
	default:
		return 0, false
	}
}

func isRankRef(expr *plan.Expr, tag, idx int32) bool {
	col := expr.GetCol()
	return col != nil && col.RelPos == tag && col.ColPos == idx
}

func referencesRank(expr *plan.Expr, tag, idx int32) bool {
	if expr == nil {
		return false
	}
	if isRankRef(expr, tag, idx) {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if referencesRank(arg, tag, idx) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if referencesRank(item, tag, idx) {
				return true
			}
		}
	}
	return false
}

func containsFunction(expr *plan.Expr, target int32) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		fid, _ := function.DecodeOverloadID(fn.Func.Obj)
		if fid == target {
			return true
		}
		for _, arg := range fn.Args {
			if containsFunction(arg, target) {
				return true
			}
		}
	}
	return false
}

func hasUnsafeExpr(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func == nil {
			return true
		}
		overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
		if !ok || overload.CannotFold() || overload.IsRealTimeRelated() {
			return true
		}
		for _, arg := range fn.Args {
			if hasUnsafeExpr(arg) {
				return true
			}
		}
	}
	if window := expr.GetW(); window != nil {
		if hasUnsafeExpr(window.WindowFunc) {
			return true
		}
		for _, item := range window.PartitionBy {
			if hasUnsafeExpr(item) {
				return true
			}
		}
		for _, item := range window.OrderBy {
			if hasUnsafeExpr(item.Expr) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if hasUnsafeExpr(item) {
				return true
			}
		}
	}
	return false
}
