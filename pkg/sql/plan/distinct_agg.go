// Copyright 2023 Matrix Origin
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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	internalSumCombineName   = "__mo_sum_combine"
	internalCountCombineName = "__mo_count_combine"
	internalAvgCombineName   = "__mo_avg_combine"
)

// RequiresSingleStageDistinctAgg reports whether an aggregate node contains a
// DISTINCT state that MergeGroup cannot combine exactly. COUNT(DISTINCT ...)
// stores its canonical arguments in the aggregate state, so BatchMerge can
// deduplicate values that appeared in different local workers. Other DISTINCT
// aggregates keep the conservative single-worker contract until their merge
// behavior has equivalent end-to-end coverage.
func RequiresSingleStageDistinctAgg(node *plan.Node) bool {
	if node == nil {
		return false
	}
	for _, expr := range node.AggList {
		if expr == nil {
			continue
		}
		agg := expr.GetF()
		if agg == nil || agg.Func == nil || uint64(agg.Func.Obj)&function.Distinct == 0 {
			continue
		}
		baseID := int64(uint64(agg.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)
		if functionID != function.COUNT {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) optimizeDistinctAgg(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		builder.optimizeDistinctAgg(childID)
	}

	if node.NodeType != plan.Node_AGG || len(node.Children) != 1 {
		return
	}
	for _, flag := range node.GroupingFlag {
		if !flag {
			return
		}
	}

	// Preserve the established single-aggregate rewrite even for small inputs.
	// Large COUNT(DISTINCT) additionally gets the forced distinct-key shuffle.
	if len(node.AggList) == 1 {
		if key, ok := singleDistinctRewriteKey(node.AggList[0]); ok {
			forceShuffle := key.GetCol() != nil &&
				isSupportedDistinctKeyShuffleType(key.Typ.Id) &&
				shouldUseDistinctKeyPath(node, builder)
			inner := builder.rewriteDistinctKeyAggregate(node, key)
			if forceShuffle && inner != nil {
				builder.markDistinctKeyShuffle(inner, int32(len(node.GroupBy)))
			}
		}
		return
	}

	key, ok := mixedDistinctRewriteKey(node)
	if !ok || !builder.localProtocolEnablesDistinctCombine() ||
		!shouldUseDistinctKeyPath(node, builder) {
		return
	}
	oldGroupLen := len(node.GroupBy)
	inner := builder.rewriteDistinctKeyAggregate(node, key)
	if inner != nil {
		builder.markDistinctKeyShuffle(inner, int32(oldGroupLen))
	}
}

func singleDistinctRewriteKey(expr *plan.Expr) (*plan.Expr, bool) {
	agg := expr.GetF()
	if agg == nil || agg.Func == nil || len(agg.Args) != 1 ||
		uint64(agg.Func.Obj)&function.Distinct == 0 ||
		agg.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE ||
		len(agg.AggConfig) != 0 || agg.Args[0].Typ.Id == int32(types.T_tuple) {
		return nil, false
	}
	baseID := int64(uint64(agg.Func.Obj) & function.DistinctMask)
	functionID, _ := function.DecodeOverloadID(baseID)
	if functionID != function.COUNT && functionID != function.SUM {
		return nil, false
	}
	return agg.Args[0], true
}

func mixedDistinctRewriteKey(node *plan.Node) (*plan.Expr, bool) {
	var key *plan.Expr
	hasDistinct := false
	for _, expr := range node.AggList {
		agg := expr.GetF()
		if agg == nil || agg.Func == nil ||
			agg.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE ||
			len(agg.AggConfig) != 0 {
			return nil, false
		}
		baseID := int64(uint64(agg.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)
		if uint64(agg.Func.Obj)&function.Distinct != 0 {
			if functionID != function.COUNT || len(agg.Args) != 1 ||
				agg.Args[0].Typ.Id == int32(types.T_tuple) {
				return nil, false
			}
			if key == nil {
				key = agg.Args[0]
			} else if !exprStructuralEqual(key, agg.Args[0]) {
				return nil, false
			}
			hasDistinct = true
			continue
		}
		switch functionID {
		case function.SUM, function.MIN, function.MAX, function.AVG:
			if len(agg.Args) != 1 {
				return nil, false
			}
		case function.COUNT:
			if len(agg.Args) == 0 {
				return nil, false
			}
		case function.STARCOUNT:
			if len(agg.Args) != 1 {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	if !hasDistinct || key == nil || key.GetCol() == nil ||
		!isSupportedDistinctKeyShuffleType(key.Typ.Id) {
		return nil, false
	}
	return key, true
}

func isSupportedDistinctKeyShuffleType(typ int32) bool {
	switch types.T(typ) {
	case types.T_int16, types.T_int32, types.T_int64,
		types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_text:
		return true
	default:
		return false
	}
}

func (builder *QueryBuilder) rewriteDistinctKeyAggregate(
	node *plan.Node,
	distinctKey *plan.Expr,
) *plan.Node {
	oldGroupBy := node.GroupBy
	oldGroupLen := len(oldGroupBy)
	newGroupTag := builder.genNewBindTag()
	newAggregateTag := builder.genNewBindTag()
	innerGroupBy := make([]*plan.Expr, 0, oldGroupLen+1)
	innerGroupBy = append(innerGroupBy, oldGroupBy...)
	innerGroupBy = append(innerGroupBy, distinctKey)
	innerAggList := make([]*plan.Expr, 0, len(node.AggList)+1)

	for _, expr := range node.AggList {
		agg := expr.GetF()
		baseID := int64(uint64(agg.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)
		if uint64(agg.Func.Obj)&function.Distinct != 0 {
			agg.Func.Obj = baseID
			agg.Args = []*plan.Expr{distinctKeyGroupCol(
				distinctKey.Typ, newGroupTag, int32(oldGroupLen))}
			continue
		}

		switch functionID {
		case function.SUM:
			pos := int32(len(innerAggList))
			innerAggList = append(innerAggList, DeepCopyExpr(expr))
			setInternalCombineAgg(agg, function.INTERNAL_SUM_COMBINE,
				internalSumCombineName, []*plan.Expr{
					distinctKeyGroupCol(expr.Typ, newAggregateTag, pos),
				})
		case function.COUNT, function.STARCOUNT:
			pos := int32(len(innerAggList))
			innerAggList = append(innerAggList, DeepCopyExpr(expr))
			setInternalCombineAgg(agg, function.INTERNAL_COUNT_COMBINE,
				internalCountCombineName, []*plan.Expr{
					distinctKeyGroupCol(expr.Typ, newAggregateTag, pos),
				})
		case function.MIN, function.MAX:
			pos := int32(len(innerAggList))
			innerAggList = append(innerAggList, DeepCopyExpr(expr))
			agg.Args = []*plan.Expr{
				distinctKeyGroupCol(expr.Typ, newAggregateTag, pos),
			}
		case function.AVG:
			arg := agg.Args[0]
			argType := types.NewWithCharset(
				types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale,
				uint8(arg.Typ.Charset))
			sumType := aggexec.SumReturnType([]types.Type{argType})
			sumPlanType := plan.Type{
				Id: int32(sumType.Oid), Width: sumType.Width,
				Scale: sumType.Scale, Charset: uint32(sumType.Charset),
			}
			sumPos := int32(len(innerAggList))
			innerAggList = append(innerAggList, distinctKeyAggExpr(
				sumPlanType, function.SUM, "sum", []*plan.Expr{DeepCopyExpr(arg)}))
			countPos := int32(len(innerAggList))
			countType := plan.Type{Id: int32(types.T_int64), NotNullable: true}
			innerAggList = append(innerAggList, distinctKeyAggExpr(
				countType, function.COUNT, "count", []*plan.Expr{DeepCopyExpr(arg)}))
			resultWitnessType := expr.Typ
			resultWitnessType.NotNullable = false
			setInternalCombineAgg(agg, function.INTERNAL_AVG_COMBINE,
				internalAvgCombineName, []*plan.Expr{
					distinctKeyGroupCol(sumPlanType, newAggregateTag, sumPos),
					distinctKeyGroupCol(countType, newAggregateTag, countPos),
					{
						Typ:  resultWitnessType,
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
					},
				})
		}
	}

	innerID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_AGG,
		Children:    []int32{node.Children[0]},
		GroupBy:     innerGroupBy,
		AggList:     innerAggList,
		BindingTags: []int32{newGroupTag, newAggregateTag},
		SpillMem:    builder.aggSpillMem,
	}, builder.ctxByNode[node.Children[0]])
	inner := builder.qry.Nodes[innerID]
	builder.determineGroupByHashKey(inner)

	node.Children[0] = innerID
	node.GroupBy = make([]*plan.Expr, oldGroupLen)
	for i := range node.GroupBy {
		node.GroupBy[i] = distinctKeyGroupCol(
			oldGroupBy[i].Typ, newGroupTag, int32(i))
	}
	return inner
}

func distinctKeyGroupCol(typ plan.Type, tag, pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: tag,
			ColPos: pos,
		}},
	}
}

func distinctKeyAggExpr(
	typ plan.Type,
	functionID int32,
	name string,
	args []*plan.Expr,
) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				Obj:     function.EncodeOverloadID(functionID, 0),
				ObjName: name,
			},
			Args: args,
		}},
	}
}

func setInternalCombineAgg(
	agg *plan.Function,
	functionID int32,
	name string,
	args []*plan.Expr,
) {
	agg.Func = &plan.ObjectRef{
		Obj:     function.EncodeOverloadID(functionID, 0),
		ObjName: name,
	}
	agg.Args = args
	agg.AggConfig = nil
	agg.AggConfigType = plan.AggregateConfigType_AGG_CONFIG_NONE
}

func (builder *QueryBuilder) markDistinctKeyShuffle(node *plan.Node, col int32) {
	if builder.distinctKeyShuffleCols == nil {
		builder.distinctKeyShuffleCols = make(map[*plan.Node]int32)
	}
	builder.distinctKeyShuffleCols[node] = col
}

func (builder *QueryBuilder) localProtocolEnablesDistinctCombine() bool {
	if builder == nil || builder.compCtx == nil {
		return false
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return ok && valid && version >= defines.MORPCVersion32
}
