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
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// applyVectorIndexForSortContext 共用原有插件入口，并保留显式的 applied 结果；
// 不能靠节点 ID 是否改变判断成功，因为插件也会原位替换 PROJECT 的孩子。
func (builder *QueryBuilder) applyVectorIndexForSortContext(
	nodeID int32, vecCtx *vectorSortContext, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr,
) (int32, bool, error) {
	indexes, err := builder.collectVectorIndexes(vecCtx.scanNode)
	if err != nil || len(indexes) == 0 {
		return nodeID, false, err
	}
	// 在插件可能移除 TABLE_SCAN 之前记录完整 prepared 索引依赖。
	if err := builder.recordPreparedPluginDependencies(vecCtx.scanNode); err != nil {
		return nodeID, false, err
	}
	opts := planplugin.ApplyForSortOpts{ColRefCnt: colRefCnt, IdxColMap: idxColMap}
	for _, index := range indexes {
		if !indexplugin.IsVectorIndexAlgo(index.IndexAlgo) || !vectorIndexSupportsContext(vecCtx, index.IndexAlgo) {
			continue
		}
		plugin, ok := indexplugin.Get(index.IndexAlgo)
		if !ok {
			continue
		}
		vctx, mti := toPlanplugin(vecCtx, index)
		newID, applied, err := plugin.Plan().ApplyForSort(builder, vctx, mti, nodeID, opts)
		if applied || err != nil {
			return newID, applied, err
		}
	}
	return nodeID, false, nil
}

func vectorCandidateModeSupported(option *plan.RankOption) bool {
	return option == nil || option.Mode == "" || option.Mode == "pre"
}

// 4.2 的 AUTO 会重放整条语句。只允许本区域无副作用，且其它区域不可能
// 触发 AUTO 的查询；普通候选明确使用 PRE，不改会话默认值或原计划的模式。
func (builder *QueryBuilder) vectorCandidateRegionSafe(root int32) bool {
	region := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(id int32) bool {
		if region[id] {
			return true
		}
		region[id] = true
		n := builder.qry.Nodes[id]
		if n.NodeType != plan.Node_PROJECT && n.NodeType != plan.Node_SORT &&
			n.NodeType != plan.Node_JOIN && n.NodeType != plan.Node_TABLE_SCAN {
			return false
		}
		if !vectorCandidateModeSupported(n.RankOption) {
			return false
		}
		for _, exprs := range [][]*plan.Expr{n.ProjectList, n.FilterList, n.OnList, {n.Limit, n.Offset}} {
			for _, expr := range exprs {
				if vectorCandidateExprUnsafe(expr) {
					return false
				}
			}
		}
		for _, order := range n.OrderBy {
			if vectorCandidateExprUnsafe(order.Expr) {
				return false
			}
		}
		for _, child := range n.Children {
			if !visit(child) {
				return false
			}
		}
		return true
	}
	if !visit(root) {
		return false
	}
	auto, _ := builder.compCtx.ResolveVariable("enable_vector_auto_mode_by_default", true, false)
	for _, node := range builder.qry.Nodes {
		if node.RankOption != nil && node.RankOption.Mode == "auto" {
			return false
		}
		if !region[node.NodeId] && auto == int8(1) && node.NodeType == plan.Node_PROJECT &&
			(node.RankOption == nil || node.RankOption.Mode == "") {
			ctx := builder.buildVectorSortContext(node)
			if ctx == nil {
				ctx = builder.buildVectorSortContextThroughJoin(node)
			}
			if ctx != nil && (ctx.rankOption == nil || ctx.rankOption.Mode == "") {
				return false
			}
		}
	}
	return true
}

func vectorCandidateExprUnsafe(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if e.F.Func == nil || function.GetFunctionIsVolatileOrRealTimeRelatedByName(e.F.Func.ObjName) {
			return true
		}
		for _, arg := range e.F.Args {
			if vectorCandidateExprUnsafe(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if vectorCandidateExprUnsafe(item) {
				return true
			}
		}
	case *plan.Expr_Sub, *plan.Expr_V:
		return true
	}
	return false
}
