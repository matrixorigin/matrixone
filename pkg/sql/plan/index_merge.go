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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// 只拉平连续 OR，不做 DNF 展开；限制规划工作和执行计划的分支数量。
const regularIndexMergeMaxBranches = 8

type regularIndexMergeBranch struct {
	index *IndexDef
	scan  *plan.Node
	match *regularIndexOnlyMatch
	rows  float64
	work  float64
}

type regularIndexMergeCandidate struct {
	branches []*regularIndexMergeBranch
	filter   int
	covering bool
	work     float64
}

func regularIndexMergeArms(expr *plan.Expr, arms *[]*plan.Expr) bool {
	if expr == nil || len(*arms) >= regularIndexMergeMaxBranches {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "or" {
		if len(fn.Args) < 2 {
			return false
		}
		for _, arg := range fn.Args {
			if !regularIndexMergeArms(arg, arms) {
				return false
			}
		}
		return true
	}
	*arms = append(*arms, expr)
	return true
}

// 表达式正规化会生成多参数 OR，而既有总性证明只处理二元布尔表达式。
// 逐个证明正规 OR 的分支；其他表达式仍遵守原有求值域/固定输入检查。
func regularIndexMergeFiltersStable(node *plan.Node) bool {
	var stable func(*plan.Expr) bool
	stable = func(expr *plan.Expr) bool {
		fn := expr.GetF()
		if fn != nil && fn.Func != nil && fn.Func.ObjName == "or" {
			id, _ := function.DecodeOverloadID(fn.Func.Obj)
			if id != function.OR || types.T(expr.Typ.Id) != types.T_bool || len(fn.Args) < 2 {
				return false
			}
			for _, arg := range fn.Args {
				if !stable(arg) {
					return false
				}
			}
			return true
		}
		proof := *node
		proof.FilterList = []*plan.Expr{expr}
		return regularIndexFiltersHaveStableEvaluation(&proof)
	}
	for _, filter := range node.FilterList {
		if !stable(filter) {
			return false
		}
	}
	return true
}

// 普通访问已失败后才枚举 OR 候选，不改变已有单索引访问的优先级。
func (builder *QueryBuilder) applyRegularIndexMerge(
	node *plan.Node, indexes []*IndexDef, refs map[[2]int32]int,
	colMap map[[2]int32]*plan.Expr, base *encodedRegularIndexCostContext,
) (int32, error) {
	if len(indexes) < 2 || !base.valid || IsDefaultStats(node.Stats) ||
		!regularIndexMergeFiltersStable(node) {
		return node.NodeId, regularIndexPlanningError(builder.compCtx.GetContext(), nil)
	}
	pk, ok := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	if !ok || pk < 0 || int(pk) >= len(node.TableDef.Cols) {
		return node.NodeId, regularIndexPlanningError(builder.compCtx.GetContext(), nil)
	}
	var best *regularIndexMergeCandidate
	for pos, filter := range node.FilterList {
		fn := filter.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "or" {
			continue
		}
		var arms []*plan.Expr
		if !regularIndexMergeArms(filter, &arms) || len(arms) < 2 {
			continue
		}
		candidate := &regularIndexMergeCandidate{filter: pos, covering: true}
		for col, count := range refs {
			if col[0] == base.relPos && col[1] != pk && col[1] >= 0 && int(col[1]) < len(base.filterRefs) &&
				count > base.filterRefs[col[1]] {
				candidate.covering = false
			}
		}
		pkMap := map[[2]int32]*plan.Expr{{base.relPos, pk}: GetColExpr(node.TableDef.Cols[pk].Typ, base.relPos, pk)}
		for i, residual := range node.FilterList {
			if i != pos && !exprUsesOnlyMappedCols(residual, pkMap) {
				candidate.covering = false
			}
		}
		rows := 0.0
		usedIndexes := make(map[string]struct{}, len(arms))
		for _, arm := range arms {
			branch := builder.matchRegularIndexMergeBranch(node, arm, indexes, base.columnWidths[pk])
			if branch == nil {
				break
			}
			candidate.branches = append(candidate.branches, branch)
			usedIndexes[branch.index.IndexName] = struct{}{}
			rows += branch.rows
			candidate.work += branch.work
		}
		if len(candidate.branches) != len(arms) || len(usedIndexes) < 2 {
			continue
		}
		// 估计预算不是硬内存上界；执行期仍由现有去重算子的内存/溢写策略管理。
		limit := float64(GetInFilterCardLimitOnPK(builder.compCtx.GetProcess().GetService(), node.Stats.TableCnt))
		if catalog.IsFakePkName(node.TableDef.Pkey.PkeyColName) {
			limit = min(limit, float64(InFilterCardLimitNonPK))
		}
		if rows > limit {
			continue
		}
		pkWidth := base.columnWidths[pk]
		// 每个分支的 PK 读取、传输、哈希及合并输出都计费，不假设分支互斥。
		candidate.work += rows * (4*pkWidth + 2*regularIndexPredicateRowWork)
		if !candidate.covering {
			inputRows := regularIndexBackfillInputRows(base.baseRows, rows)
			probeRows := inputRows
			sortedPK := GetSortOrder(node.TableDef, pk) == 0
			if !sortedPK || builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
				probeRows = base.baseRows
			}
			candidate.work += probeRows*pkWidth + inputRows*max(0, base.baseWidth-pkWidth) +
				rows*(float64(len(node.FilterList))*regularIndexPredicateRowWork+base.outputWidth) + regularIndexJoinFixedWork
			candidate.work += regularIndexBackfillPrepareWork(rows, base.serialWidths[pk], base.localScanReaders())
			if !sortedPK {
				candidate.work += probeRows * regularIndexInSearchComparisons(rows) * regularIndexPredicateRowWork
			}
		}
		if !finitePositive(candidate.work) || !base.force && candidate.work >= base.baseWork {
			continue
		}
		if best == nil || candidate.work < best.work {
			best = candidate
		}
	}
	if best == nil {
		return node.NodeId, regularIndexPlanningError(builder.compCtx.GetContext(), nil)
	}
	return builder.buildRegularIndexMerge(node, pk, best, colMap)
}

func (builder *QueryBuilder) matchRegularIndexMergeBranch(
	node *plan.Node, arm *plan.Expr, indexes []*IndexDef, pkWidth float64,
) *regularIndexMergeBranch {
	// 克隆表达式：估算和普通索引匹配可能正规化参数，不能修改原 OR。
	scan := *node
	scan.FilterList = splitPlanConjunction(DeepCopyExpr(arm))
	scan.Limit, scan.Offset = nil, nil
	scan.Stats = DeepCopyStats(node.Stats)
	statsInfo := builder.getStatsInfoByTableID(node.TableDef.TblId)
	if statsInfo == nil || statsInfo.GetStats() == nil || !finitePositive(statsInfo.GetStats().TableCnt) {
		return nil
	}
	refs := make(map[[2]int32]int)
	for _, filter := range scan.FilterList {
		estimateExprSelectivity(filter, builder, statsInfo.GetStats())
		increaseRefCnt(filter, 1, refs)
	}
	scan.Stats.Selectivity = estimateAndSelectivity(scan.FilterList, builder, statsInfo.GetStats())
	scan.Stats.Outcnt = scan.Stats.TableCnt * scan.Stats.Selectivity
	scan.Stats.Cost = scan.Stats.TableCnt
	cost := builder.newEncodedRegularIndexCostContext(&scan, refs)
	var best *regularIndexMergeBranch
	for _, index := range indexes {
		// 分支必须能在索引侧精确验证自己的全部条件，不能丢掉另一分支独有的行。
		for _, match := range builder.matchRegularIndexOnlyScans(index, &scan, cost) {
			if match == nil {
				continue
			}
			work, _, valid := cost.score(index, match.filterIdx, match.residualLeadingPos, encodedRegularIndexCostIndexOnly)
			if !valid {
				continue
			}
			selectivity, spans := 1.0, 1.0
			known := true
			for _, pos := range match.filterIdx {
				known = known && regularIndexMergeFilterStatsKnown(scan.FilterList[pos], node.TableDef, statsInfo.GetStats())
				selectivity *= scan.FilterList[pos].Selectivity
				spans = max(spans, regularIndexMergeSpans(scan.FilterList[pos]))
			}
			if !cost.force && !known {
				continue
			}
			rows := max(scan.Stats.Outcnt, scan.Stats.TableCnt*selectivity)
			// 每个连续键段可能跨两个边界块；IN/同列 OR 不能冒充单个连续段。
			blockRows := min(scan.Stats.TableCnt, (math.Ceil(rows/float64(objectio.BlockMaxRows))+spans)*float64(objectio.BlockMaxRows))
			width := pkWidth
			for _, part := range index.Parts {
				col := node.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
				if len(index.Parts) == 1 {
					width += cost.columnWidths[col]
				} else {
					width += cost.serialWidths[col]
				}
			}
			if builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
				blockRows = scan.Stats.TableCnt
			}
			work += max(0, blockRows-rows) * width
			if !finitePositive(work) || !finitePositive(rows) {
				continue
			}
			if best == nil || work < best.work {
				best = &regularIndexMergeBranch{index: index, scan: &scan, match: match, rows: rows, work: work}
			}
		}
	}
	return best
}

// 通用估算器会给缺失列统计赋予默认选择率；该默认值不能证明新增多路扫描更便宜。
func regularIndexMergeFilterStatsKnown(expr *plan.Expr, table *plan.TableDef, stats *statspb.StatsInfo) bool {
	fn := expr.GetF()
	col := regularIndexBoundColumn(fn)
	if col == nil || col.ColPos < 0 || int(col.ColPos) >= len(table.Cols) {
		return false
	}
	name := table.Cols[col.ColPos].Name
	switch fn.Func.ObjName {
	case "=", "in":
		return finitePositive(stats.NdvMap[name])
	case "or":
		for _, arm := range fn.Args {
			if !regularIndexMergeFilterStatsKnown(arm, table, stats) {
				return false
			}
		}
		return true
	case "<", "<=", ">", ">=", "between", "in_range":
		_, typed := stats.DataTypeMap[name]
		lo, lower := stats.MinValMap[name]
		hi, upper := stats.MaxValMap[name]
		// 通用范围估算器用 0.1 回退未知/越域估算，不能把它当成多路访问收益证明。
		// 即使真实选择率恰好为 10%，这里也保守回退；不改变其他访问路径的估算。
		return typed && lower && upper && !math.IsNaN(lo) && !math.IsNaN(hi) && !math.IsInf(lo, 0) && !math.IsInf(hi, 0) &&
			lo < hi && expr.Selectivity != regularIndexUnknownRangeSel
	default:
		return false
	}
}

func regularIndexMergeSpans(expr *plan.Expr) float64 {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return 1
	}
	switch fn.Func.ObjName {
	case "in":
		if len(fn.Args) == 2 {
			if vec := fn.Args[1].GetVec(); vec != nil {
				return max(1, float64(vec.Len))
			}
			if list := fn.Args[1].GetList(); list != nil {
				return max(1, float64(len(list.List)))
			}
		}
	case "or":
		spans := 0.0
		for _, arg := range fn.Args {
			spans += regularIndexMergeSpans(arg)
		}
		return max(1, spans)
	}
	return 1
}

func (builder *QueryBuilder) buildRegularIndexMerge(
	node *plan.Node, pk int32, candidate *regularIndexMergeCandidate, colMap map[[2]int32]*plan.Expr,
) (int32, error) {
	snapshot := node.ScanSnapshot
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	// 构造所有扫描成功后才发布节点和列映射；元数据错误沿用普通索引的错误传播。
	scans := make([]*plan.Node, 0, len(candidate.branches))
	for _, branch := range candidate.branches {
		localMap := make(map[[2]int32]*plan.Expr)
		scan, err := builder.buildRegularIndexOnlyScan(branch.index, branch.scan, localMap, snapshot, branch.match)
		if err != nil {
			return node.NodeId, regularIndexPlanningError(builder.compCtx.GetContext(), err)
		}
		if scan == nil {
			return node.NodeId, regularIndexPlanningError(builder.compCtx.GetContext(), nil)
		}
		scans = append(scans, scan)
	}
	if err := regularIndexPlanningError(builder.compCtx.GetContext(), nil); err != nil {
		return node.NodeId, err
	}
	ctx := builder.ctxByNode[node.NodeId]
	pkType := node.TableDef.Cols[pk].Typ
	root, tag := int32(-1), int32(-1)
	for i, scan := range scans {
		builder.addNameByColRef(scan.BindingTags[0], scan.TableDef)
		scanID := builder.appendNode(scan, ctx)
		builder.inheritIndexHints(scanID, node.NodeId)
		projectTag := builder.genNewBindTag()
		project := builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{scanID}, BindingTags: []int32{projectTag},
			ProjectList: []*plan.Expr{GetColExpr(pkType, scan.BindingTags[0], 1)},
		}, ctx)
		if root == -1 {
			root, tag = project, projectTag
			continue
		}
		unionTag := builder.genNewBindTag()
		kind := plan.Node_UNION_ALL
		if i == len(scans)-1 {
			kind = plan.Node_UNION
		}
		root = builder.appendNode(&plan.Node{
			NodeType: kind, Children: []int32{root, project}, BindingTags: []int32{unionTag},
			ProjectList: []*plan.Expr{GetColExpr(pkType, tag, 0)},
		}, ctx)
		tag = unionTag
	}
	pkExpr := GetColExpr(pkType, tag, 0)
	if candidate.covering {
		mapping := map[[2]int32]*plan.Expr{{node.BindingTags[0], pk}: pkExpr}
		var residuals []*plan.Expr
		for i, filter := range node.FilterList {
			if i != candidate.filter {
				residuals = append(residuals, replaceColumnsForExpr(DeepCopyExpr(filter), mapping))
			}
		}
		if len(residuals) > 0 {
			root = builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{root}, FilterList: residuals}, ctx)
		}
		builder.qry.Nodes[root].Limit, builder.qry.Nodes[root].Offset = node.Limit, node.Offset
		colMap[[2]int32{node.BindingTags[0], pk}] = pkExpr
	} else {
		cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			GetColExpr(pkType, node.BindingTags[0], pk), pkExpr,
		})
		if err != nil {
			return node.NodeId, regularIndexPlanningError(builder.compCtx.GetContext(), err)
		}
		root = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN, JoinType: plan.Node_INDEX, Children: []int32{node.NodeId, root},
			OnList: []*plan.Expr{cond}, Limit: node.Limit, Offset: node.Offset,
		}, ctx)
		node.Limit, node.Offset = nil, nil
	}
	return root, nil
}
