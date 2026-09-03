// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (builder *QueryBuilder) generateScalarPredicateRuntimeFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.generateScalarPredicateRuntimeFilters(childID)
	}
	if node.NodeType == plan.Node_FILTER {
		builder.generateScalarPredicateRuntimeFilter(node)
	}
}

// generateScalarPredicateRuntimeFilter accelerates an uncorrelated scalar
// equality without moving its SINGLE join.  The original FILTER + SINGLE pair
// remains at its logical position and therefore retains the established scope
// of the scalar cardinality error.  HashBuild publishes an exact one-value
// filter only after observing exactly one scalar row; zero rows publish DROP
// and multiple rows publish PASS.
func (builder *QueryBuilder) generateScalarPredicateRuntimeFilter(filter *plan.Node) {
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return
	}
	version, _ := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.MOProtocolVersion)
	protocolVersion, ok := version.(int64)
	if !ok || protocolVersion < defines.MORPCVersion43 {
		return
	}
	if filter == nil || filter.IsEnd || filter.FilterIsBarrier ||
		filter.RollupFilter || filter.Limit != nil || filter.Offset != nil ||
		len(filter.Children) != 1 || len(filter.FilterList) == 0 ||
		!areTruncationSafePredicates(filter.FilterList) {
		return
	}

	single := builder.qry.Nodes[filter.Children[0]]
	if single == nil || single.NodeType != plan.Node_JOIN ||
		single.JoinType != plan.Node_SINGLE || single.IsRightJoin ||
		len(single.Children) != 2 || len(single.OnList) != 0 ||
		len(single.RuntimeFilterBuildList) != 0 ||
		single.Stats == nil || single.Stats.HashmapStats == nil ||
		single.Stats.HashmapStats.Shuffle {
		return
	}

	for _, predicate := range filter.FilterList {
		probeOutput, scalarOutput, ok := scalarRuntimeFilterEquality(
			predicate, single)
		if !ok {
			continue
		}

		probeNodeID, probeExpr, ok := builder.scalarRuntimeFilterScanColumn(
			single.Children[0], probeOutput, make(map[[2]int32]bool))
		if !ok {
			continue
		}
		probeCol := probeExpr.GetCol()
		probeNode := builder.qry.Nodes[probeNodeID]
		if probeNode == nil || probeNode.NodeType != plan.Node_TABLE_SCAN ||
			probeNode.Limit != nil || probeNode.Offset != nil ||
			probeCol.ColPos < 0 || probeNode.TableDef == nil ||
			int(probeCol.ColPos) >= len(probeNode.TableDef.Cols) {
			continue
		}

		scalarRoot := builder.qry.Nodes[single.Children[1]]
		if scalarOutput < 0 || int(scalarOutput) >= len(scalarRoot.ProjectList) {
			continue
		}
		buildExpr := GetColExpr(
			scalarRoot.ProjectList[scalarOutput].Typ, -1, scalarOutput)
		rfTag := builder.genNewMsgTag()
		probeSpec, buildSpec, ok := builder.makeExactRuntimeFilterPair(
			rfTag, false, 1, probeExpr, buildExpr,
			probeNode.TableDef.Cols[probeCol.ColPos].Name !=
				probeNode.TableDef.Pkey.GetPkeyColName())
		if !ok || buildSpec.KeyEncoding !=
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1 {
			continue
		}
		buildSpec.ScalarPredicate = true

		probeNode.RuntimeFilterProbeList = append(
			probeNode.RuntimeFilterProbeList, probeSpec)
		single.RuntimeFilterBuildList = append(
			single.RuntimeFilterBuildList, buildSpec)
		return
	}
}

func scalarRuntimeFilterEquality(
	expr *plan.Expr,
	single *plan.Node,
) (probeOutput, scalarOutput int32, ok bool) {
	fn := expr.GetF()
	if single == nil || fn == nil || fn.Func == nil ||
		!IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 {
		return 0, 0, false
	}
	leftCol, rightCol := fn.Args[0].GetCol(), fn.Args[1].GetCol()
	if leftCol == nil || rightCol == nil ||
		leftCol.ColPos < 0 || rightCol.ColPos < 0 ||
		int(leftCol.ColPos) >= len(single.ProjectList) ||
		int(rightCol.ColPos) >= len(single.ProjectList) {
		return 0, 0, false
	}
	leftInput := single.ProjectList[leftCol.ColPos].GetCol()
	rightInput := single.ProjectList[rightCol.ColPos].GetCol()
	if leftInput == nil || rightInput == nil {
		return 0, 0, false
	}
	if leftInput.RelPos == 0 && rightInput.RelPos == 1 {
		return leftInput.ColPos, rightInput.ColPos, true
	}
	if rightInput.RelPos == 0 && leftInput.RelPos == 1 {
		return rightInput.ColPos, leftInput.ColPos, true
	}
	return 0, 0, false
}

func (builder *QueryBuilder) scalarRuntimeFilterScanColumn(
	nodeID, outputPos int32,
	visited map[[2]int32]bool,
) (int32, *plan.Expr, bool) {
	key := [2]int32{nodeID, outputPos}
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) ||
		outputPos < 0 || visited[key] {
		return 0, nil, false
	}
	visited[key] = true
	node := builder.qry.Nodes[nodeID]
	if node == nil || int(outputPos) >= len(node.ProjectList) {
		return 0, nil, false
	}
	projectExpr := node.ProjectList[outputPos]
	col := projectExpr.GetCol()
	if col == nil || col.ColPos < 0 {
		return 0, nil, false
	}
	if node.NodeType == plan.Node_TABLE_SCAN {
		if !areTruncationSafePredicates(node.FilterList) {
			return 0, nil, false
		}
		return nodeID, DeepCopyExpr(projectExpr), true
	}
	// Match the old logical rewrite's legality boundary, but leave the SINGLE
	// itself in place.  Crossing an aggregate, window, projection, outer join,
	// or another SINGLE can change which rows, errors, or volatile expressions
	// are observed.  Runtime filters must follow physical probe input 0. If we
	// filtered build input 1, an empty build could short-circuit the unchecked
	// probe subtree and suppress an error or volatile evaluation there. A
	// current-CN scalar message also cannot follow a probe column across a
	// shuffle boundary.
	if node.NodeType != plan.Node_JOIN || node.Limit != nil || node.Offset != nil ||
		len(node.Children) != 2 ||
		node.Stats == nil || node.Stats.HashmapStats == nil ||
		node.Stats.HashmapStats.Shuffle ||
		!areTruncationSafePredicates(node.OnList) ||
		!areTruncationSafePredicates(node.FilterList) {
		return 0, nil, false
	}
	if col.RelPos < 0 || int(col.RelPos) >= len(node.Children) {
		return 0, nil, false
	}
	switch node.JoinType {
	case plan.Node_INNER:
		if col.RelPos != 0 {
			return 0, nil, false
		}
	case plan.Node_SEMI, plan.Node_ANTI:
		// IsRightJoin means physical input 0 is the build input even though the
		// logical output still comes from the preserved side.
		if node.IsRightJoin || col.RelPos != 0 {
			return 0, nil, false
		}
	default:
		return 0, nil, false
	}
	return builder.scalarRuntimeFilterScanColumn(
		node.Children[col.RelPos], col.ColPos, visited)
}
