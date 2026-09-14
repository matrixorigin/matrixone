// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func preparedSetOperation(node *plan.Node) bool {
	return isPreparedSetOperationNode(node.NodeType)
}

// Synthetic aggregate columns address GroupBy/AggList, not the child batch.
func preparedAggregateOutput(node *plan.Node, colPos int32) *plan.Expr {
	if node.NodeType != plan.Node_AGG || colPos < 0 || int(colPos) >= len(node.ProjectList) {
		return nil
	}
	col := node.ProjectList[colPos].GetCol()
	if col == nil {
		return nil
	}
	switch col.RelPos {
	case -1:
		if col.ColPos >= 0 && int(col.ColPos) < len(node.GroupBy) {
			return node.GroupBy[col.ColPos]
		}
	case -2:
		pos := col.ColPos - int32(len(node.GroupBy))
		if pos >= 0 && int(pos) < len(node.AggList) {
			return node.AggList[pos]
		}
	}
	return nil
}

func markPreparedOutputSource(expr *plan.Expr, nodeID, colPos int32, positions map[int32]struct{}) {
	m := ensurePreparedNumericMetadata(expr)
	m.Fallback = true
	m.FallbackSource = true
	m.FallbackSourceNodeId = nodeID
	m.FallbackSourceColPos = colPos
	m.ParamPos = minimumPreparedPosition(positions)
}
