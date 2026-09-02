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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partitionhash"
)

const (
	// windowHashPartitionAutoEnabled is deliberately fail-closed until the
	// real-Window acceptance matrix has established the resource and latency
	// contract for the blocking HASH implementation. SORT remains the wire-zero
	// value and the only planner-selected algorithm meanwhile.
	windowHashPartitionAutoEnabled = false
	windowHashEntryOverhead        = 32
	windowVarlenKeyWidth           = 128
	// Each hash group crosses the Window boundary and maintains one equality
	// state per key. Scale this conservative cost by key count so a near-unique
	// composite key cannot look cheaper than the local sort just because the
	// sort comparator also has more keys.
	windowHashGroupWork = 32
)

// determineWindowPartitionAlgorithms makes the planner's final physical
// choice after statistics and access paths have settled. Only a PARTITION
// directly owned by a Window is eligible; other PARTITION uses retain SORT.
func (builder *QueryBuilder) determineWindowPartitionAlgorithms(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.determineWindowPartitionAlgorithms(childID)
	}
	if !windowHashPartitionAutoEnabled || node.NodeType != planpb.Node_WINDOW || len(node.Children) != 1 {
		return
	}
	selectWindowHashPartition(builder, node)
}

// selectWindowHashPartition applies the admission contract after the caller has
// established that experimental automatic selection is enabled. Keeping the
// contract separate lets its fail-closed boundaries be verified without
// changing the feature gate.
func selectWindowHashPartition(builder *QueryBuilder, node *planpb.Node) bool {
	if node.NodeType != planpb.Node_WINDOW || len(node.Children) != 1 {
		return false
	}
	partitionNode := builder.qry.Nodes[node.Children[0]]
	if partitionNode.NodeType != planpb.Node_PARTITION || partitionNode.Limit != nil ||
		len(partitionNode.Children) != 1 || len(partitionNode.OrderBy) == 0 {
		return false
	}

	child := builder.qry.Nodes[partitionNode.Children[0]]
	if child.Stats == nil || !finitePositiveWindowStat(child.Stats.Outcnt) {
		return false
	}
	n := child.Stats.Outcnt
	if n < float64(colexec.DefaultBatchSize) {
		return false
	}

	keyWidth := 0
	groupCount := 1.0
	for _, spec := range partitionNode.OrderBy {
		if spec == nil || spec.Expr == nil || spec.Expr.GetCol() == nil ||
			!partitionhash.Compatible(types.T(spec.Expr.Typ.Id)) {
			return false
		}
		width, ok := windowPartitionKeyWidth(spec.Expr)
		if !ok || keyWidth > math.MaxInt-width {
			return false
		}
		keyWidth += width

		ndv := getExprNdv(spec.Expr, builder)
		if !finitePositiveWindowStat(ndv) {
			return false
		}
		if groupCount > n/ndv {
			groupCount = n
		} else {
			groupCount *= ndv
			if groupCount > n {
				groupCount = n
			}
		}
	}

	threshold := builder.aggSpillMem
	resolvedThreshold := colexec.ResolveSpillThreshold(threshold)
	if shouldUseWindowHashPartition(n, groupCount, keyWidth, len(partitionNode.OrderBy), threshold, resolvedThreshold) {
		partitionNode.PartitionAlgorithm = planpb.Node_PARTITION_ALGORITHM_HASH
		partitionNode.SpillMem = threshold
		return true
	}
	return false
}

func finitePositiveWindowStat(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func windowPartitionKeyWidth(expr *planpb.Expr) (int, bool) {
	typ := types.T(expr.Typ.Id)
	width := typ.FixedLength()
	if width < 0 {
		width = windowVarlenKeyWidth
		if expr.Typ.Width > 0 {
			width = int(expr.Typ.Width)
		}
	}
	if width <= 0 {
		return 0, false
	}
	if !expr.Typ.NotNullable {
		width++
	}
	return width, true
}

func shouldUseWindowHashPartition(
	n, groupCount float64,
	keyWidth, keyCount int,
	configuredThreshold, resolvedThreshold int64,
) bool {
	if !finitePositiveWindowStat(n) || !finitePositiveWindowStat(groupCount) || groupCount > n ||
		keyWidth <= 0 || keyCount <= 0 || resolvedThreshold <= 0 {
		return false
	}
	if n < float64(colexec.DefaultBatchSize) {
		return false
	}
	if configuredThreshold > 0 && configuredThreshold <= 100000 && n >= float64(configuredThreshold) {
		return false
	}
	sortWork := n * math.Log2(math.Max(n, 2)) * float64(keyCount)
	// Each group crosses the downstream Window boundary and carries equality
	// state for every partition key. This is material for near-unique composite
	// keys even when the hash lookup itself is cheap.
	hashWork := n*float64(keyCount) + n + windowHashGroupWork*groupCount*float64(keyCount)
	if hashWork >= sortWork {
		return false
	}
	hashAux := 16*n + groupCount*float64(keyWidth+windowHashEntryOverhead)
	return !math.IsInf(hashAux, 0) && !math.IsNaN(hashAux) && hashAux <= float64(resolvedThreshold)
}
