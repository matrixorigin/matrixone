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

package compile

import (
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// parallelUnionDistinct keeps a complete equality tuple at one owner without
// increasing the sum of explicit spill thresholds. The existing automatic
// minimum is a per-owner cost floor, not a grant or a hard stage-memory cap.
// Auto mode remains serial until stage-wide adaptive allocation is defined.
func (c *Compile) parallelUnionDistinct(node *plan.Node, inputs ...[]*Scope) ([]int32, int) {
	const minOwnerSpill = int64(128 << 20)
	if node == nil || node.NodeType != plan.Node_UNION || node.Stats == nil ||
		c.IsTpQuery() || c.isPrepare || c.ncpu <= 1 ||
		node.SpillMem < 2*minOwnerSpill || len(c.cnList) != 1 ||
		c.addr == "" || c.cnList[0].Addr != c.addr || len(node.ProjectList) == 0 {
		return nil, 1
	}
	dop := min(c.capFanoutByMaxDop(c.ncpu), int(node.Stats.Dop), int(node.SpillMem/minOwnerSpill))
	if dop <= 1 {
		return nil, 1
	}
	for _, scopes := range inputs {
		if len(scopes) == 0 {
			return nil, 1
		}
		for _, scope := range scopes {
			if scope == nil || scope.Proc == nil || !sameExecutionNode(scope.NodeInfo, c.cnList[0]) {
				return nil, 1
			}
		}
	}
	if len(node.PhysicalEqualityKeyList) > 0 && len(node.PhysicalEqualityKeyList) != len(node.ProjectList) {
		return nil, 1
	}
	keys := make([]int32, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		if expr == nil || expr.GetCol() == nil || expr.GetCol().RelPos != 0 || expr.GetCol().ColPos != int32(i) {
			return nil, 1
		}
		switch types.T(expr.Typ.Id) {
		case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
			types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_decimal64, types.T_decimal128, types.T_date, types.T_datetime,
			types.T_timestamp, types.T_time:
		default:
			return nil, 1
		}
		if len(node.PhysicalEqualityKeyList) > 0 {
			physical := node.PhysicalEqualityKeyList[i]
			if physical == nil || physical.GetCol() == nil ||
				physical.GetCol().RelPos != 0 || physical.GetCol().ColPos != int32(i) {
				return nil, 1
			}
			visibleType, physicalType := expr.Typ, physical.Typ
			visibleType.NotNullable, physicalType.NotNullable = false, false
			if !proto.Equal(&visibleType, &physicalType) {
				return nil, 1
			}
		}
		keys[i] = int32(i)
	}
	return keys, dop
}

// Only a mergeable aggregate may consume the disjoint complete owners without
// gathering first. Each owner still deduplicates full UNION tuples, and the
// aggregate's ordinary MergeGroup completes keys shared by different owners.
func canPreserveParallelUnionDistinctOwners(aggregate, union *plan.Node) bool {
	return aggregate != nil && aggregate.NodeType == plan.Node_AGG && len(aggregate.GroupBy) > 0 &&
		union != nil && union.NodeType == plan.Node_UNION &&
		len(union.Children) == 2 && union.Limit == nil && union.Offset == nil &&
		len(union.OrderBy) == 0 && len(union.FilterList) == 0 && !union.FilterIsBarrier &&
		aggregate.ExtraOptions == "" && !slices.Contains(aggregate.GroupingFlag, false) &&
		!nodeHasUserLevelLockFunction(union) && !nodeHasUserLevelLockFunction(aggregate)
}

func (c *Compile) compileParallelUnionDistinctOwners(node *plan.Node, left, right []*Scope, keys []int32, dop int) []*Scope {
	owners := c.newScopeListWithNode(dop, 2, c.cnList[0])
	feeders := []*Scope{c.newMergeScope(left), c.newMergeScope(right)}
	for i, source := range feeders {
		router := shuffle.NewArgument()
		router.ShuffleType = int32(plan.ShuffleType_Hash)
		router.ShuffleKeyColumns = append([]int32(nil), keys...)
		router.BucketNum = int32(dop)
		router.DrainAllBuckets = true
		router.SetAnalyzeControl(c.anal.curNodeIdx, false)
		source.setRootOperator(router)
		send, err := constructLocalDispatchFromScopes(i, owners, source)
		if err != nil {
			panic(err) // placement and receiver shape were established above
		}
		send.FuncId = dispatch.ShuffleToAllFunc
		send.ShuffleType = plan2.ShuffleToRegIndex
		send.SetAnalyzeControl(c.anal.curNodeIdx, false)
		source.setRootOperator(send)
		source.IsEnd = true
	}
	owners[0].PreScopes = append(owners[0].PreScopes, feeders...)
	for _, owner := range owners {
		op := c.constructDistinctSetGroup(node)
		op.SpillMem = node.SpillMem / int64(dop)
		op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
		owner.setRootOperator(op)
	}
	c.anal.isFirst = false
	return owners
}
