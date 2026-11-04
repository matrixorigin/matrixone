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
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// Equal complete keys are equal on every identity-key subset. Hash collisions
// merely co-locate rows; the membership kernels still compare the full tuple.
// The explicit spill setting only bounds fanout here: these kernels do not
// spill, and their shared query allocation budget remains authoritative.
// Keep automatic mode serial and cap the extra exchange at eight owners.
func (c *Compile) parallelSetMembership(node *plan.Node, inputs ...[]*Scope) ([]int32, int) {
	const minOwnerHeadroom = int64(128 << 20)
	if node == nil || (node.NodeType != plan.Node_MINUS && node.NodeType != plan.Node_INTERSECT) ||
		node.Stats == nil || c.IsTpQuery() || c.isPrepare || c.ncpu <= 1 ||
		node.SpillMem < 2*minOwnerHeadroom || len(c.cnList) != 1 ||
		c.addr == "" || c.cnList[0].Addr != c.addr || len(node.ProjectList) == 0 ||
		node.Limit != nil || node.Offset != nil || len(node.OrderBy) != 0 ||
		len(node.FilterList) != 0 || node.FilterIsBarrier || nodeHasUserLevelLockFunction(node) {
		return nil, 1
	}
	dop := min(8, c.capFanoutByMaxDop(c.ncpu), int(node.Stats.Dop), int(node.SpillMem/minOwnerHeadroom))
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
	var keys []int32
	for i, visible := range node.ProjectList {
		if visible == nil || visible.GetCol() == nil || visible.GetCol().RelPos != 0 || visible.GetCol().ColPos != int32(i) {
			return nil, 1
		}
		key := visible
		if len(node.PhysicalEqualityKeyList) > 0 {
			key = node.PhysicalEqualityKeyList[i]
		}
		if key == nil || key.GetCol() == nil || key.GetCol().RelPos != 0 || key.GetCol().ColPos != int32(i) {
			continue
		}
		visibleType, keyType := visible.Typ, key.Typ
		visibleType.NotNullable, keyType.NotNullable = false, false
		if !proto.Equal(&visibleType, &keyType) {
			continue
		}
		switch types.T(key.Typ.Id) {
		case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
			types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_decimal64, types.T_decimal128, types.T_date, types.T_datetime,
			types.T_timestamp, types.T_time:
			keys = append(keys, int32(i))
		}
	}
	if len(keys) == 0 {
		return nil, 1
	}
	return keys, dop
}

func (c *Compile) compileParallelSetMembership(node *plan.Node, left, right []*Scope, keys []int32, dop int) []*Scope {
	owners := c.newScopeListWithNode(dop, 2, c.cnList[0])
	feeders := []*Scope{c.newMergeScope(left), c.newMergeScope(right)}
	first := c.anal.isFirst
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
			panic(err)
		}
		send.FuncId = dispatch.ShuffleToAllFunc
		send.ShuffleType = plan2.ShuffleToRegIndex
		send.SetAnalyzeControl(c.anal.curNodeIdx, false)
		source.setRootOperator(send)
		source.IsEnd = true
	}
	owners[0].PreScopes = append(owners[0].PreScopes, feeders...)
	for _, owner := range owners {
		owner.RootOp.(*merge.Merge).WithPartial(0, 1)
		rightMerge := merge.NewArgument().WithPartial(1, 2)
		if node.NodeType == plan.Node_INTERSECT {
			op := intersect.NewArgument()
			op.KeyExprs = node.PhysicalEqualityKeyList
			op.SetAnalyzeControl(c.anal.curNodeIdx, first)
			owner.setRootOperator(op)
			op.AppendChild(rightMerge)
		} else {
			op := minus.NewArgument()
			op.KeyExprs = node.PhysicalEqualityKeyList
			op.SetAnalyzeControl(c.anal.curNodeIdx, first)
			owner.setRootOperator(op)
			op.AppendChild(rightMerge)
		}
	}
	c.hasMergeOp = true
	c.anal.isFirst = false
	return []*Scope{c.newMergeScope(owners)}
}
