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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/updateunique"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestUpdateUniqueStagesStayOnCoordinator(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 2},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 2},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	inputs := []*Scope{newRemoteMergeInputForTest(c, nodes[1], 0)}
	node := &plan.Node{UpdateUniqueCtx: &plan.UpdateUniqueCtx{
		RowId: 0, Active: 1, Keys: []plan.UpdateUniqueKey{{Original: 2, Current: 3, Proposed: 4, SnapshotHit: 5}},
	}}
	result, err := c.compileUpdateUnique(node, inputs)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, c.currentCNWorker().Addr, result[0].NodeInfo.Addr)
	require.Equal(t, 1, result[0].NodeInfo.Mcpu)
	op, ok := result[0].RootOp.(*updateunique.UpdateUnique)
	require.True(t, ok)
	require.Equal(t, []updateunique.Key{{Original: 2, Current: 3, Proposed: 4, SnapshotHit: 5}}, op.Keys)
}

func TestUpdateUniqueWithoutKeysReusesOnlyLocalMergeInput(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 2},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 2},
	}
	for _, remote := range []bool{false, true} {
		c := newCompileForShuffleJoinTest(t, nodes)
		var input *Scope
		if remote {
			input = newRemoteMergeInputForTest(c, nodes[1], 0)
		} else {
			input = c.newEmptyMergeScope()
			input.Proc = c.proc.NewNoContextChildProc(0)
		}
		result, err := c.compileUpdateUnique(&plan.Node{UpdateUniqueCtx: &plan.UpdateUniqueCtx{}}, []*Scope{input})
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, c.currentCNWorker().Addr, result[0].NodeInfo.Addr)
		_, ok := result[0].RootOp.(*updateunique.UpdateUnique)
		require.True(t, ok)
		if remote {
			require.NotSame(t, input, result[0])
		} else {
			require.Same(t, input, result[0])
		}
	}
}
