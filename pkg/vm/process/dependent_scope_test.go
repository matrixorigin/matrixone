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

package process_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDependentProcessOwnsOnlyInvocationState(t *testing.T) {
	parent := testutil.NewProcess(t)
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	require.NoError(t, parent.SetExecutionResourceBudgetForTesting(generation))
	slot, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, parent.Mp())
	require.NoError(t, err)
	defer slot.Free(parent.Mp())
	parentBoard := parent.GetMessageBoard()
	parent.SetBaseProcessRunningStatus(false)
	require.Equal(t, uint64(0), parent.NextGroupConcatInputRowBase(0, 10))
	child, err := parent.NewDependentProcess(1, []*vector.Vector{slot}, 2)
	require.NoError(t, err)
	defer child.CloseDependentProcess()
	require.Same(t, parent.Base, child.Base)
	require.NotSame(t, parentBoard, child.GetMessageBoard())
	borrowedBudget, err := child.GetExecutionResourceBudget()
	require.NoError(t, err)
	require.Same(t, generation, borrowedBudget)
	child.SetBaseProcessRunningStatus(true)
	require.False(t, parent.GetBaseProcessRunningStatus())
	require.Equal(t, uint64(0), child.NextGroupConcatInputRowBase(0, 2))
	require.Equal(t, uint64(10), parent.NextGroupConcatInputRowBase(0, 0))
	pipeline := child.NewContextChildProc(0)
	require.Equal(t, uint64(2), pipeline.NextGroupConcatInputRowBase(0, 1))
	value, err := pipeline.DependentSlot(1, 0)
	require.NoError(t, err)
	require.Same(t, slot, value)
	pipeline.Free() // Borrowing a domain never retires its owner or Base.
	_, err = child.DependentSlot(1, 0)
	require.NoError(t, err)
	child.Free()
	require.Zero(t, generation.Used())
	_, err = child.DependentSlot(1, 0)
	require.Error(t, err)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](slot, 0))
	require.Same(t, parentBoard, parent.GetMessageBoard())
	require.NoError(t, context.Cause(parent.Ctx))
	require.Equal(t, uint64(10), parent.NextGroupConcatInputRowBase(0, 0))
	next, err := parent.NewDependentProcess(1, []*vector.Vector{slot}, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(0), next.NextGroupConcatInputRowBase(0, 0))
	next.CloseDependentProcess()
	require.Zero(t, generation.Used())
}

func TestDependentProcessAdmissionAndCancellation(t *testing.T) {
	parent := testutil.NewProcess(t)
	budget := process.MustNewExecutionResourceBudget(1, 1)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	require.NoError(t, parent.SetExecutionResourceBudgetForTesting(generation))
	child, err := parent.NewDependentProcess(1, nil, 1)
	require.Error(t, err)
	require.Nil(t, child)
	require.Zero(t, generation.Used())
	parent.BuildPipelineContext(context.Background())
	parent.Cancel(context.Canceled)
	child, err = parent.NewDependentProcess(1, nil, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, child)
}

func TestDependentProtocolGenerationFence(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, t.Name(), mpool.MustNewZero())
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	previous, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
	for _, version := range []any{int64(defines.MORPCVersion118), int64(defines.MORPCVersion117), int64(defines.MORPCVersion116), int64(defines.MORPCVersion115), int64(defines.MORPCVersion114), int64(defines.MORPCVersion113), int64(defines.MORPCVersion112), int64(defines.MORPCVersion111), "119", nil} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t, proc.RequireDependentProtocol(), "protocol version 119")
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
	require.NoError(t, proc.RequireDependentProtocol())
	// A cached operator must consult the current generation's admission again.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion118)
	require.ErrorContains(t, proc.RequireDependentProtocol(), "protocol version 119")
}
