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

package table_scan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type emptyMembershipReader struct {
	lateMaterializationTestReader
	t *testing.T
}

func (r *emptyMembershipReader) ReadWithFilter(_ context.Context, _ []string, _ []int, filter *engine.ReaderFilterRequest, mp *mpool.MPool, output *batch.Batch) (bool, error) {
	r.reads++
	if r.reads == 2 {
		require.Nil(r.t, filter.Membership, "empty output must not delay optional-stage retirement")
		require.Equal(r.t, []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnUnused}, filter.ColumnUses)
		return true, nil
	}
	require.NotNil(r.t, filter.Membership)
	require.Equal(r.t, []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnUnused}, filter.Membership.ColumnUses)
	require.NoError(r.t, vector.AppendFixed(output.Vecs[0], int32(1), false, mp))
	require.NoError(r.t, vector.AppendFixed(output.Vecs[1], int32(0), false, mp))
	output.SetRowCount(1)
	scratch, err := filter.MembershipScratch(2)
	require.NoError(r.t, err)
	require.NoError(r.t, scratch.Physical.PreExtend(1, mp))
	_, err = filter.Membership.Select(output, []int{0})
	require.NoError(r.t, err)
	selection, err := filter.Select(output, []int{1})
	require.NoError(r.t, err)
	require.False(r.t, selection.All)
	require.Empty(r.t, selection.Sels)
	output.CleanOnlyData()
	return false, nil
}

func TestMembershipRetiresBeforeEmptyBatchContinue(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetMessageBoard(message.NewMessageBoard())
	t.Cleanup(proc.Free)
	budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(generation.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	domain, _ := runtimefilter.NewLocalDomain(types.T_int32, 1000, 1, 1, 1000000)
	t.Cleanup(domain.Close)
	source, err := domain.Begin(proc, 0, 1)
	require.NoError(t, err)
	keys := testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	t.Cleanup(func() { keys.Free(proc.Mp()) })
	require.NoError(t, source.AddVector(proc.Ctx, keys, 1))
	source.Complete(false)
	reader := &emptyMembershipReader{t: t}
	scan := NewArgument()
	typ := types.T_int32.ToType()
	scan.Reader, scan.Attrs = reader, []string{"key", "predicate"}
	scan.Types = []plan.Type{typ.PlanType(), typ.PlanType()}
	scan.FilterExprs = []*plan.Expr{lateTestCompare(t, ">", 1, 0)}
	scan.ProjectList = []*plan.Expr{lateTestCol(0, typ)}
	scan.OutputAllocation = allocation
	scan.LocalDomainProbes = []runtimefilter.LocalDomainBinding{{Domain: domain, Column: "key", ProbeNDV: 1000000}}
	scan.SetAnalyzeControl(0, true)
	consumer := &ownedColumnUseConsumer{MockOperator: colexec.NewMockOperator(), owned: true}
	consumer.AppendChild(scan)
	vm.ConfigureDictionaryBoundaries(consumer)
	t.Cleanup(func() { scan.Free(proc, false, nil); scan.Release() })
	require.NoError(t, scan.Prepare(proc))
	scan.ctr.domainProbes[0].tested = runtimefilter.LocalDomainBatchRows - 1
	_, err = scan.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 2, reader.reads)
	require.True(t, scan.ctr.domainProbes[0].pass)
	require.Nil(t, scan.ctr.readerFilter.Membership)
}
