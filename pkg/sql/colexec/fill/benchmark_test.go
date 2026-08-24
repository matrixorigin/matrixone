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

package fill

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func BenchmarkFillNextResident(b *testing.B) {
	benchmarkFillNextResident(b, nil)
}

func BenchmarkFillNextResidentAccounted(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	proc.Base.Lim.Size = 64 << 20
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(b, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(b, err)
	account, err := registry.OpenWithController(64<<20, generation)
	require.NoError(b, err)
	benchmarkFillNextResidentWithProcess(b, proc, account)
	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(b, err)
	require.True(b, first)
	require.Zero(b, snapshot.Used)
	proc.Free()
	require.Zero(b, proc.Mp().CurrNB())
}

func benchmarkFillNextResident(
	b *testing.B,
	account *mpool.AllocationAccount,
) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	benchmarkFillNextResidentWithProcess(b, proc, account)
	require.Zero(b, proc.Mp().CurrNB())
}

func benchmarkFillNextResidentWithProcess(
	b *testing.B,
	proc *process.Process,
	account *mpool.AllocationAccount,
) {
	values := make([]int64, 8192)
	parts := make([]int64, len(values))
	for i := range values {
		values[i] = int64(i + 1)
		parts[i] = 1
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		arg := &Fill{
			ColLen:          1,
			FillType:        plan.Node_NEXT,
			PartitionColIdx: []int32{1},
		}
		if account != nil {
			require.NoError(b, arg.SetAllocationAccount(account))
		}
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			partitionedBatch(proc.Mp(), values, nil, parts),
			partitionedBatch(proc.Mp(), values, nil, parts),
		})
		arg.AppendChild(child)
		b.StartTimer()
		require.NoError(b, arg.Prepare(proc))
		for {
			result, err := vm.Exec(arg, proc)
			require.NoError(b, err)
			if result.Status == vm.ExecStop {
				break
			}
		}
		b.StopTimer()
		child.Free(proc, false, nil)
		arg.Free(proc, false, nil)
		if account != nil {
			require.NoError(b, arg.ClearAllocationAccount(account))
		}
		require.Zero(b, proc.Mp().CurrNB())
		b.StartTimer()
	}
}
