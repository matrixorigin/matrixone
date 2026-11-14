// Copyright 2021 Matrix Origin
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

package shuffle

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

type dictionaryRoutingSource struct {
	*colexec.MockOperator
	state      vm.DictionaryOutputState
	allocation *vector.AllocationAccountSelection
}

func (s *dictionaryRoutingSource) DictionaryOutput() (bool, *vector.AllocationAccountSelection) {
	return true, s.allocation
}
func (s *dictionaryRoutingSource) DictionaryOutputState() *vm.DictionaryOutputState { return &s.state }
func (s *dictionaryRoutingSource) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return s.state.Normalize(input, proc.Mp())
}

type dictionaryRoutingSink struct{ *colexec.MockOperator }

func (*dictionaryRoutingSink) AcceptsDictionary(child int) bool { return child == 0 }

func TestDictionaryOrdinaryShufflePartitionsAndAccounting(t *testing.T) {
	for _, tc := range []struct {
		mode    plan.ShuffleType
		async   bool
		forward bool
		name    string
	}{
		{plan.ShuffleType_Hash, false, false, "hash"}, {plan.ShuffleType_Range, false, false, "range"}, {plan.ShuffleType_Hash, true, false, "async-hash"}, {plan.ShuffleType_Range, true, false, "async-range"}, {plan.ShuffleType_Range, false, true, "forward"}, {plan.ShuffleType_Range, true, true, "async-forward"},
	} {
		mode := tc.mode
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(8 << 20)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Snapshot().Used)
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
			selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
			require.NoError(t, err)
			source, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), selection)
			require.NoError(t, err)
			t.Cleanup(func() { source.Free(proc.Mp()) })
			require.NoError(t, vector.AppendFixedList(source, []int64{0, 1, 2, 3}, []bool{false, false, true, false}, proc.Mp()))
			input := batch.NewOffHeapWithSize(1)
			require.NoError(t, input.SetAllocationAccount(selection))
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			retained, err := vector.TrySelectDictionaryVectors([]*vector.Vector{source}, input.Vecs, []int64{3, 2, 0, 3}, 4, proc.Mp(), selection)
			require.NoError(t, err)
			require.True(t, retained)
			input.SetRowCount(4)
			child := &dictionaryRoutingSource{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}), allocation: selection}
			arg := NewArgument()
			arg.BucketNum = 4
			arg.ShuffleType = int32(mode)
			arg.ShuffleColMin = 0
			arg.ShuffleColMax = 4
			arg.DrainAllBuckets = true
			if tc.forward {
				arg.BucketNum = 1
			}
			if tc.async {
				arg.BucketNum = 1
				arg.DrainAllBuckets = false
				arg.SetShufflePool(NewShufflePool(1, 1, false))
			}
			arg.AppendChild(child)
			t.Cleanup(func() {
				vm.Reset(arg, proc, false, nil)
				vm.Free(arg, proc, false, nil)
				child.ResetBatchs()
				vm.Free(child, proc, false, nil)
				arg.Release()
			})
			require.NoError(t, vm.Prepare(arg, proc))
			if tc.forward {
				sink := &dictionaryRoutingSink{colexec.NewMockOperator()}
				sink.AppendChild(arg)
				vm.ConfigureDictionaryBoundaries(sink)
				require.True(t, arg.DictionaryOutputState().AllowsDictionaryOutput())
			}
			require.Equal(t, tc.forward, arg.AcceptsDictionary(0))
			require.Equal(t, tc.forward, child.state.AllowsDictionaryOutput())
			possible, _ := arg.DictionaryOutput()
			require.True(t, possible, "capability must not depend on current admission")
			flatSink := colexec.NewMockOperator()
			flatSink.AppendChild(arg)
			vm.ConfigureDictionaryBoundaries(flatSink)
			require.False(t, arg.AcceptsDictionary(0))
			require.False(t, child.state.AllowsDictionaryOutput())
			if tc.forward {
				sink := &dictionaryRoutingSink{colexec.NewMockOperator()}
				sink.AppendChild(arg)
				vm.ConfigureDictionaryBoundaries(sink)
				require.True(t, arg.AcceptsDictionary(0), "reconfiguration must not get stuck flat")
				require.True(t, child.state.AllowsDictionaryOutput())
			}
			var values []int64
			nulls := 0
			for {
				result, err := vm.Exec(arg, proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				if result.Batch.IsEmpty() {
					continue
				} // Synchronous EOF progress is an empty control batch.
				require.Equal(t, tc.forward, result.Batch.HasDictionary())
				if tc.forward {
					require.Same(t, input, result.Batch)
				}
				for row := 0; row < result.Batch.RowCount(); row++ {
					require.True(t, vector.AllocationAccountSelectionsEqual(selection, result.Batch.Vecs[0].AllocationAccountSelection()))
					if result.Batch.Vecs[0].IsNull(uint64(row)) {
						nulls++
						require.Zero(t, result.Batch.ShuffleIDX)
						continue
					}
					value := vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], row)
					bucket := plan2.SimpleInt64HashToRange(uint64(value), uint64(arg.BucketNum))
					if mode == plan.ShuffleType_Range {
						bucket = plan2.GetRangeShuffleIndexSignedMinMax(0, 4, value, uint64(arg.BucketNum))
					}
					require.Equal(t, int32(bucket), result.Batch.ShuffleIDX)
					values = append(values, value)
				}
			}
			require.ElementsMatch(t, []int64{3, 0, 3}, values)
			require.Equal(t, 1, nulls)
			require.True(t, input.HasDictionary())
		})
	}
}

func TestDictionarySortedRangeUsesEndpointsWithoutEnumerating(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		t.Run(map[bool]string{false: "endpoint-fast-path", true: "null-control"}[nullable], func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			source := vector.NewVec(types.T_int64.ToType())
			t.Cleanup(func() { source.Free(proc.Mp()) })
			require.NoError(t, vector.AppendFixedList(source, []int64{1, 2, 3}, []bool{false, nullable, false}, proc.Mp()))
			source.SetSorted(true)
			base, err := vector.FreezeVector(source, proc.Mp(), nil)
			require.NoError(t, err)
			t.Cleanup(base.Release)
			rows, err := vector.NewRowSelection([]int64{0, 1, 1, 2}, 3, proc.Mp(), nil)
			require.NoError(t, err)
			t.Cleanup(rows.Release)
			selected, err := vector.NewDictionaryVector(base, rows, proc.Mp(), nil)
			require.NoError(t, err)
			input := batch.NewWithSize(1)
			input.Vecs[0] = selected
			input.SetRowCount(4)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			arg := NewArgument()
			t.Cleanup(arg.Release)
			arg.BucketNum = 4
			arg.ShuffleType = int32(plan.ShuffleType_Range)
			arg.ShuffleColMin = 0
			arg.ShuffleColMax = 100
			arg.DrainAllBuckets = true
			arg.ctr.sels = make([][]int32, 4)
			result, err := rangeShuffle(arg, input, proc)
			require.NoError(t, err)
			require.Same(t, input, result)
			require.Zero(t, result.ShuffleIDX)
			total := 0
			for _, sel := range arg.ctr.sels {
				total += len(sel)
			}
			if nullable {
				require.Equal(t, 4, total)
			} else {
				require.Zero(t, total, "sorted same-bucket input must not build per-row routing selections")
			}
		})
	}
}
