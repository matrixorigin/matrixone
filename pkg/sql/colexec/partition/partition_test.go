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

package partition

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// add unit tests for cases
type partitionTestCase struct {
	arg  *Partition
	proc *process.Process
}

func makeTestCases(t *testing.T) []partitionTestCase {
	return []partitionTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Partition{
				OrderBySpecs: []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int32), Flag: 0}},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestPartition(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		tc.arg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestPartitionOutputHonorsCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &Partition{
		OrderBySpecs: []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int32)}},
	}
	require.NoError(t, arg.Prepare(proc))

	bat := colexec.MakeMockPartitionBatchs(1, proc.Mp())
	arg.ctr.batchList = append(arg.ctr.batchList, bat)
	require.NoError(t, arg.ctr.evaluateOrderColumn(proc, 0))
	arg.ctr.indexList = []int64{0}
	arg.ctr.status = eval

	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()

	_, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)

	arg.Free(proc, true, err)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

type partitionComparator = compare.Compare

type countedPartitionCompare struct {
	partitionComparator
	calls int
}

func (c *countedPartitionCompare) Set(i int, vec *vector.Vector) {
	c.calls++
	c.partitionComparator.Set(i, vec)
}

func TestPartitionGroupSearchDoesNotRevisitEarlierHeads(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &Partition{OrderBySpecs: []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int32)}}}
	t.Cleanup(func() {
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	require.NoError(t, arg.Prepare(proc))
	for i := 0; i < 2; i++ {
		arg.ctr.batchList = append(arg.ctr.batchList,
			makeHashPartitionBatch(t, proc, []int32{2}, nil, []int64{20 + int64(i)}))
	}
	arg.ctr.batchList = append(arg.ctr.batchList, makeHashPartitionBatch(t, proc,
		[]int32{1, 1, 1, 1, 1, 1, 1, 1, 3}, nil,
		[]int64{0, 1, 2, 3, 4, 5, 6, 7, 30}))
	for i := range arg.ctr.batchList {
		require.NoError(t, arg.ctr.evaluateOrderColumn(proc, i))
	}
	arg.ctr.indexList = make([]int64, len(arg.ctr.batchList))
	counter := &countedPartitionCompare{partitionComparator: arg.ctr.compares[0]}
	arg.ctr.compares[0] = counter
	result := vm.NewCallResult()
	done, err := arg.ctr.pickAndSend(proc, &result)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7},
		vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
	// Fixed initialization/final checks plus linear work within this group.
	// Earlier heads remain 2 throughout: rechecking both for every 1 is waste.
	require.LessOrEqual(t, counter.calls, 4*result.Batch.RowCount()+4*len(arg.ctr.batchList))
}

func TestPartitionGroupSearchPreservesGroupsAndReset(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flag  plan.OrderBySpec_OrderByFlag
		keys  [][]int32
		nulls [][]bool
	}{
		{name: "ascending", keys: [][]int32{{2, 2, 4}, {2, 3}, {1, 1, 1, 2, 4}}},
		{
			name: "descending_nulls_last", flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST,
			keys:  [][]int32{{2, 2, 0}, {2, 1}, {3, 3, 3, 2, 0}},
			nulls: [][]bool{{false, false, true}, nil, {false, false, false, false, true}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			arg := &Partition{OrderBySpecs: []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int32), Flag: tc.flag}}}
			t.Cleanup(func() {
				arg.Reset(proc, false, nil)
				arg.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			for run := 0; run < 2; run++ {
				require.NoError(t, arg.Prepare(proc))
				payloads := [][]int64{{0, 1, 2}, {3, 4}, {5, 6, 7, 8, 9}}
				for i, keys := range tc.keys {
					var ns []bool
					if tc.nulls != nil {
						ns = tc.nulls[i]
					}
					arg.ctr.batchList = append(arg.ctr.batchList, makeHashPartitionBatch(t, proc, keys, ns, payloads[i]))
					require.NoError(t, arg.ctr.evaluateOrderColumn(proc, i))
				}
				arg.ctr.indexList = make([]int64, len(arg.ctr.batchList))
				var groups [][]int64
				for len(arg.ctr.batchList) != 0 {
					result := vm.NewCallResult()
					_, err := arg.ctr.pickAndSend(proc, &result)
					require.NoError(t, err)
					groups = append(groups, append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])...))
				}
				require.Equal(t, [][]int64{{5, 6, 7}, {0, 1, 3, 8}, {4}, {2, 9}}, groups)
				arg.Reset(proc, false, nil)
			}
		})
	}
}

func newExpression(pos int32, typeID types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: plan.Type{
			Id: int32(typeID),
		},
	}
}

func resetChildren(arg *Partition, m *mpool.MPool) {
	bat1 := colexec.MakeMockPartitionBatchs(1, m)
	bat2 := colexec.MakeMockPartitionBatchs(2, m)
	bat3 := colexec.MakeMockPartitionBatchs(3, m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat1, bat2, bat3})
	arg.Children = nil
	arg.AppendChild(op)
}
