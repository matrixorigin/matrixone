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

package fill

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

// partitionedBatch builds one input batch shaped (val int64 nullable, part int64).
// nullRows lists the val rows that are NULL.
func partitionedBatch(mp *mpool.MPool, vals []int64, nullRows []uint64, parts []int64) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.SetVector(0, testutil.MakeInt64Vector(vals, nullRows, mp))
	bat.SetVector(1, testutil.MakeInt64Vector(parts, nil, mp))
	bat.SetRowCount(len(vals))
	return bat
}

// runFill drives the operator to exhaustion and returns the val column as
// (values, isNull) per row across every output batch.
func runFill(t *testing.T, proc *process.Process, fillType plan.Node_FillType, bats []*batch.Batch) (vals []int64, nulls []bool) {
	t.Helper()
	arg := &Fill{
		ColLen:          1,
		FillType:        fillType,
		PartitionColIdx: []int32{1},
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(bats))
	require.NoError(t, arg.Prepare(proc))
	defer arg.Free(proc, false, nil)

	for {
		res, err := arg.Call(proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		vec := res.Batch.Vecs[0]
		for r := 0; r < res.Batch.RowCount(); r++ {
			if vec.IsNull(uint64(r)) {
				nulls = append(nulls, true)
				vals = append(vals, 0)
			} else {
				nulls = append(nulls, false)
				vals = append(vals, vector.GetFixedAtNoTypeCheck[int64](vec, r))
			}
		}
		if res.Status == vm.ExecStop {
			break
		}
	}
	return
}

// fill(prev) must not carry the last value of one partition into the next:
// the first window of a new partition has no previous value.
func TestFillPrevStopsAtPartitionBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// batch 1: partition 1 = [10, NULL]; batch 2: partition 2 = [NULL, 30]
	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{10, 0}, []uint64{1}, []int64{1, 1}),
		partitionedBatch(proc.Mp(), []int64{0, 30}, []uint64{0}, []int64{2, 2}),
	}
	defer func() {
		for _, b := range bats {
			b.Clean(proc.Mp())
		}
	}()

	vals, nulls := runFill(t, proc, plan.Node_PREV, bats)
	require.Equal(t, []bool{false, false, true, false}, nulls,
		"partition 2's leading NULL has no previous value and must stay NULL")
	require.Equal(t, int64(10), vals[1], "within partition 1 the previous value fills")
	require.Equal(t, int64(30), vals[3])
}

// The boundary must also hold inside a single batch: the non-sliding window
// forwards rows of several partitions in one batch.
func TestFillPrevPartitionBoundaryWithinBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{10, 0, 0, 30}, []uint64{1, 2}, []int64{1, 1, 2, 2}),
	}
	defer bats[0].Clean(proc.Mp())

	vals, nulls := runFill(t, proc, plan.Node_PREV, bats)
	require.Equal(t, []bool{false, false, true, false}, nulls)
	require.Equal(t, int64(10), vals[1])
}

// fill(next) is the mirror image: the trailing NULLs of a partition have no
// next value, because the following rows belong to another group.
func TestFillNextStopsAtPartitionBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// partition 1 = [NULL, 10, NULL]; partition 2 = [20, NULL]
	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{0, 10, 0}, []uint64{0, 2}, []int64{1, 1, 1}),
		partitionedBatch(proc.Mp(), []int64{20, 0}, []uint64{1}, []int64{2, 2}),
	}
	defer func() {
		for _, b := range bats {
			b.Clean(proc.Mp())
		}
	}()

	vals, nulls := runFill(t, proc, plan.Node_NEXT, bats)
	require.Equal(t, []bool{false, false, true, false, true}, nulls,
		"trailing NULLs of each partition must stay NULL")
	require.Equal(t, int64(10), vals[0], "within partition 1 the next value fills backwards")
}

// A batch containing no NULL must not end the scan: batches after it still
// need their NULLs filled. The old streaming state machine dropped them.
func TestFillNextConsumesBatchesAfterNullFreeBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{1, 2}, nil, []int64{1, 1}),
		partitionedBatch(proc.Mp(), []int64{0, 9}, []uint64{0}, []int64{1, 1}),
	}
	defer func() {
		for _, b := range bats {
			b.Clean(proc.Mp())
		}
	}()

	vals, nulls := runFill(t, proc, plan.Node_NEXT, bats)
	require.Len(t, vals, 4, "every input row must be emitted")
	require.Equal(t, []bool{false, false, false, false}, nulls)
	require.Equal(t, int64(9), vals[2])
}

// fill(linear) needs a neighbour on both sides inside the same partition.
// decimal128 exercises the built-in midpoint path, which needs no expression
// executor.
func TestFillLinearStopsAtPartitionBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// One batch, two partitions: [10, NULL, 30, NULL | NULL, 100].
	// The NULL at the tail of partition 1 has no following value in its
	// partition, and partition 2's leading NULL has no previous one: without
	// the boundary, linear would interpolate 30 and 100 across the groups.
	typ := types.New(types.T_decimal128, 38, 0)
	valVec := vector.NewVec(typ)
	set := func(v int64, isNull bool) {
		require.NoError(t, vector.AppendFixed(valVec, types.Decimal128FromInt64(v), isNull, proc.Mp()))
	}
	set(10, false)
	set(0, true)
	set(30, false)
	set(0, true)
	set(0, true)
	set(100, false)

	bat := batch.NewWithSize(2)
	bat.SetVector(0, valVec)
	bat.SetVector(1, testutil.MakeInt64Vector([]int64{1, 1, 1, 1, 2, 2}, nil, proc.Mp()))
	bat.SetRowCount(6)
	defer bat.Clean(proc.Mp())

	arg := &Fill{ColLen: 1, FillType: plan.Node_LINEAR, PartitionColIdx: []int32{1}}
	ctr := &arg.ctr
	ctr.bats = []*batch.Batch{bat}
	ctr.linRun = make([][]fillCoord, 1)
	ctr.linPre = []fillCoord{{seq: -1, row: -1}}

	require.NoError(t, ctr.consumeLinear(arg, bat, 0, proc))

	require.False(t, valVec.IsNull(1))
	require.Equal(t, types.Decimal128FromInt64(20),
		vector.GetFixedAtNoTypeCheck[types.Decimal128](valVec, 1), "midpoint of 10 and 30")
	require.True(t, valVec.IsNull(3), "partition 1's trailing NULL must stay NULL")
	require.True(t, valVec.IsNull(4), "partition 2's leading NULL must stay NULL")
	require.False(t, valVec.IsNull(5))
}
