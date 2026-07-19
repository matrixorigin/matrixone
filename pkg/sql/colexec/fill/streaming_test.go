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

// countingChild records how many times the operator was pulled, so a test can
// assert that fill emitted a result before draining the whole child.
type countingChild struct {
	*colexec.MockOperator
	calls int
}

var _ vm.Operator = (*countingChild)(nil)

func (c *countingChild) Call(proc *process.Process) (vm.CallResult, error) {
	c.calls++
	return c.MockOperator.Call(proc)
}

type cell struct {
	val  int64
	null bool
}

func readCol(bat *batch.Batch, col int) []cell {
	vec := bat.Vecs[col]
	out := make([]cell, 0, bat.RowCount())
	for r := 0; r < bat.RowCount(); r++ {
		if vec.IsNull(uint64(r)) {
			out = append(out, cell{null: true})
		} else {
			out = append(out, cell{val: vector.GetFixedAtNoTypeCheck[int64](vec, r)})
		}
	}
	return out
}

// drainCol keeps calling the operator and concatenates column col of every
// emitted batch. Each batch stays valid until the next Call frees it, so it is
// read before advancing.
func drainCol(t *testing.T, arg *Fill, proc *process.Process, col int) []cell {
	t.Helper()
	var out []cell
	for {
		res, err := arg.Call(proc)
		require.NoError(t, err)
		if res.Batch == nil {
			return out
		}
		out = append(out, readCol(res.Batch, col)...)
	}
}

// A no-NULL NEXT stream must flow straight through: the first batch is emitted
// after a single child pull, never after materializing the whole input. This
// is the regression the reviewer asked for — an outer LIMIT 1 could stop the
// child right here.
func TestFillNextStreamsWithoutBuffering(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const n = 5
	bats := make([]*batch.Batch, 0, n)
	for i := 0; i < n; i++ {
		bats = append(bats, partitionedBatch(proc.Mp(),
			[]int64{int64(2 * i), int64(2*i + 1)}, nil, []int64{1, 1}))
	}

	child := &countingChild{MockOperator: colexec.NewMockOperator().WithBatchs(bats)}
	arg := &Fill{ColLen: 1, FillType: plan.Node_NEXT, PartitionColIdx: []int32{1}}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	res, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 1, child.calls, "first batch must be emitted after exactly one child pull")
	require.LessOrEqual(t, len(arg.ctr.bats), 1, "no-NULL NEXT must not buffer the stream")

	all := append(readCol(res.Batch, 0), drainCol(t, arg, proc, 0)...)
	require.Len(t, all, 2*n)
	for i, c := range all {
		require.False(t, c.null)
		require.Equal(t, int64(i), c.val)
	}
	require.Equal(t, n+1, child.calls, "the child is drained exactly once end to end")

	arg.Free(proc, false, nil)
	for _, b := range bats {
		b.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// LINEAR keeps at most one batch buffered while streaming a no-NULL input: it
// pins the last non-NULL as a possible left endpoint, so it emits batch k only
// after batch k+1 arrives — bounded, not the whole input.
func TestFillLinearStreamsBounded(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const n = 5
	bats := make([]*batch.Batch, 0, n)
	for i := 0; i < n; i++ {
		bats = append(bats, partitionedBatch(proc.Mp(),
			[]int64{int64(2 * i), int64(2*i + 1)}, nil, []int64{1, 1}))
	}

	child := &countingChild{MockOperator: colexec.NewMockOperator().WithBatchs(bats)}
	arg := &Fill{ColLen: 1, FillType: plan.Node_LINEAR, PartitionColIdx: []int32{1}}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	res, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 2, child.calls, "LINEAR emits its first batch after a one-batch look-ahead")
	require.LessOrEqual(t, len(arg.ctr.bats), 1, "LINEAR buffers at most one batch on a no-NULL stream")

	all := append(readCol(res.Batch, 0), drainCol(t, arg, proc, 0)...)
	require.Len(t, all, 2*n)
	for i, c := range all {
		require.False(t, c.null)
		require.Equal(t, int64(i), c.val)
	}

	arg.Free(proc, false, nil)
	for _, b := range bats {
		b.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// A NULL that spans several batches is filled by the value that finally arrives
// downstream, no matter how far away it is.
func TestFillNextLongCrossBatchGap(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{10}, nil, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{40}, nil, []int64{1}),
	}

	vals, nulls := runFill(t, proc, plan.Node_NEXT, bats)
	require.Equal(t, []bool{false, false, false, false}, nulls)
	require.Equal(t, []int64{10, 40, 40, 40}, vals)

	for _, b := range bats {
		b.Clean(proc.Mp())
	}
	proc.Free()
}

func TestFillNextLongGapSpillsPendingSuffix(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const nullBatches = 64
	bats := make([]*batch.Batch, 0, nullBatches+2)
	bats = append(bats, partitionedBatch(proc.Mp(), []int64{10}, nil, []int64{1}))
	for i := 0; i < nullBatches; i++ {
		bats = append(bats, partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}))
	}
	bats = append(bats, partitionedBatch(proc.Mp(), []int64{90}, nil, []int64{1}))

	child := &countingChild{MockOperator: colexec.NewMockOperator().WithBatchs(bats)}
	arg := &Fill{ColLen: 1, FillType: plan.Node_NEXT, PartitionColIdx: []int32{1}, SpillThreshold: 2}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	first, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []cell{{val: 10}}, readCol(first.Batch, 0))
	require.Equal(t, 1, child.calls, "resolved prefix must still stream before spill")

	second, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, second.Batch)
	require.NotNil(t, arg.ctr.spill)
	require.Equal(t, arg.ctr.spill.inputRecords, arg.ctr.spill.outputRecords)
	require.Empty(t, arg.ctr.bats)
	require.Zero(t, arg.ctr.pendingBytes)
	require.Zero(t, arg.ctr.pendingRows)

	all := append(readCol(second.Batch, 0), drainCol(t, arg, proc, 0)...)
	require.Len(t, all, nullBatches+1)
	for _, value := range all {
		require.Equal(t, cell{val: 90}, value)
	}

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// Trailing NULLs with no following value in the stream stay NULL at EOF rather
// than being dropped or hanging the operator.
func TestFillNextEOFTail(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{10}, nil, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{0, 0}, []uint64{0, 1}, []int64{1, 1}),
	}

	vals, nulls := runFill(t, proc, plan.Node_NEXT, bats)
	require.Equal(t, []bool{false, true, true}, nulls)
	require.Equal(t, int64(10), vals[0])

	for _, b := range bats {
		b.Clean(proc.Mp())
	}
	proc.Free()
}

func TestFillNextSpillKeepsEOFTailNull(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{10}, nil, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
	}
	child := colexec.NewMockOperator().WithBatchs(bats)
	arg := &Fill{ColLen: 1, FillType: plan.Node_NEXT, PartitionColIdx: []int32{1}, SpillThreshold: 2}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	got := drainCol(t, arg, proc, 0)
	require.Equal(t, []cell{{val: 10}, {null: true}, {null: true}}, got)

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// Two fill columns resolve independently: a row leaves the operator only once
// every filled column has a value, and each column's gap is closed by its own
// next value.
func TestFillNextMultiColumnGap(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// col0: [1, NULL, 3]; col1: [NULL, 20, 30]; part all 1.
	bat := batch.NewWithSize(3)
	bat.SetVector(0, testutil.MakeInt64Vector([]int64{1, 0, 3}, []uint64{1}, proc.Mp()))
	bat.SetVector(1, testutil.MakeInt64Vector([]int64{0, 20, 30}, []uint64{0}, proc.Mp()))
	bat.SetVector(2, testutil.MakeInt64Vector([]int64{1, 1, 1}, nil, proc.Mp()))
	bat.SetRowCount(3)

	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg := &Fill{ColLen: 2, FillType: plan.Node_NEXT, PartitionColIdx: []int32{2}}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	var col0, col1 []cell
	for {
		res, err := arg.Call(proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		col0 = append(col0, readCol(res.Batch, 0)...)
		col1 = append(col1, readCol(res.Batch, 1)...)
	}
	require.Equal(t, []cell{{val: 1}, {val: 3}, {val: 3}}, col0, "col0 gap filled by its own next value")
	require.Equal(t, []cell{{val: 20}, {val: 20}, {val: 30}}, col1, "col1 gap filled independently")

	arg.Free(proc, false, nil)
	bat.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestFillNextSpillMultiColumnAndPartitionBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	makeBatch := func(col0, col1 []int64, null0, null1 []uint64, parts []int64) *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.SetVector(0, testutil.MakeInt64Vector(col0, null0, proc.Mp()))
		bat.SetVector(1, testutil.MakeInt64Vector(col1, null1, proc.Mp()))
		bat.SetVector(2, testutil.MakeInt64Vector(parts, nil, proc.Mp()))
		bat.SetRowCount(len(parts))
		return bat
	}
	bats := []*batch.Batch{
		makeBatch([]int64{1}, []int64{0}, nil, []uint64{0}, []int64{1}),
		makeBatch([]int64{0}, []int64{20}, []uint64{0}, nil, []int64{1}),
		makeBatch([]int64{3}, []int64{30}, nil, nil, []int64{1}),
		// Partition 1's candidates must not fill partition 2's leading NULLs.
		makeBatch([]int64{0}, []int64{0}, []uint64{0}, []uint64{0}, []int64{2}),
		makeBatch([]int64{8}, []int64{80}, nil, nil, []int64{2}),
	}
	child := colexec.NewMockOperator().WithBatchs(bats)
	arg := &Fill{ColLen: 2, FillType: plan.Node_NEXT, PartitionColIdx: []int32{2}, SpillThreshold: 2}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	var col0, col1 []cell
	for {
		res, err := arg.Call(proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		col0 = append(col0, readCol(res.Batch, 0)...)
		col1 = append(col1, readCol(res.Batch, 1)...)
	}
	require.Equal(t, []cell{{val: 1}, {val: 3}, {val: 3}, {val: 8}, {val: 8}}, col0)
	require.Equal(t, []cell{{val: 20}, {val: 20}, {val: 30}, {val: 80}, {val: 80}}, col1)

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// LINEAR interpolates across a batch boundary: the midpoint uses the non-NULL
// values that bracket the gap even when they land in different batches.
func TestFillLinearCrossBatchGap(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	typ := types.New(types.T_decimal128, 38, 0)
	set := func(vec *vector.Vector, v int64, isNull bool) {
		require.NoError(t, vector.AppendFixed(vec, types.Decimal128FromInt64(v), isNull, proc.Mp()))
	}
	// batch 1: [10, NULL]; batch 2: [NULL, 40]; one partition, so the midpoint
	// of 10 and 40 (=25) fills both gaps.
	v1 := vector.NewVec(typ)
	set(v1, 10, false)
	set(v1, 0, true)
	b1 := batch.NewWithSize(2)
	b1.SetVector(0, v1)
	b1.SetVector(1, testutil.MakeInt64Vector([]int64{1, 1}, nil, proc.Mp()))
	b1.SetRowCount(2)

	v2 := vector.NewVec(typ)
	set(v2, 0, true)
	set(v2, 40, false)
	b2 := batch.NewWithSize(2)
	b2.SetVector(0, v2)
	b2.SetVector(1, testutil.MakeInt64Vector([]int64{1, 1}, nil, proc.Mp()))
	b2.SetRowCount(2)

	bats := []*batch.Batch{b1, b2}
	child := colexec.NewMockOperator().WithBatchs(bats)
	arg := &Fill{ColLen: 1, FillType: plan.Node_LINEAR, PartitionColIdx: []int32{1}}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	var got []types.Decimal128
	var isNull []bool
	for {
		res, err := arg.Call(proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		vec := res.Batch.Vecs[0]
		for r := 0; r < res.Batch.RowCount(); r++ {
			isNull = append(isNull, vec.IsNull(uint64(r)))
			got = append(got, vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, r))
		}
	}
	require.Equal(t, []bool{false, false, false, false}, isNull)
	require.Equal(t, types.Decimal128FromInt64(25), got[1], "midpoint of 10 and 40")
	require.Equal(t, types.Decimal128FromInt64(25), got[2])

	arg.Free(proc, false, nil)
	for _, b := range bats {
		b.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestFillLinearLongGapSpillsPendingSuffix(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const nullBatches = 64
	typ := types.New(types.T_decimal128, 38, 0)
	makeBatch := func(value int64, isNull bool) *batch.Batch {
		vec := vector.NewVec(typ)
		require.NoError(t, vector.AppendFixed(vec, types.Decimal128FromInt64(value), isNull, proc.Mp()))
		bat := batch.NewWithSize(2)
		bat.SetVector(0, vec)
		bat.SetVector(1, testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()))
		bat.SetRowCount(1)
		return bat
	}
	bats := make([]*batch.Batch, 0, nullBatches+2)
	bats = append(bats, makeBatch(10, false))
	for i := 0; i < nullBatches; i++ {
		bats = append(bats, makeBatch(0, true))
	}
	bats = append(bats, makeBatch(90, false))

	child := &countingChild{MockOperator: colexec.NewMockOperator().WithBatchs(bats)}
	arg := &Fill{
		ColLen:          1,
		FillType:        plan.Node_LINEAR,
		PartitionColIdx: []int32{1},
		SpillThreshold:  2,
	}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	first, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, first.Batch)
	require.NotNil(t, arg.ctr.spill)
	require.Empty(t, arg.ctr.bats)
	require.Zero(t, arg.ctr.pendingBytes)
	require.Zero(t, arg.ctr.pendingRows)

	readDecimal := func(bat *batch.Batch) []types.Decimal128 {
		return append([]types.Decimal128(nil), vector.MustFixedColNoTypeCheck[types.Decimal128](bat.Vecs[0])...)
	}
	all := readDecimal(first.Batch)
	for {
		res, callErr := arg.Call(proc)
		require.NoError(t, callErr)
		if res.Batch == nil {
			break
		}
		all = append(all, readDecimal(res.Batch)...)
	}
	require.Len(t, all, nullBatches+2)
	require.Equal(t, types.Decimal128FromInt64(10), all[0])
	for _, value := range all[1 : len(all)-1] {
		require.Equal(t, types.Decimal128FromInt64(50), value)
	}
	require.Equal(t, types.Decimal128FromInt64(90), all[len(all)-1])

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestFillSpillResetReleasesState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bats := []*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{90}, nil, []int64{1}),
	}
	arg := &Fill{ColLen: 1, FillType: plan.Node_NEXT, PartitionColIdx: []int32{1}, SpillThreshold: 2}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(bats))
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.NotNil(t, arg.ctr.spill)

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.spill)
	require.Empty(t, arg.ctr.bats)
	require.Zero(t, arg.ctr.pendingBytes)
	require.Zero(t, arg.ctr.pendingRows)

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
