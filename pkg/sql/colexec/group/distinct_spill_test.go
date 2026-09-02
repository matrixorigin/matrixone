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

package group

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestDistinctSpillRecordEnvelopeRejectsCorruption(t *testing.T) {
	mp := mpool.MustNewZero()
	ctr := &container{mp: mp}
	controller, err := newDistinctSpillController(ctr)
	require.NoError(t, err)
	defer controller.close()
	groups := batch.NewWithSize(0)
	groups.SetRowCount(1)

	var encoded bytes.Buffer
	_, err = controller.writeRecord(
		&encoded, 17, 11, 0, groups, 0, []byte("key"))
	require.NoError(t, err)
	valid := bytes.Clone(encoded.Bytes())
	_, err = controller.writeRecord(
		shortGroupSpillWriter{}, 17, 11, 0, groups, 0, []byte("key"))
	require.ErrorIs(t, err, io.ErrShortWrite)

	hash, groupHash, aggregate, payload, eof, err := controller.readRecord(
		bytes.NewReader(valid), groups)
	require.NoError(t, err)
	require.False(t, eof)
	require.Equal(t, uint64(17), hash)
	require.Equal(t, uint64(11), groupHash)
	require.Zero(t, aggregate)
	require.Equal(t, []byte("key"), payload)

	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "bad header",
			mutate: func(value []byte) []byte {
				value[0] ^= 0xff
				return value
			},
		},
		{
			name: "bad version",
			mutate: func(value []byte) []byte {
				value[8] = 0xff
				return value
			},
		},
		{
			name: "truncated trailer",
			mutate: func(value []byte) []byte {
				return value[:len(value)-1]
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := test.mutate(bytes.Clone(valid))
			_, _, _, _, _, err := controller.readRecord(
				bytes.NewReader(corrupt), groups)
			require.Error(t, err)
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestH0DistinctNoSpillKeepsNormalPath(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2}, nil, proc.Mp())
	input.SetRowCount(3)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{
		countDistinctAgg(0),
	})
	g.SpillMem = 64 << 20
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{2},
		vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
	require.Nil(t, g.ctr.distinctSpill)
	require.Zero(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillActivations"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestDistinctSpillCancellationCleansPublishedOwnership(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 2, 3}, nil, proc.Mp())
	input.SetRowCount(3)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{
		countDistinctAgg(0),
	})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	_, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)

	base := proc.Ctx
	ctx, cancel := context.WithCancel(base)
	proc.Ctx = ctx
	cancel()
	err = g.ctr.finalizeH0ExactCountDistinct(proc)
	require.ErrorIs(t, err, context.Canceled)
	proc.Ctx = base
	g.Free(proc, true, err)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestDistinctSpillDrainPublishesBeforeResidentRelease(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2, 3, 3}, nil, proc.Mp())
	input.SetRowCount(5)

	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{
		countDistinctAgg(0),
	})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	require.False(t, g.ctr.distinctSpill != nil)

	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.False(t, needSpill)
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)
	require.NotNil(t, g.ctr.distinctSpill)
	require.Equal(t, uint64(3), g.ctr.distinctSpill.keys)
	require.Positive(t, g.ctr.distinctSpill.bytes)

	var rootKeys int64
	for _, bucket := range g.ctr.distinctSpill.root {
		rootKeys += bucket.cnt
	}
	require.Equal(t, int64(3), rootKeys)

	result, err := g.ctr.aggList[0].Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{0},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(g.ctr.mp)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestH0CountDistinctCompletesThroughBoundedSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2, 3, 3}, nil, proc.Mp())
	input.SetRowCount(5)

	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{
		countDistinctAgg(0),
	})
	g.SpillMem = 2
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Len(t, result.Batch.Vecs, 1)
	require.Equal(t, []int64{3},
		vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
	require.Nil(t, g.ctr.distinctSpill)
	require.True(t, g.ctr.distinctFinalized)
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillKeys"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestH0CountDistinctForcedCollisionUsesExternalSort(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	values := make([]int32, 0, 150)
	for value := int32(0); value < 100; value++ {
		values = append(values, value)
		if value%2 == 0 {
			values = append(values, value)
		}
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(len(values))

	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{
		countDistinctAgg(0),
	})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.False(t, needSpill)

	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	controller.hashForUT = func(uint64, int, []byte) uint64 { return 0 }
	g.ctr.distinctSpill = controller
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)
	controller.forceExternalSortForUT = true
	controller.sortArenaBytesForUT = 2 * 1024
	require.NoError(t, g.ctr.finalizeH0ExactCountDistinct(proc))
	require.Positive(t, controller.externalSorts)

	result, err := g.ctr.aggList[0].Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{100},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(g.ctr.mp)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestH0CountDistinctRecursivelyRepartitionsOversizedLeaf(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	values := make([]int32, 100)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(len(values))

	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{
		countDistinctAgg(0),
	})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.False(t, needSpill)

	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	controller.hashForUT = func(_ uint64, _ int, payload []byte) uint64 {
		// Every key uses root bucket zero while the next five bits retain a
		// normal distribution, forcing one measurable recursive split.
		return xxhash.Sum64(payload) << distinctSpillMaskBits
	}
	controller.sortArenaBytesForUT = 4 * 1024
	g.ctr.distinctSpill = controller
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)
	require.NoError(t, g.ctr.finalizeH0ExactCountDistinct(proc))
	require.Positive(t, controller.repartitions)
	require.Zero(t, controller.externalSorts)

	result, err := g.ctr.aggList[0].Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{100},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(g.ctr.mp)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupedHotKeyCountDistinctCompletesThroughKeySpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2, 2, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(
		[]int32{10, 10, 10, 20, 20}, nil, proc.Mp())
	input.SetRowCount(5)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	g.SpillMem = 64 << 20
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	g.ctr.distinctDrainKeysForUT = 2

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
	counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
	got := make(map[int32]int64, len(keys))
	for row, key := range keys {
		got[key] = counts[row]
	}
	require.Equal(t, map[int32]int64{1: 1, 2: 2}, got)
	require.Nil(t, g.ctr.distinctSpill)
	require.False(t, g.ctr.distinctGroupReset)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupedHotKeyForcedCollisionUsesExternalSort(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	groups := make([]int32, 100)
	values := make([]int32, 100)
	for i := range values {
		groups[i] = 7
		values[i] = int32(i)
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(groups, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(len(values))

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	_, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	controller.hashForUT = func(uint64, int, []byte) uint64 { return 0 }
	controller.sortArenaBytesForUT = 2 * 1024
	g.ctr.distinctSpill = controller
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)
	require.NoError(t, g.ctr.finalizeGroupedExactCountDistinct(proc))
	require.Positive(t, controller.externalSorts)

	result, err := g.ctr.aggList[0].Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{100},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(g.ctr.mp)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupedDistinctSpillPreservesMixedAggregateState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2, 2, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(
		[]int32{10, 10, 10, 20, 20}, nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt32Vector(
		[]int32{5, 7, 1, 2, 3}, nil, proc.Mp())
	input.SetRowCount(5)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{
			sumAgg(2),
			countStarAgg(),
			countDistinctAgg(1),
		},
	)
	g.SpillMem = 64 << 20
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	g.ctr.distinctDrainKeysForUT = 2

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
	sums := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
	rows := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[2])
	distinct := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[3])
	type aggregateResult struct {
		sum      int64
		rows     int64
		distinct int64
	}
	got := make(map[int32]aggregateResult, len(keys))
	for row, key := range keys {
		got[key] = aggregateResult{
			sum: sums[row], rows: rows[row], distinct: distinct[row],
		}
	}
	require.Equal(t, map[int32]aggregateResult{
		1: {sum: 12, rows: 2, distinct: 1},
		2: {sum: 6, rows: 3, distinct: 2},
	}, got)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestDistinctKeySpillComposesWithRecursiveGroupSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2, 2, 3, 3}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(
		[]int32{10, 10, 20, 21, 30, 31}, nil, proc.Mp())
	input.SetRowCount(6)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	// Historical sub-10K SpillMem is a deterministic group-count threshold.
	// The separate exact-key threshold activates first; two groups then force
	// ordinary group spill, exercising both ownership graphs together.
	g.SpillMem = 2
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	g.ctr.distinctDrainKeysForUT = 2

	got := make(map[int32]int64, 3)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for row, key := range keys {
			got[key] = counts[row]
		}
	}
	require.Equal(t, map[int32]int64{1: 1, 2: 2, 3: 2}, got)
	require.GreaterOrEqual(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillMaxLevel"], int64(1))
	require.Nil(t, g.ctr.distinctSpill)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestDistinctContributionPathIsFilteredAndAppliedOnce(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(
		[]int32{10, 20}, nil, proc.Mp())
	input.SetRowCount(2)
	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	_, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)
	require.Len(t, g.ctr.spillHashCodes, 2)
	require.NoError(t, g.ctr.prepareGroupedDistinctContributions(proc))

	paths := make(map[[spillMaxPass]uint8]struct{})
	for _, hash := range g.ctr.spillHashCodes {
		var path [spillMaxPass]uint8
		path[0] = uint8(distinctGroupBucket(hash, 1))
		path[1] = uint8(distinctGroupBucket(hash, 2))
		paths[path] = struct{}{}
	}
	var first *spillBucket
	for path := range paths {
		bucket := &spillBucket{path: path, pathLen: 2}
		if first == nil {
			first = bucket
		}
		require.NoError(t, g.ctr.applyDistinctContributions(proc, bucket))
	}
	require.NotNil(t, first)
	require.NoError(t, g.ctr.applyDistinctContributions(proc, first),
		"reapplying one completed leaf path must be idempotent")

	result, err := g.ctr.aggList[0].Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 1},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(g.ctr.mp)
	g.ctr.finishDistinctContributions()

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestIntermediateDistinctSpillEmitsExactKeysAcrossWorkers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	aggs := []aggexec.AggFuncExecExpression{countDistinctAgg(0)}
	buildPartial := func(values []int32) []*batch.Batch {
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(len(values))
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		partial := newGroupOp(proc, nil, aggs)
		partial.NeedEval = false
		partial.SpillMem = 64 << 20
		partial.AppendChild(child)
		allocation := installGroupTestAllocation(t, partial, proc, 64<<20)
		require.NoError(t, partial.Prepare(proc))
		partial.ctr.distinctDrainKeysForUT = 2

		var cloned []*batch.Batch
		for {
			result, err := vm.Exec(partial, proc)
			require.NoError(t, err)
			if result.Batch == nil || result.Status == vm.ExecStop {
				break
			}
			cloned = append(cloned, cloneBatch(t, proc, result.Batch))
		}
		require.Greater(t, len(cloned), 1,
			"ordinary neutral state plus exact-key leaves must stream separately")
		partial.Free(proc, false, nil)
		require.Zero(t, allocation.account.Snapshot().Used)
		finalizeGroupTestAllocation(t, partial, allocation)
		child.Free(proc, false, nil)
		return cloned
	}

	partials := append(
		buildPartial([]int32{1, 1, 2, 3}),
		buildPartial([]int32{3, 4, 4, 5})...,
	)
	child := colexec.NewMockOperator().WithBatchs(partials)
	merge := newMergeGroupOp(aggs)
	merge.AppendChild(child)
	allocation := installGroupTestAllocation(t, merge, proc, 64<<20)
	require.NoError(t, merge.Prepare(proc))
	// Drain every incoming exact-key leaf so the shared key reaches the spill
	// controller from both workers. With a larger threshold, the resident
	// skiplist may remove it before spill and the spill-level duplicate metric
	// would correctly remain zero.
	merge.ctr.distinctDrainKeysForUT = 1
	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	require.Equal(t, []int64{5},
		vector.MustFixedColNoTypeCheck[int64](outputs[0].Vecs[0]))
	require.Positive(t,
		merge.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillKeys"])
	require.Positive(t,
		merge.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillDuplicatesRemoved"])

	merge.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, merge, allocation)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestH0DistinctSpillPreservesMultiArgumentNullSemantics(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt64Vector(
		[]int64{1, 1, 2, 2, 3, 4}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeVarcharVector(
		[]string{"a", "a", "b", "b", "ignored", "d"},
		[]uint64{4},
		proc.Mp(),
	)
	input.SetRowCount(6)
	agg := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfCountColumn,
		true,
		[]*plan.Expr{
			colExpr(0, types.T_int64),
			colExpr(1, types.T_varchar),
		},
		nil,
	)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{agg})
	g.SpillMem = 64 << 20
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	g.ctr.distinctDrainKeysForUT = 2

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{3},
		vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestH0DistinctSpillCanonicalizesSignedZero(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeFloat64Vector(
		[]float64{math.Copysign(0, -1), 0, 1, 1}, nil, proc.Mp())
	input.SetRowCount(4)
	agg := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfCountColumn,
		true,
		[]*plan.Expr{colExpr(0, types.T_float64)},
		nil,
	)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{agg})
	g.SpillMem = 64 << 20
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	g.ctr.distinctDrainKeysForUT = 1

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{2},
		vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestH0DistinctSpillRespectsHardAccountBelowFullSetSize(t *testing.T) {
	const (
		accountLimit = uint64(2 << 20)
		payloadBytes = 64 << 10
		keys         = 40
		batchKeys    = 4
	)
	require.Greater(t, uint64(payloadBytes*keys), accountLimit)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	batches := make([]*batch.Batch, 0, keys/batchKeys)
	for start := 0; start < keys; start += batchKeys {
		values := make([]string, batchKeys)
		for row := range values {
			values[row] = fmt.Sprintf(
				"%03d-%s", start+row, strings.Repeat("x", payloadBytes-4))
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
		input.SetRowCount(len(values))
		batches = append(batches, input)
	}
	agg := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfCountColumn,
		true,
		[]*plan.Expr{colExpr(0, types.T_varchar)},
		nil,
	)
	child := colexec.NewMockOperator().WithBatchs(batches)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{agg})
	g.SpillMem = 512 << 10
	g.AppendChild(child)
	allocation := installGroupTestAllocation(t, g, proc, accountLimit)
	require.NoError(t, g.Prepare(proc))

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{keys},
		vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
	require.LessOrEqual(t, allocation.account.Snapshot().Peak, accountLimit)
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillKeys"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
