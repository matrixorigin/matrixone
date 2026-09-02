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
	_, err := newDistinctSpillController(nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
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
	for failAt := 1; failAt <= 7; failAt++ {
		_, err = controller.writeRecord(
			&distinctFailNthWriter{failAt: failAt}, 17, 11, 0, groups, 0, []byte("key"))
		require.ErrorIs(t, err, io.ErrClosedPipe)
	}
	_, err = controller.writeRecord(nil, 17, 11, 0, groups, 0, []byte("key"))
	require.Error(t, err)
	_, err = controller.writeRecord(&encoded, 17, 11, math.MaxInt32+1, groups, 0, []byte("key"))
	require.ErrorContains(t, err, "ordinal")
	_, _, _, _, eof, err := controller.readRecord(bytes.NewReader(nil), groups)
	require.NoError(t, err)
	require.True(t, eof)

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
			name: "bad kind",
			mutate: func(value []byte) []byte {
				value[10] = 0xff
				return value
			},
		},
		{
			name: "zero length",
			mutate: func(value []byte) []byte {
				clear(value[12:16])
				return value
			},
		},
		{
			name: "truncated body",
			mutate: func(value []byte) []byte {
				return value[:20]
			},
		},
		{
			name: "bad trailing length",
			mutate: func(value []byte) []byte {
				value[len(value)-12] ^= 0xff
				return value
			},
		},
		{
			name: "bad trailing magic",
			mutate: func(value []byte) []byte {
				value[len(value)-8] ^= 0xff
				return value
			},
		},
		{
			name: "negative aggregate",
			mutate: func(value []byte) []byte {
				for i := 32; i < 36; i++ {
					value[i] = 0xff
				}
				return value
			},
		},
		{
			name: "negative payload length",
			mutate: func(value []byte) []byte {
				offset := len(value) - 12 - 4 - len("key")
				for i := offset; i < offset+4; i++ {
					value[i] = 0xff
				}
				return value
			},
		},
		{
			name: "truncated trailer",
			mutate: func(value []byte) []byte {
				return value[:len(value)-1]
			},
		},
		{name: "truncated magic", mutate: func(value []byte) []byte { return value[:1] }},
		{name: "missing version", mutate: func(value []byte) []byte { return value[:8] }},
		{name: "truncated version", mutate: func(value []byte) []byte { return value[:9] }},
		{name: "missing kind", mutate: func(value []byte) []byte { return value[:10] }},
		{name: "truncated kind", mutate: func(value []byte) []byte { return value[:11] }},
		{name: "missing length", mutate: func(value []byte) []byte { return value[:12] }},
		{name: "truncated length", mutate: func(value []byte) []byte { return value[:13] }},
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

func TestDistinctContributionEnvelopeRejectsCorruption(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countDistinctAgg(0)})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	groups := batch.NewWithSize(0)
	groups.SetRowCount(1)
	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	require.Error(t, controller.writeContribution(nil, spillfs, 11, 0, groups, 0, 3))
	require.Error(t, controller.writeContribution(proc, spillfs, 11, 0, groups, 0, 0))
	require.NoError(t, controller.writeContribution(proc, spillfs, 11, 0, groups, 0, 3))
	require.NoError(t, controller.result.flushWriter())
	_, err = controller.result.file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	valid, err := io.ReadAll(controller.result.file)
	require.NoError(t, err)

	hash, aggregate, count, eof, err := controller.readContribution(bytes.NewReader(valid), groups)
	require.NoError(t, err)
	require.False(t, eof)
	require.Equal(t, uint64(11), hash)
	require.Zero(t, aggregate)
	require.Equal(t, uint64(3), count)
	_, _, _, eof, err = controller.readContribution(bytes.NewReader(nil), groups)
	require.NoError(t, err)
	require.True(t, eof)

	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "bad header", mutate: func(value []byte) []byte { value[0] ^= 0xff; return value }},
		{name: "bad version", mutate: func(value []byte) []byte { value[8] = 0xff; return value }},
		{name: "bad kind", mutate: func(value []byte) []byte { value[10] = 0xff; return value }},
		{name: "zero length", mutate: func(value []byte) []byte { clear(value[12:16]); return value }},
		{name: "truncated body", mutate: func(value []byte) []byte { return value[:20] }},
		{name: "bad trailer", mutate: func(value []byte) []byte { value[len(value)-12] ^= 0xff; return value }},
		{name: "negative aggregate", mutate: func(value []byte) []byte {
			for i := 24; i < 28; i++ {
				value[i] = 0xff
			}
			return value
		}},
		{name: "zero count", mutate: func(value []byte) []byte {
			clear(value[len(value)-20 : len(value)-12])
			return value
		}},
		{name: "truncated trailer", mutate: func(value []byte) []byte { return value[:len(value)-1] }},
		{name: "truncated magic", mutate: func(value []byte) []byte { return value[:1] }},
		{name: "missing version", mutate: func(value []byte) []byte { return value[:8] }},
		{name: "truncated version", mutate: func(value []byte) []byte { return value[:9] }},
		{name: "missing kind", mutate: func(value []byte) []byte { return value[:10] }},
		{name: "truncated kind", mutate: func(value []byte) []byte { return value[:11] }},
		{name: "missing length", mutate: func(value []byte) []byte { return value[:12] }},
		{name: "truncated length", mutate: func(value []byte) []byte { return value[:13] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, _, err := controller.readContribution(
				bytes.NewReader(test.mutate(bytes.Clone(valid))), groups)
			require.Error(t, err)
		})
	}

	controller.close()
	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

type distinctFailNthWriter struct {
	writes int
	failAt int
}

func (w *distinctFailNthWriter) Write(value []byte) (int, error) {
	w.writes++
	if w.writes == w.failAt {
		return 0, io.ErrClosedPipe
	}
	return len(value), nil
}

func TestDistinctSpillControllerBoundaryContracts(t *testing.T) {
	var nilController *distinctSpillController
	nilController.close()
	nilController.recordCompletion()
	require.Nil(t, nilController.takePartialPartition())
	require.ErrorIs(t, nilController.pushPartialChildren(nil), mpool.ErrAllocationAccountInvalid)
	freeDistinctWave(nil)
	require.ErrorIs(t, nilController.mergeCommittedWave(nil, nil, nil), mpool.ErrAllocationAccountInvalid)
	_, _, err := nilController.repartition(nil, nil, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, _, err = nilController.allocateSortArena()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = nilController.flushSortSet(nil, nil, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = nilController.mergeSortRuns(nil, nil, nil, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = nilController.externalSortH0Partition(nil, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.Equal(t, 1024*1024, nilController.sortArenaCapacity())
	require.Zero(t, distinctGroupBucket(17, 0))
	require.GreaterOrEqual(t, distinctGroupBucket(17, 1), 0)

	controller := &distinctSpillController{}
	require.ErrorIs(t, controller.pushPartialChildren(nil), mpool.ErrAllocationAccountInvalid)
	children := [spillNumBuckets]*spillBucket{}
	children[1] = &spillBucket{name: "pending", cnt: 1}
	children[2] = &spillBucket{name: "empty"}
	require.NoError(t, controller.pushPartialChildren(&children))
	require.Nil(t, children[1])
	require.NotNil(t, children[2])
	require.Equal(t, "pending", controller.takePartialPartition().name)
	controller.root[3] = &spillBucket{name: "root", cnt: 1}
	require.Equal(t, "root", controller.takePartialPartition().name)
	require.Nil(t, controller.takePartialPartition())
	controller.partialPendingCount = len(controller.partialPending)
	children[0] = &spillBucket{name: "overflow", cnt: 1}
	require.ErrorContains(t, controller.pushPartialChildren(&children), "overflow")
	controller.partialPendingCount = 0

	for length := 1; length <= spillMaxPass; length++ {
		path := [spillMaxPass]uint8{1, 2, 3}
		applied, err := controller.contributionPathApplied(path, length)
		require.NoError(t, err)
		require.False(t, applied)
		require.NoError(t, controller.markContributionPathApplied(path, length))
		applied, err = controller.contributionPathApplied(path, length)
		require.NoError(t, err)
		require.True(t, applied)
	}
	_, err = controller.contributionPathApplied([spillMaxPass]uint8{}, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, controller.markContributionPathApplied(
		[spillMaxPass]uint8{}, spillMaxPass+1), mpool.ErrAllocationAccountInvalid)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countDistinctAgg(0)})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	require.Equal(t, 1024*1024, controller.sortArenaCapacity())
	controller.ctr = &g.ctr
	controller.sortArenaBytesForUT = 2048
	require.Equal(t, 2048, controller.sortArenaCapacity())
	controller.sortArenaBytesForUT = 0
	g.ctr.spillMem = 64 * 1024
	require.Equal(t, 64*1024, controller.sortArenaCapacity())
	g.ctr.spillMem = 64 * 1024 * 1024
	require.Equal(t, 8*1024*1024, controller.sortArenaCapacity())
	wave, err := controller.newPrivateWave()
	require.NoError(t, err)
	for _, bucket := range wave {
		require.NotEmpty(t, bucket.name)
	}
	freeDistinctWave(&wave)
	for _, bucket := range wave {
		require.Nil(t, bucket)
	}
	require.NoError(t, controller.ensureSortBuffers())
	require.NoError(t, controller.ensureSortBuffers())
	arenaBuffer, emptySet, err := controller.allocateSortArena()
	require.NoError(t, err)
	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	_, err = controller.flushSortSet(proc, spillfs, emptySet)
	require.ErrorContains(t, err, "empty")
	g.ctr.mp.Free(arenaBuffer)
	buffer, err := newGroupSpillBuffer(&g.ctr, GroupAllocationSiteDistinctRecord)
	require.NoError(t, err)
	require.Error(t, writeDistinctSortKey(io.Discard, nil))
	var wire bytes.Buffer
	require.NoError(t, writeDistinctSortKey(&wire, []byte("key")))
	key, eof, err := readDistinctSortKey(&wire, buffer)
	require.NoError(t, err)
	require.False(t, eof)
	require.Equal(t, []byte("key"), key)
	_, eof, err = readDistinctSortKey(bytes.NewReader(nil), buffer)
	require.NoError(t, err)
	require.True(t, eof)
	_, _, err = readDistinctSortKey(nil, buffer)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	var zeroLength bytes.Buffer
	require.NoError(t, types.WriteInt32(&zeroLength, 0))
	_, _, err = readDistinctSortKey(&zeroLength, buffer)
	require.ErrorContains(t, err, "length")

	_, _, err = (*container)(nil).groupBatchRow(0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, row, err := g.ctr.groupBatchRow(0)
	require.NoError(t, err)
	require.Zero(t, row)
	_, _, err = g.ctr.groupBatchRow(1)
	require.ErrorContains(t, err, "has no group row")
	require.False(t, func() bool { value, _, _ := (*container)(nil).exactCountDistinctStats(); return value != 0 }())
	require.False(t, func() bool { value, _ := (*container)(nil).hasExactCountDistinctArguments(); return value }())
	require.NoError(t, (*container)(nil).finalizeExactCountDistinct(proc, nil))
	require.NoError(t, (*container)(nil).applyDistinctContributions(proc, nil))
	_, err = (*container)(nil).loadNextDistinctPartialLeaf(proc)
	require.NoError(t, err)
	require.ErrorIs(t, (*container)(nil).finalizeGroupedExactCountDistinct(proc),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*container)(nil).prepareGroupedDistinctContributions(proc),
		mpool.ErrAllocationAccountInvalid)
	_, err = (*container)(nil).finalizeSingleGroupDistinctPartition(
		proc, controller, nil, nil, false)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*container)(nil).writeCompactedDistinctContributions(
		proc, controller, nil, nil, nil), mpool.ErrAllocationAccountInvalid)
	(*container)(nil).finishDistinctContributions()
	(*container)(nil).resetForDistinctPartialLeaf()
	buffer.Free()
	controller.close()

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
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

func TestH0DistinctExternalSortMergesMultipleWideKeyRuns(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	values := make([]string, 0, 60)
	for i := 0; i < 40; i++ {
		value := fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 4096))
		values = append(values, value)
		if i%2 == 0 {
			values = append(values, value)
		}
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	input.SetRowCount(len(values))
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countDistinctAgg(0)})
	allocation := installGroupTestAllocation(t, g, proc, 8<<20)
	require.NoError(t, g.Prepare(proc))
	_, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	controller.hashForUT = func(uint64, int, []byte) uint64 { return 0 }
	controller.forceExternalSortForUT = true
	controller.sortArenaBytesForUT = 64 * 1024
	g.ctr.distinctSpill = controller
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)
	require.NoError(t, g.ctr.finalizeH0ExactCountDistinct(proc))
	require.Positive(t, controller.externalSorts)

	result, err := g.ctr.aggList[0].Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{40}, vector.MustFixedColNoTypeCheck[int64](result[0]))
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

func TestGroupedDistinctSpillRecursivelyRepartitionsOversizedLeaf(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	groups := make([]int32, 100)
	values := make([]int32, 100)
	for i := range values {
		groups[i] = int32(i % 10)
		values[i] = int32(i)
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(groups, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(len(values))

	g := newGroupOp(proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	_, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	controller.hashForUT = func(_ uint64, _ int, payload []byte) uint64 {
		return xxhash.Sum64(payload) << distinctSpillMaskBits
	}
	controller.sortArenaBytesForUT = 4 * 1024
	g.ctr.distinctSpill = controller
	drained, err := g.ctr.drainExactCountDistinct(proc, g.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)
	require.NoError(t, g.ctr.finalizeGroupedExactCountDistinct(proc))
	require.Positive(t, controller.repartitions)

	result, err := g.ctr.aggList[0].Flush()
	require.NoError(t, err)
	for _, count := range vector.MustFixedColNoTypeCheck[int64](result[0]) {
		require.Equal(t, int64(10), count)
	}
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

func TestIntermediateDistinctSpillRepartitionsOversizedLeaf(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	values := make([]int32, 100)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(len(values))
	partial := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countDistinctAgg(0)})
	partial.NeedEval = false
	allocation := installGroupTestAllocation(t, partial, proc, 64<<20)
	require.NoError(t, partial.Prepare(proc))
	_, err := partial.buildOneBatch(proc, input)
	require.NoError(t, err)
	controller, err := newDistinctSpillController(&partial.ctr)
	require.NoError(t, err)
	controller.hashForUT = func(_ uint64, _ int, payload []byte) uint64 {
		return xxhash.Sum64(payload) << distinctSpillMaskBits
	}
	controller.sortArenaBytesForUT = 4 * 1024
	partial.ctr.distinctSpill = controller
	drained, err := partial.ctr.drainExactCountDistinct(proc, partial.OpAnalyzer)
	require.NoError(t, err)
	require.True(t, drained)

	total := int64(0)
	leaves := 0
	for {
		loaded, err := partial.ctr.loadNextDistinctPartialLeaf(proc)
		require.NoError(t, err)
		if !loaded {
			break
		}
		result, err := partial.ctr.aggList[0].Flush()
		require.NoError(t, err)
		for _, count := range vector.MustFixedColNoTypeCheck[int64](result[0]) {
			total += count
		}
		result[0].Free(partial.ctr.mp)
		leaves++
	}
	require.Greater(t, leaves, 1)
	require.Equal(t, int64(100), total)
	require.Positive(t, controller.repartitions)
	require.Nil(t, partial.ctr.distinctSpill)

	partial.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, partial, allocation)
	input.Clean(proc.Mp())
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
