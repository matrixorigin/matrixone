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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	body := bytes.Clone(valid[16 : len(valid)-12])
	payloadLengthOffset := len(body) - 4 - len("key")
	withPayloadLength := func(length int32) []byte {
		var payload bytes.Buffer
		_, err := payload.Write(body[:payloadLengthOffset])
		require.NoError(t, err)
		require.NoError(t, types.WriteInt32(&payload, length))
		_, err = payload.Write(body[payloadLengthOffset+4:])
		require.NoError(t, err)
		return distinctTestEnvelope(t, distinctSpillKindKey, payload.Bytes())
	}
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
	_, _, _, _, _, err = controller.readRecord(nil, groups)
	require.Error(t, err)

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
	for _, test := range []struct {
		name string
		wire []byte
	}{
		{name: "inner route hash", wire: distinctTestEnvelope(t, distinctSpillKindKey, body[:1])},
		{name: "inner group hash", wire: distinctTestEnvelope(t, distinctSpillKindKey, body[:8])},
		{name: "inner aggregate", wire: distinctTestEnvelope(t, distinctSpillKindKey, body[:16])},
		{name: "inner group", wire: distinctTestEnvelope(t, distinctSpillKindKey, body[:20])},
		{name: "inner payload length", wire: distinctTestEnvelope(t, distinctSpillKindKey, body[:payloadLengthOffset])},
		{name: "payload exceeds body", wire: withPayloadLength(int32(len("key") + 1))},
		{name: "trailing inner payload", wire: withPayloadLength(int32(len("key") - 1))},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, _, _, err := controller.readRecord(bytes.NewReader(test.wire), groups)
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
	resultBucket := distinctGroupBucket(11, 1)
	result := controller.result[0][resultBucket]
	require.NoError(t, result.flushWriter())
	_, err = result.file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	valid, err := io.ReadAll(result.file)
	require.NoError(t, err)
	body := bytes.Clone(valid[16 : len(valid)-12])
	countOffset := len(body) - 8

	hash, aggregate, count, eof, err := controller.readContribution(bytes.NewReader(valid), groups)
	require.NoError(t, err)
	require.False(t, eof)
	require.Equal(t, uint64(11), hash)
	require.Zero(t, aggregate)
	require.Equal(t, uint64(3), count)
	_, _, _, eof, err = controller.readContribution(bytes.NewReader(nil), groups)
	require.NoError(t, err)
	require.True(t, eof)
	_, _, _, _, err = controller.readContribution(nil, groups)
	require.Error(t, err)

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
	for _, test := range []struct {
		name string
		wire []byte
	}{
		{name: "inner group hash", wire: distinctTestEnvelope(t, distinctSpillKindContribution, body[:1])},
		{name: "inner aggregate", wire: distinctTestEnvelope(t, distinctSpillKindContribution, body[:8])},
		{name: "inner group", wire: distinctTestEnvelope(t, distinctSpillKindContribution, body[:12])},
		{name: "inner count", wire: distinctTestEnvelope(t, distinctSpillKindContribution, body[:countOffset])},
		{name: "truncated inner count", wire: distinctTestEnvelope(t, distinctSpillKindContribution, body[:countOffset+4])},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, _, err := controller.readContribution(bytes.NewReader(test.wire), groups)
			require.Error(t, err)
		})
	}

	for failAt := 1; failAt <= 7; failAt++ {
		result.writer = &distinctFailNthWriter{failAt: failAt}
		err = controller.writeContribution(proc, spillfs, 11, 0, groups, 0, 3)
		require.ErrorIs(t, err, io.ErrClosedPipe)
	}
	result.writer = nil
	controller.result[0][resultBucket] = nil
	require.ErrorContains(t,
		controller.writeContribution(proc, spillfs, 11, 0, groups, 0, 3),
		"closed")
	controller.result[0][resultBucket] = result

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

type distinctFlushErrorWriter struct {
	err error
}

func (w *distinctFlushErrorWriter) Write(value []byte) (int, error) {
	return len(value), nil
}

func (w *distinctFlushErrorWriter) Flush() error {
	return w.err
}

type distinctResizeErrorBuffer struct {
	err      error
	capacity int
}

func (b *distinctResizeErrorBuffer) Write(value []byte) (int, error) {
	return len(value), nil
}

func (*distinctResizeErrorBuffer) Bytes() []byte { return nil }
func (*distinctResizeErrorBuffer) Len() int      { return 0 }
func (*distinctResizeErrorBuffer) Reset()        {}
func (b *distinctResizeErrorBuffer) Cap() int    { return b.capacity }
func (b *distinctResizeErrorBuffer) Resize(int) error {
	return b.err
}
func (*distinctResizeErrorBuffer) Free() {}

type distinctFailNthBuffer struct {
	unaccountedSpillBuffer
	writes int
	failAt int
	err    error
}

func (b *distinctFailNthBuffer) Write(value []byte) (int, error) {
	b.writes++
	if b.writes == b.failAt {
		return 0, b.err
	}
	return b.unaccountedSpillBuffer.Write(value)
}

func distinctTestEnvelope(t *testing.T, kind uint16, payload []byte) []byte {
	t.Helper()
	require.LessOrEqual(t, len(payload), math.MaxInt32)
	var encoded bytes.Buffer
	require.NoError(t, types.WriteUint64(&encoded, distinctSpillMagic))
	require.NoError(t, types.WriteUint16(&encoded, distinctSpillVersion))
	require.NoError(t, types.WriteUint16(&encoded, kind))
	require.NoError(t, types.WriteInt32(&encoded, int32(len(payload))))
	_, err := encoded.Write(payload)
	require.NoError(t, err)
	require.NoError(t, types.WriteInt32(&encoded, int32(len(payload))))
	require.NoError(t, types.WriteUint64(&encoded, distinctSpillMagic))
	return encoded.Bytes()
}

func distinctTestFile(t *testing.T, contents []byte) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "distinct-spill-")
	require.NoError(t, err)
	if len(contents) > 0 {
		_, err = file.Write(contents)
		require.NoError(t, err)
	}
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return file
}

func distinctTestSortFile(t *testing.T, keys ...[]byte) *os.File {
	t.Helper()
	file := distinctTestFile(t, nil)
	for _, key := range keys {
		require.NoError(t, writeDistinctSortKey(file, key))
	}
	_, err := file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return file
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
	require.ErrorIs(t, controller.ensureRecordBuffer(), mpool.ErrAllocationAccountInvalid)
	controller.recordCompletion()
	require.True(t, controller.completionRecorded)
	controller.completionRecorded = false
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
	var truncatedKey bytes.Buffer
	require.NoError(t, types.WriteInt32(&truncatedKey, 3))
	_, err = truncatedKey.Write([]byte("x"))
	require.NoError(t, err)
	_, _, err = readDistinctSortKey(&truncatedKey, buffer)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	var resizeKey bytes.Buffer
	require.NoError(t, types.WriteInt32(&resizeKey, 1))
	_, err = resizeKey.Write([]byte("x"))
	require.NoError(t, err)
	resizeErr := fmt.Errorf("resize distinct key")
	_, _, err = readDistinctSortKey(
		&resizeKey, &distinctResizeErrorBuffer{err: resizeErr})
	require.ErrorIs(t, err, resizeErr)
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
	require.ErrorIs(t, (*container)(nil).finalizeGroupedExactCountDistinctViaSpill(
		proc, nil),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*distinctSpillController)(nil).flushContributionWriters(),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (&distinctSpillController{closed: true}).flushContributionWriters(),
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
	flushErr := fmt.Errorf("flush distinct partition")
	require.ErrorIs(t, controller.mergeCommittedWave(
		proc,
		spillfs,
		&[spillNumBuckets]*spillBucket{
			0: {cnt: 1, writer: &distinctFlushErrorWriter{err: flushErr}},
		}), flushErr)
	_, err = controller.mergeSortRuns(
		proc,
		spillfs,
		&spillBucket{writer: &distinctFlushErrorWriter{err: flushErr}},
		&spillBucket{},
	)
	require.ErrorIs(t, err, flushErr)
	_, err = controller.mergeSortRuns(
		proc,
		spillfs,
		&spillBucket{},
		&spillBucket{writer: &distinctFlushErrorWriter{err: flushErr}},
	)
	require.ErrorIs(t, err, flushErr)
	buffer.Free()
	controller.close()
	pending := &distinctSpillController{partialPendingCount: 1}
	pending.partialPending[0] = &spillBucket{}
	pending.close()

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestDistinctSpillInternalIOFailureBoundaries(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countDistinctAgg(0)})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	defer controller.close()
	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	groups := batch.NewWithSize(0)
	groups.SetRowCount(1)
	sentinel := fmt.Errorf("distinct spill buffer failure")

	for failAt := 1; failAt <= 6; failAt++ {
		controller.record = &distinctFailNthBuffer{failAt: failAt, err: sentinel}
		_, err = controller.writeRecord(
			io.Discard, 17, 11, 0, groups, 0, []byte("key"))
		require.ErrorIs(t, err, sentinel)
		controller.record.Free()
		controller.record = nil
	}
	for failAt := 1; failAt <= 4; failAt++ {
		controller.record = &distinctFailNthBuffer{failAt: failAt, err: sentinel}
		err = controller.writeContribution(proc, spillfs, 11, 0, groups, 0, 3)
		require.ErrorIs(t, err, sentinel)
		controller.record.Free()
		controller.record = nil
	}

	var encoded bytes.Buffer
	_, err = controller.writeRecord(
		&encoded, 17, 11, 0, groups, 0, []byte("key"))
	require.NoError(t, err)
	validRecord := bytes.Clone(encoded.Bytes())
	recordBodyLength := len(validRecord) - 16 - 12
	_, _, _, _, _, err = controller.readRecord(
		bytes.NewReader(validRecord[:16+recordBodyLength]), groups)
	require.ErrorIs(t, err, io.EOF)
	controller.record = &distinctResizeErrorBuffer{err: sentinel}
	_, _, _, _, _, err = controller.readRecord(bytes.NewReader(validRecord), groups)
	require.ErrorIs(t, err, sentinel)
	controller.record = nil
	missingRecordController := &distinctSpillController{}
	_, _, _, _, _, err = missingRecordController.readRecord(
		bytes.NewReader(validRecord), groups)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	groups.SetRowCount(1)
	require.NoError(t, controller.writeContribution(proc, spillfs, 11, 0, groups, 0, 3))
	result := controller.result[0][distinctGroupBucket(11, 1)]
	require.NoError(t, result.flushWriter())
	_, err = result.file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	validContribution, err := io.ReadAll(result.file)
	require.NoError(t, err)
	contributionBodyLength := len(validContribution) - 16 - 12
	_, _, _, _, err = controller.readContribution(
		bytes.NewReader(validContribution[:16+contributionBodyLength]), groups)
	require.ErrorIs(t, err, io.EOF)
	controller.record = &distinctResizeErrorBuffer{err: sentinel}
	_, _, _, _, err = controller.readContribution(
		bytes.NewReader(validContribution), groups)
	require.ErrorIs(t, err, sentinel)
	controller.record = nil
	_, _, _, _, err = missingRecordController.readContribution(
		bytes.NewReader(validContribution), groups)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	noContainer := &distinctSpillController{}
	require.ErrorIs(t, noContainer.ensureSortBuffers(), mpool.ErrAllocationAccountInvalid)
	noContainer.sortKey = &unaccountedSpillBuffer{}
	require.ErrorIs(t, noContainer.ensureSortBuffers(), mpool.ErrAllocationAccountInvalid)
	noContainer.mergeLeft = &unaccountedSpillBuffer{}
	require.ErrorIs(t, noContainer.ensureSortBuffers(), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, noContainer.mergeCommittedWave(
		proc, spillfs, &[spillNumBuckets]*spillBucket{}),
		mpool.ErrAllocationAccountInvalid)
	_, err = noContainer.mergeSortRuns(
		proc, spillfs, &spillBucket{}, &spillBucket{})
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	controller.copy = &distinctResizeErrorBuffer{err: sentinel}
	require.ErrorIs(t, controller.mergeCommittedWave(
		proc, spillfs, &[spillNumBuckets]*spillBucket{}), sentinel)
	controller.copy = nil
	controller.copy = &distinctResizeErrorBuffer{
		err: sentinel, capacity: spillWrBufSize,
	}
	resizeSource := distinctTestFile(t, nil)
	resizeWave := &[spillNumBuckets]*spillBucket{
		0: {file: resizeSource, cnt: 1},
	}
	require.ErrorIs(t,
		controller.mergeCommittedWave(proc, spillfs, resizeWave), sentinel)
	controller.copy = nil
	require.NoError(t, resizeSource.Close())
	originalContext := proc.Ctx
	canceledContext, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = canceledContext
	cancel()
	require.Error(t, controller.mergeCommittedWave(
		proc, spillfs, &[spillNumBuckets]*spillBucket{}))
	proc.Ctx = originalContext
	closedWaveFile := distinctTestFile(t, nil)
	require.NoError(t, closedWaveFile.Close())
	closedWave := &[spillNumBuckets]*spillBucket{
		0: {file: closedWaveFile, cnt: 1},
	}
	require.Error(t, controller.mergeCommittedWave(proc, spillfs, closedWave))
	copySource := distinctTestFile(t, []byte("copy"))
	copyTarget := distinctTestFile(t, nil)
	controller.root[0] = &spillBucket{
		file: copyTarget, writer: &distinctFailNthWriter{failAt: 1},
	}
	copyWave := &[spillNumBuckets]*spillBucket{
		0: {file: copySource, cnt: 1},
	}
	require.ErrorIs(t,
		controller.mergeCommittedWave(proc, spillfs, copyWave), io.ErrClosedPipe)
	require.NoError(t, copySource.Close())
	closedLeft := distinctTestFile(t, nil)
	require.NoError(t, closedLeft.Close())
	validRight := distinctTestSortFile(t, []byte("right"))
	_, err = controller.mergeSortRuns(
		proc, spillfs, &spillBucket{file: closedLeft}, &spillBucket{file: validRight})
	require.Error(t, err)
	require.NoError(t, validRight.Close())
	validLeft := distinctTestSortFile(t, []byte("left"))
	closedRight := distinctTestFile(t, nil)
	require.NoError(t, closedRight.Close())
	_, err = controller.mergeSortRuns(
		proc, spillfs, &spillBucket{file: validLeft}, &spillBucket{file: closedRight})
	require.Error(t, err)
	require.NoError(t, validLeft.Close())

	malformedLeft := distinctTestFile(t, []byte{1})
	emptyRight := distinctTestFile(t, nil)
	_, err = controller.mergeSortRuns(
		proc, spillfs, &spillBucket{file: malformedLeft}, &spillBucket{file: emptyRight})
	require.Error(t, err)
	require.NoError(t, malformedLeft.Close())
	require.NoError(t, emptyRight.Close())
	emptyLeft := distinctTestFile(t, nil)
	malformedRight := distinctTestFile(t, []byte{1})
	_, err = controller.mergeSortRuns(
		proc, spillfs, &spillBucket{file: emptyLeft}, &spillBucket{file: malformedRight})
	require.Error(t, err)
	require.NoError(t, emptyLeft.Close())
	require.NoError(t, malformedRight.Close())

	cancelLeft := distinctTestSortFile(t, []byte("left"))
	cancelRight := distinctTestSortFile(t, []byte("right"))
	originalContext = proc.Ctx
	canceledContext, cancel = context.WithCancel(proc.Ctx)
	proc.Ctx = canceledContext
	cancel()
	_, err = controller.mergeSortRuns(
		proc, spillfs, &spillBucket{file: cancelLeft}, &spillBucket{file: cancelRight})
	require.Error(t, err)
	proc.Ctx = originalContext
	require.NoError(t, cancelLeft.Close())
	require.NoError(t, cancelRight.Close())

	repartitionFlushFile := distinctTestFile(t, nil)
	_, _, err = controller.repartition(proc, &spillBucket{
		file:   repartitionFlushFile,
		writer: &distinctFlushErrorWriter{err: sentinel},
		cnt:    1,
	}, groups)
	require.ErrorIs(t, err, sentinel)
	require.NoError(t, repartitionFlushFile.Close())
	repartitionClosed := distinctTestFile(t, nil)
	require.NoError(t, repartitionClosed.Close())
	_, _, err = controller.repartition(
		proc, &spillBucket{file: repartitionClosed, cnt: 1}, groups)
	require.Error(t, err)
	repartitionMalformed := distinctTestFile(t, []byte{1})
	_, _, err = controller.repartition(
		proc, &spillBucket{file: repartitionMalformed, cnt: 1}, groups)
	require.Error(t, err)
	require.NoError(t, repartitionMalformed.Close())
	repartitionWrite := distinctTestFile(t, nil)
	groups.SetRowCount(1)
	_, err = controller.writeRecord(
		repartitionWrite, 0, 0, 0, groups, 0, []byte("key"))
	require.NoError(t, err)
	_, err = repartitionWrite.Seek(0, io.SeekStart)
	require.NoError(t, err)
	controller.record = &distinctFailNthBuffer{failAt: 1, err: sentinel}
	_, _, err = controller.repartition(
		proc, &spillBucket{file: repartitionWrite, cnt: 1}, groups)
	require.ErrorIs(t, err, sentinel)
	controller.record = nil
	require.NoError(t, repartitionWrite.Close())

	controller.close()
	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestDistinctSpillExternalSortFailureBoundaries(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countDistinctAgg(0)})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	controller, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	defer controller.close()
	groups := batch.NewWithSize(0)
	groups.SetRowCount(1)

	flushErr := fmt.Errorf("flush external sort partition")
	flushFile := distinctTestFile(t, nil)
	_, err = controller.externalSortH0Partition(
		proc, &spillBucket{file: flushFile, writer: &distinctFlushErrorWriter{err: flushErr}})
	require.ErrorIs(t, err, flushErr)
	require.NoError(t, flushFile.Close())
	closed := distinctTestFile(t, nil)
	require.NoError(t, closed.Close())
	_, err = controller.externalSortH0Partition(proc, &spillBucket{file: closed})
	require.Error(t, err)

	empty := distinctTestFile(t, nil)
	counts, err := controller.externalSortH0Partition(proc, &spillBucket{file: empty})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, counts)
	require.NoError(t, empty.Close())
	malformed := distinctTestFile(t, []byte{1})
	_, err = controller.externalSortH0Partition(proc, &spillBucket{file: malformed})
	require.Error(t, err)
	require.NoError(t, malformed.Close())

	outOfRange := distinctTestFile(t, nil)
	_, err = controller.writeRecord(outOfRange, 0, 0, 1, groups, 0, []byte("key"))
	require.NoError(t, err)
	_, err = outOfRange.Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = controller.externalSortH0Partition(proc, &spillBucket{file: outOfRange})
	require.ErrorContains(t, err, "ordinal")
	require.NoError(t, outOfRange.Close())

	resizeFile := distinctTestFile(t, nil)
	_, err = controller.writeRecord(resizeFile, 0, 0, 0, groups, 0, []byte("key"))
	require.NoError(t, err)
	_, err = resizeFile.Seek(0, io.SeekStart)
	require.NoError(t, err)
	resizeErr := fmt.Errorf("resize external sort key")
	controller.sortKey = &distinctResizeErrorBuffer{err: resizeErr}
	_, err = controller.externalSortH0Partition(proc, &spillBucket{file: resizeFile})
	require.ErrorIs(t, err, resizeErr)
	controller.sortKey = nil
	require.NoError(t, resizeFile.Close())

	wide := distinctTestFile(t, nil)
	_, err = controller.writeRecord(
		wide, 0, 0, 0, groups, 0, []byte(strings.Repeat("x", 64*1024)))
	require.NoError(t, err)
	_, err = wide.Seek(0, io.SeekStart)
	require.NoError(t, err)
	controller.sortArenaBytesForUT = 4 * 1024
	_, err = controller.externalSortH0Partition(proc, &spillBucket{file: wide})
	require.ErrorContains(t, err, "requires more than")
	controller.sortArenaBytesForUT = 0
	require.NoError(t, wide.Close())

	controller.close()
	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestDistinctSpillTerminalStateBoundaries(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	emptyController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	g.ctr.distinctSpill = emptyController
	loaded, err := g.ctr.loadNextDistinctPartialLeaf(proc)
	require.NoError(t, err)
	require.False(t, loaded)
	require.Nil(t, g.ctr.distinctSpill)
	require.True(t, g.ctr.distinctFinalized)

	g.ctr.distinctFinalized = false
	preparedController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	g.ctr.distinctSpill = preparedController
	require.NoError(t, g.ctr.prepareGroupedDistinctContributions(proc))
	require.True(t, g.ctr.distinctContributionsPrepared)
	g.ctr.finishDistinctContributions()
	require.Nil(t, g.ctr.distinctSpill)
	require.True(t, g.ctr.distinctFinalized)

	g.ctr.distinctFinalized = false
	applyController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	applyController.result[0][1] = nil
	g.ctr.distinctSpill = applyController
	bucket := &spillBucket{pathLen: 1, path: [spillMaxPass]uint8{1}}
	require.NoError(t, g.ctr.applyDistinctContributions(proc, bucket))
	require.NoError(t, g.ctr.applyDistinctContributions(proc, bucket))
	deepEmpty := &spillBucket{
		pathLen: spillMaxPass,
		path:    [spillMaxPass]uint8{1, 2, 3},
	}
	require.NoError(t, g.ctr.applyDistinctContributions(proc, deepEmpty))
	require.NoError(t, g.ctr.applyDistinctContributions(proc, deepEmpty))
	require.ErrorContains(t,
		g.ctr.applyDistinctContributions(proc, &spillBucket{}), "path")
	applyController.close()
	g.ctr.distinctSpill = nil

	activeController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	activeFile := distinctTestFile(t, nil)
	activeController.partialActive = &spillBucket{file: activeFile}
	activeController.close()
	_, err = activeFile.Stat()
	require.Error(t, err, "closing a controller must retire its active partial leaf")

	flushController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	flushResultFile := distinctTestFile(t, nil)
	contributionFlushErr := fmt.Errorf("flush contributions")
	flushController.result[0][1] = &spillBucket{
		file:    flushResultFile,
		writer:  &distinctFlushErrorWriter{err: contributionFlushErr},
		cnt:     1,
		path:    [spillMaxPass]uint8{1},
		pathLen: 1,
	}
	require.ErrorIs(t, flushController.flushContributionWriters(),
		contributionFlushErr)
	flushController.result[0][1].writer =
		&distinctFlushErrorWriter{err: contributionFlushErr}
	g.ctr.distinctSpill = flushController
	require.Error(t, g.ctr.applyDistinctContributions(proc, bucket))
	flushController.close()

	seekController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	seekResultFile := distinctTestFile(t, nil)
	require.NoError(t, seekResultFile.Close())
	seekController.result[0][1] = &spillBucket{
		file: seekResultFile, cnt: 1,
		path: [spillMaxPass]uint8{1}, pathLen: 1,
	}
	g.ctr.distinctSpill = seekController
	require.Error(t, g.ctr.applyDistinctContributions(proc, bucket))
	seekController.close()

	malformedController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	malformedController.result[0][1] = &spillBucket{
		file: distinctTestFile(t, []byte{1}), cnt: 1,
		path: [spillMaxPass]uint8{1}, pathLen: 1,
	}
	g.ctr.distinctSpill = malformedController
	require.Error(t, g.ctr.applyDistinctContributions(proc, bucket))
	malformedController.close()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	contributionGroups, err := g.ctr.createNewGroupByBatchWithAllocation(
		nil, 1, g.ctr.spillGroupByAllocation)
	require.NoError(t, err)
	contributionGroups.SetRowCount(1)
	groupHash := uint64(11)
	matchingBucket := &spillBucket{
		pathLen: 1,
		path:    [spillMaxPass]uint8{uint8(distinctGroupBucket(groupHash, 1))},
	}
	outOfRangeController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	require.NoError(t, outOfRangeController.writeContribution(
		proc, spillfs, groupHash, 1, contributionGroups, 0, 1))
	g.ctr.distinctSpill = outOfRangeController
	require.ErrorContains(t,
		g.ctr.applyDistinctContributions(proc, matchingBucket), "out of range")
	outOfRangeController.close()
	contributionGroups.Clean(proc.Mp())
	g.ctr.distinctSpill = nil

	emptyGroupedController, err := newDistinctSpillController(&g.ctr)
	require.NoError(t, err)
	g.ctr.distinctSpill = emptyGroupedController
	require.ErrorContains(t,
		g.ctr.finalizeGroupedExactCountDistinctViaSpill(proc, g.OpAnalyzer),
		"did not spill resident groups")
	emptyGroupedController.close()
	g.ctr.distinctSpill = nil

	g.ctr.distinctContributionsPrepared = true
	require.NoError(t, g.ctr.finalizeExactCountDistinct(proc, g.OpAnalyzer))
	g.ctr.distinctContributionsPrepared = false
	g.ctr.distinctFinalized = false
	require.NoError(t, g.ctr.finalizeExactCountDistinct(proc, g.OpAnalyzer))
	require.True(t, g.ctr.distinctFinalized)

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

	got := make(map[int32]int64)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		output := result.Batch
		keys := vector.MustFixedColNoTypeCheck[int32](output.Vecs[0])
		counts := vector.MustFixedColNoTypeCheck[int64](output.Vecs[1])
		for row, key := range keys {
			got[key] = counts[row]
		}
	}
	require.Equal(t, map[int32]int64{1: 1, 2: 2}, got)
	require.Nil(t, g.ctr.distinctSpill)
	require.True(t, g.ctr.distinctGroupReset)
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

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
	require.NoError(t,
		g.ctr.finalizeGroupedExactCountDistinctViaSpill(proc, g.OpAnalyzer))
	require.Positive(t, controller.externalSorts)

	var counts []int64
	for {
		result, err := g.ctr.outputOneBatchFinal(proc, g.OpAnalyzer, g.Aggs)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		counts = append(counts,
			vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])...)
	}
	require.Equal(t, []int64{100}, counts)

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
	require.NoError(t,
		g.ctr.finalizeGroupedExactCountDistinctViaSpill(proc, g.OpAnalyzer))
	require.Positive(t, controller.repartitions)

	rows := 0
	for {
		result, err := g.ctr.outputOneBatchFinal(proc, g.OpAnalyzer, g.Aggs)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		for _, count := range vector.MustFixedColNoTypeCheck[int64](
			result.Batch.Vecs[1]) {
			require.Equal(t, int64(10), count)
			rows++
		}
	}
	require.Equal(t, 10, rows)

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

	type aggregateResult struct {
		sum      int64
		rows     int64
		distinct int64
	}
	got := make(map[int32]aggregateResult)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		output := result.Batch
		keys := vector.MustFixedColNoTypeCheck[int32](output.Vecs[0])
		sums := vector.MustFixedColNoTypeCheck[int64](output.Vecs[1])
		rows := vector.MustFixedColNoTypeCheck[int64](output.Vecs[2])
		distinct := vector.MustFixedColNoTypeCheck[int64](output.Vecs[3])
		for row, key := range keys {
			got[key] = aggregateResult{
				sum: sums[row], rows: rows[row], distinct: distinct[row],
			}
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

func TestDistinctContributionPathIsPartitionedAndAppliedOnce(t *testing.T) {
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
	contributionRecords := g.ctr.distinctSpill.contributionRecords
	require.Positive(t, contributionRecords)

	paths := make(map[[spillMaxPass]uint8]struct{})
	for _, hash := range g.ctr.spillHashCodes {
		var path [spillMaxPass]uint8
		path[0] = uint8(distinctGroupBucket(hash, 1))
		path[1] = uint8(distinctGroupBucket(hash, 2))
		path[2] = uint8(distinctGroupBucket(hash, 3))
		paths[path] = struct{}{}
	}
	var first *spillBucket
	for path := range paths {
		bucket := &spillBucket{path: path, pathLen: 3}
		if first == nil {
			first = bucket
		}
		require.NoError(t, g.ctr.applyDistinctContributions(proc, bucket))
	}
	require.NotNil(t, first)
	require.NoError(t, g.ctr.applyDistinctContributions(proc, first),
		"reapplying one completed leaf path must be idempotent")
	require.Equal(t, contributionRecords*3,
		g.ctr.distinctSpill.contributionReads,
		"a recursive consolidation reads each contribution once per level")

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

func TestIntermediateDistinctSpillTerminalLeafContinuesWithinHardAccount(t *testing.T) {
	for _, test := range []struct {
		name      string
		level     int
		routeHash func(int) uint64
	}{
		{
			name:  "maximum depth uniform input",
			level: spillMaxPass,
			routeHash: func(row int) uint64 {
				return uint64(row) * 0x9e3779b97f4a7c15
			},
		},
		{
			name:  "forced no progress",
			level: 0,
			routeHash: func(int) uint64 {
				return 0
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			partial := newGroupOp(
				proc,
				[]*plan.Expr{colExpr(0, types.T_int32)},
				[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
			)
			partial.NeedEval = false
			const accountLimit = uint64(2 << 20)
			allocation := installGroupTestAllocation(
				t, partial, proc, accountLimit)
			require.NoError(t, partial.Prepare(proc))
			seed := batch.NewWithSize(2)
			seed.Vecs[0] = testutil.MakeInt32Vector([]int32{0}, nil, proc.Mp())
			seed.Vecs[1] = testutil.MakeInt32Vector([]int32{0}, nil, proc.Mp())
			seed.SetRowCount(1)
			_, err := partial.buildOneBatch(proc, seed)
			require.NoError(t, err)
			partial.ctr.resetForDistinctPartialLeaf()
			seed.Clean(proc.Mp())

			controller, err := newDistinctSpillController(&partial.ctr)
			require.NoError(t, err)
			controller.sortArenaBytesForUT = 4 << 10
			partial.ctr.distinctSpill = controller
			partition := controller.root[0]
			partition.lv = test.level
			spillfs, err := proc.GetSpillFileService()
			require.NoError(t, err)
			require.NoError(t,
				partial.ctr.openSpillBucket(proc, spillfs, partition))
			groups := batch.NewWithSize(1)
			groups.Vecs[0] = testutil.MakeInt32Vector(
				[]int32{0}, nil, proc.Mp())
			groups.SetRowCount(1)
			groupValues := vector.MustFixedColNoTypeCheck[int32](groups.Vecs[0])
			const records = 20_000
			var payload [8]byte
			for row := 0; row < records; row++ {
				groupValues[0] = int32(row % 32)
				binary.BigEndian.PutUint64(payload[:], uint64(row))
				_, err := controller.writeRecord(
					partition.writer,
					test.routeHash(row),
					uint64(groupValues[0])+1,
					0,
					groups,
					0,
					payload[:],
				)
				require.NoError(t, err)
				partition.cnt++
			}

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
			require.Equal(t, int64(records), total)
			require.Greater(t, leaves, 1)
			require.Positive(t, controller.partialContinuations)
			require.LessOrEqual(t, allocation.account.Snapshot().Peak, accountLimit)
			require.Nil(t, partial.ctr.distinctSpill)

			partial.Free(proc, false, nil)
			require.Zero(t, allocation.account.Snapshot().Used)
			finalizeGroupTestAllocation(t, partial, allocation)
			groups.Clean(proc.Mp())
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func groupedDistinctHardAccountInput(
	proc *process.Process,
	argument int32,
) *batch.Batch {
	return groupedDistinctHardAccountInputRange(
		proc, 0, aggexec.AggBatchSize, argument)
}

func groupedDistinctHardAccountInputRange(
	proc *process.Process,
	start int,
	groups int,
	argument int32,
) *batch.Batch {
	keys := make([]int32, groups)
	arguments := make([]int32, groups)
	for row := range groups {
		keys[row] = int32(start + row)
		arguments[row] = argument
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(arguments, nil, proc.Mp())
	input.SetRowCount(groups)
	return input
}

func buildGroupedDistinctPartial(
	t *testing.T,
	proc *process.Process,
	start int,
	groups int,
	argument int32,
) *batch.Batch {
	t.Helper()
	input := groupedDistinctHardAccountInputRange(
		proc, start, groups, argument)
	partial := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	partial.NeedEval = false
	partial.AppendChild(colexec.NewMockOperator().WithBatchs(
		[]*batch.Batch{input}))
	require.NoError(t, partial.Prepare(proc))
	outputs := collectBatches(t, partial, proc)
	require.Len(t, outputs, 1)
	cloned := cloneBatch(t, proc, outputs[0])
	partial.Free(proc, false, nil)
	input.Clean(proc.Mp())
	return cloned
}

func assertGroupedDistinctHardAccountResult(
	t *testing.T,
	proc *process.Process,
	op vm.Operator,
	wantRows int,
	wantCount int64,
) {
	t.Helper()
	rows := 0
	for {
		result, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		rows += result.Batch.RowCount()
		for _, count := range vector.MustFixedColNoTypeCheck[int64](
			result.Batch.Vecs[1]) {
			require.Equal(t, wantCount, count)
		}
	}
	require.Equal(t, wantRows, rows)
}

func TestGroupedDistinctSpillFinalizationCompletesWithinHardAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	first := groupedDistinctHardAccountInput(proc, 1)
	second := groupedDistinctHardAccountInput(proc, 2)
	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	g.SpillMem = 64 << 20
	g.AppendChild(colexec.NewMockOperator().WithBatchs(
		[]*batch.Batch{first, second}))
	allocation := installGroupTestAllocation(t, g, proc, 1<<20)
	require.NoError(t, g.Prepare(proc))

	assertGroupedDistinctHardAccountResult(
		t, proc, g, aggexec.AggBatchSize, 2)
	require.GreaterOrEqual(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillActivations"],
		int64(2),
	)
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.LessOrEqual(t, allocation.account.Snapshot().Peak, uint64(1<<20))
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMergeGroupedDistinctSpillFinalizationCompletesWithinHardAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	const partialGroups = 512
	partials := make([]*batch.Batch, 0,
		2*aggexec.AggBatchSize/partialGroups)
	for _, argument := range []int32{1, 2} {
		for start := 0; start < aggexec.AggBatchSize; start += partialGroups {
			partials = append(partials, buildGroupedDistinctPartial(
				t, proc, start, partialGroups, argument))
		}
	}
	merge := newMergeGroupOp(
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)})
	merge.SpillMem = 64 << 20
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partials))
	allocation := installGroupTestAllocation(t, merge, proc, 1<<20)
	require.NoError(t, merge.Prepare(proc))

	assertGroupedDistinctHardAccountResult(
		t, proc, merge, aggexec.AggBatchSize, 2)
	require.GreaterOrEqual(t,
		merge.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillActivations"],
		int64(2),
	)
	require.Positive(t,
		merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	merge.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.LessOrEqual(t, allocation.account.Snapshot().Peak, uint64(1<<20))
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, merge, allocation)
	for _, partial := range partials {
		partial.Clean(proc.Mp())
	}
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupedDistinctSpillMultiChunkDrainCompletesWithinHardAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	const chunks = 3
	inputs := make([]*batch.Batch, 0, chunks)
	for chunk := range chunks {
		inputs = append(inputs, groupedDistinctHardAccountInputRange(
			proc,
			chunk*aggexec.AggBatchSize,
			aggexec.AggBatchSize,
			1,
		))
	}
	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
	)
	g.SpillMem = 64 << 20
	g.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	allocation := installGroupTestAllocation(t, g, proc, 2<<20)
	require.NoError(t, g.Prepare(proc))

	assertGroupedDistinctHardAccountResult(
		t, proc, g, chunks*aggexec.AggBatchSize, 1)
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillActivations"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.LessOrEqual(t, allocation.account.Snapshot().Peak, uint64(2<<20))
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	for _, input := range inputs {
		input.Clean(proc.Mp())
	}
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMergeGroupedDistinctSpillMultiChunkDrainCompletesWithinHardAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	const chunks = 3
	partials := make([]*batch.Batch, 0, chunks)
	for chunk := range chunks {
		partials = append(partials, buildGroupedDistinctPartial(
			t,
			proc,
			chunk*aggexec.AggBatchSize,
			aggexec.AggBatchSize,
			1,
		))
	}
	merge := newMergeGroupOp(
		[]aggexec.AggFuncExecExpression{countDistinctAgg(1)})
	merge.SpillMem = 64 << 20
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partials))
	allocation := installGroupTestAllocation(t, merge, proc, 2<<20)
	require.NoError(t, merge.Prepare(proc))

	assertGroupedDistinctHardAccountResult(
		t, proc, merge, chunks*aggexec.AggBatchSize, 1)
	require.Positive(t,
		merge.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillActivations"])

	merge.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.LessOrEqual(t, allocation.account.Snapshot().Peak, uint64(2<<20))
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, merge, allocation)
	for _, partial := range partials {
		partial.Clean(proc.Mp())
	}
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

func BenchmarkExactCountDistinctSpill(b *testing.B) {
	const rows = 4096
	for _, test := range []struct {
		name              string
		groups            int
		value             func(int) int32
		spillMem          int64
		distinctDrainKeys uint64
		wantSpill         bool
	}{
		{
			name:   "low-ndv-no-spill",
			groups: 8,
			value: func(row int) int32 {
				return int32(row % 16)
			},
			spillMem: 64 << 20,
		},
		{
			name:              "combined-distinct-and-group-spill",
			groups:            128,
			value:             func(row int) int32 { return int32(row) },
			spillMem:          16,
			distinctDrainKeys: 64,
			wantSpill:         true,
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
			defer proc.Free()
			keys := make([]int32, rows)
			values := make([]int32, rows)
			for row := range rows {
				keys[row] = int32(row % test.groups)
				values[row] = test.value(row)
			}
			b.ReportAllocs()
			b.SetBytes(rows * 8)
			for b.Loop() {
				b.StopTimer()
				input := batch.NewWithSize(2)
				input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
				input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
				input.SetRowCount(rows)
				g := newGroupOp(
					proc,
					[]*plan.Expr{colExpr(0, types.T_int32)},
					[]aggexec.AggFuncExecExpression{countDistinctAgg(1)},
				)
				g.SpillMem = test.spillMem
				g.AppendChild(colexec.NewMockOperator().WithBatchs(
					[]*batch.Batch{input}))
				allocation := installGroupTestAllocation(b, g, proc, 64<<20)
				b.StartTimer()

				if err := g.Prepare(proc); err != nil {
					b.Fatal(err)
				}
				g.ctr.distinctDrainKeysForUT = test.distinctDrainKeys
				outputRows := 0
				for {
					result, err := vm.Exec(g, proc)
					if err != nil {
						b.Fatal(err)
					}
					if result.Batch == nil || result.Status == vm.ExecStop {
						break
					}
					outputRows += result.Batch.RowCount()
				}
				if outputRows != test.groups {
					b.Fatalf("unexpected group count: got %d, want %d",
						outputRows, test.groups)
				}
				stats := g.OpAnalyzer.GetOpStats().ExtraStats
				if test.wantSpill {
					if stats["GroupDistinctSpillActivations"] == 0 ||
						stats["GroupSpillMaxLevel"] == 0 {
						b.Fatal("combined spill benchmark did not activate both paths")
					}
				} else if stats["GroupDistinctSpillActivations"] != 0 {
					b.Fatal("no-spill benchmark activated distinct spill")
				}

				b.StopTimer()
				g.Free(proc, false, nil)
				if used := allocation.account.Snapshot().Used; used != 0 {
					b.Fatalf("group allocation account leaked %d bytes", used)
				}
				finalizeGroupTestAllocation(b, g, allocation)
				input.Clean(proc.Mp())
				if allocated := proc.Mp().CurrNB(); allocated != 0 {
					b.Fatalf("group leaked %d bytes", allocated)
				}
				b.StartTimer()
			}
		})
	}
}
