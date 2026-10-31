// Copyright 2026 MatrixOne
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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

// The D-4 stage-1 mask (design rev 14 R14-F1) is a mask-only offset filter for
// the alive-generation blocks: application must remove exactly the mapped
// row offsets of the asked block, be idempotent under a full workspace/delete
// stage chain, stay inert for blocks the mask never mapped, and never panic
// on an empty mask. These are the ordinary-local consumer invariants of
// design R-D4 (designated plan members: own-delete suppression across a
// compaction, dual-view control, idempotence).
func newTestMask(t *testing.T, frontier types.TS, bid *types.Blockid, offsets ...uint32) *ExecutionReadTombstoneMask {
	t.Helper()
	mask := &ExecutionReadTombstoneMask{frontier: frontier}
	if bid != nil {
		mask.byBlock = map[types.Blockid]map[uint32]struct{}{
			*bid: {},
		}
		for _, off := range offsets {
			mask.byBlock[*bid][off] = struct{}{}
		}
	}
	return mask
}

func newTestBlockid(t *testing.T, n byte) *types.Blockid {
	t.Helper()
	var bid types.Blockid
	for i := range bid {
		bid[i] = n
	}
	return &bid
}

func TestExecutionReadTombstoneMaskApplyOffsets(t *testing.T) {
	frontier := types.BuildTS(900, 1)
	bidP := newTestBlockid(t, 1)
	bidOther := newTestBlockid(t, 2)
	mask := newTestMask(t, frontier, bidP, 3, 7, 8)

	// Mapped offsets die, unmapped survive, order preserved otherwise.
	left := mask.applyOffsets(bidP, []int64{0, 1, 2, 3, 4, 7, 8, 100})
	require.Equal(t, []int64{0, 1, 2, 4, 100}, left)

	// A foreign block is untouched.
	left = mask.applyOffsets(bidOther, []int64{3, 7, 8})
	require.Equal(t, []int64{3, 7, 8}, left)

	// Idempotence: re-application removes nothing further.
	require.Equal(t, left, mask.applyOffsets(bidOther, left))

	// An empty mask owns nothing.
	emptyP := &ExecutionReadTombstoneMask{frontier: frontier}
	require.Equal(t, []int64{3, 7},
		emptyP.applyOffsets(bidP, []int64{3, 7}))

	// The nil receiver is inert (unmarked readers pay nothing).
	var nilMask *ExecutionReadTombstoneMask
	require.Equal(t, []int64{3, 7}, nilMask.applyOffsets(bidP, []int64{3, 7}))
}

func TestExecutionReadTombstoneMaskMarkInto(t *testing.T) {
	frontier := types.BuildTS(900, 1)
	bidP := newTestBlockid(t, 1)
	mask := newTestMask(t, frontier, bidP, 5, 11)

	target := objectio.GetReusableBitmap()
	defer target.Release()
	mask.markInto(bidP, &target)
	require.True(t, target.Contains(5))
	require.True(t, target.Contains(11))
	require.False(t, target.Contains(0))
	require.False(t, target.Contains(6))

	foreign := objectio.GetReusableBitmap()
	defer foreign.Release()
	mask.markInto(newTestBlockid(t, 2), &foreign)
	require.True(t, foreign.IsEmpty())

	var nilMask *ExecutionReadTombstoneMask
	// A nil receiver (the unmarked-reader path) is inert.
	nilMask.markInto(bidP, &foreign)
	require.True(t, foreign.IsEmpty())
}

// The P1 flushed-stats enumeration member: a flushed DELETE workspace entry
// carries ONE MARSHALLED ObjectStats ROW PER TOMBSTONE OBJECT (colexec
// FillBlockInfoBat's tombstone shape), and the mask producer must enumerate
// every one of them from the owning entry — entry.fileName only records the
// FIRST object's location, so name-keyed registry lookups drop whole
// flushed intents whenever a flush produced more than one tombstone object,
// and an empty mask would resurrect the transaction's own deleted rows.
func TestFlushedTombstoneStatsEnumeratesEveryObjectEntry(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.GetMPool()

	require.Nil(t, flushedTombstoneStats(nil))
	require.Empty(t, flushedTombstoneStats(&batch.Batch{}),
		"an entry without the stats column enumerates nothing")

	bat := colexec.AllocCNS3ResultBat(true)
	defer bat.CleanOnlyData()
	idA := types.NewObjectid()
	idB := types.NewObjectid()
	statsA := objectio.NewObjectStatsWithObjectID(&idA, false, true, true)
	statsB := objectio.NewObjectStatsWithObjectID(&idB, false, true, true)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], statsA.Marshal(), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], statsB.Marshal(), false, mp))
	bat.SetRowCount(2)

	got := flushedTombstoneStats(bat)
	require.Len(t, got, 2, "every tombstone object of the shared entry is enumerated")
	require.Equal(t, statsA.Marshal(), got[0].Marshal())
	require.Equal(t, statsB.Marshal(), got[1].Marshal())
}

// The P1 flushed-intent consumer member on a REAL flush: the production S3
// tombstone writer persists this transaction's own deletes, and the mask
// producer's reader must read the tombstone objects back with the writer's
// exact (rowid, pk) column order and a valid per-block location — the
// pre-fix defects (nil Location slice for BlockLocationTo, reversed
// column order) silently produced no locations and peeled rows as
// (pk, rowid), which the pre-fix name-mismatched producer could never
// reach to expose.
func TestCollectFlushedMaskIntentsReadsRealTombstoneRows(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx
	mp := proc.GetMPool()

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	source := make(map[types.Rowid]int32)
	for i := 0; i < 100; i++ {
		row := types.RandomRowid()
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		source[row] = int32(i)
	}
	bat.SetRowCount(100)

	fs, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)

	writer := colexec.NewCNS3TombstoneWriterForService(
		"execread-mask-tombstone", mp, fs, types.T_int32.ToType(), -1)
	require.NoError(t, writer.Write(ctx, bat))
	stats, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, stats)

	// The flushed workspace entry keeps the writer's stats column
	// (FillBlockInfoBat's tombstone shape); the P1 producer enumerates
	// exactly those objects instead of a name-keyed registry lookup.
	entryBat, err := writer.FillBlockInfoBat()
	require.NoError(t, err)
	enum := flushedTombstoneStats(entryBat)
	require.Len(t, enum, len(stats))
	for i := range stats {
		require.Equal(t, stats[i].Marshal(), enum[i].Marshal())
	}

	// The consumer reads the flushed (rowid, pk) rows back through the
	// clone's engine filesystem with the production reader.
	op, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	op.AddWorkspace(&Transaction{engine: &Engine{fs: fs}})
	_, tbl := newExecutionReadTestTable(t, op)

	var gotRows []types.Rowid
	var gotPKs []int32
	err = collectFlushedMaskIntents(ctx, tbl, stats, types.T_int32.ToType(),
		func(rowid types.Rowid, pkColumn *vector.Vector, j int64) {
			gotRows = append(gotRows, rowid)
			gotPKs = append(gotPKs, vector.MustFixedColWithTypeCheck[int32](pkColumn)[j])
		},
		func() bool { return false })
	require.NoError(t, err)
	require.Len(t, gotRows, len(source))
	require.Len(t, gotPKs, len(source))
	for i := range gotRows {
		require.Contains(t, source, gotRows[i],
			"every read-back rowid must be one the writer persisted")
		require.Equal(t, source[gotRows[i]], gotPKs[i],
			"the pk column must stay the pk column (writer column order)")
	}
}
