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

package disttae

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestUpsertProbeManyChangedObjects(t *testing.T) {
	tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
	proc := tbl.proc.Load()
	part := eng.GetOrCreateLatestPart(proc.Ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	iter, err := part.Snapshot().NewObjectsIter(types.MaxTs(), false, false)
	require.NoError(t, err)
	require.True(t, iter.Next())
	target := iter.Entry().ObjectStats
	require.NoError(t, iter.Close())

	state := logtailreplay.NewPartitionState("", false, 0, false)
	zm := index.NewZM(types.T_int64, 0)
	other := int64(1)
	index.UpdateZM(zm, types.EncodeInt64(&other))
	for i := 0; i < 65; i++ {
		stats := target
		require.NoError(t, objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectName(objectio.NewSegmentid(), uint16(i))))
		require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&stats, zm))
		require.NoError(t, state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
			ObjectStats: stats, CreateTime: types.BuildTS(int64(11+i), 0),
		}, false))
		if i != 63 && i != 64 {
			continue
		}
		baseline := proc.Mp().CurrNB()
		changed, err := tbl.PKPersistedBetween(proc.Ctx, state, types.BuildTS(10, 0), types.BuildTS(50, 0), keys.Vecs[0], false)
		require.NoError(t, err)
		require.False(t, changed, "unrelated object summaries cannot require a retry, count=%d", i+1)
		require.Equal(t, baseline, proc.Mp().CurrNB())
		require.Empty(t, pkCheckSemaphore)
	}
	// Modified keeps its conservative object-count policy.
	changed, err := tbl.PKPersistedBetween(proc.Ctx, state, types.BuildTS(10, 0), types.BuildTS(50, 0), keys.Vecs[0], true)
	require.NoError(t, err)
	require.True(t, changed)

	// The matching object follows every unrelated event in the time index.
	require.NoError(t, state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
		ObjectStats: target, CreateTime: types.BuildTS(100, 0),
	}, false))
	changed, err = tbl.PKPersistedBetween(proc.Ctx, state, types.BuildTS(10, 0), types.BuildTS(50, 0), keys.Vecs[0], false)
	require.NoError(t, err)
	require.True(t, changed)
	require.Empty(t, pkCheckSemaphore)
}

func TestUpsertProbeCancellationAndReadError(t *testing.T) {
	tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
	proc := tbl.proc.Load()
	part := eng.GetOrCreateLatestPart(proc.Ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state := part.Snapshot()
	from, to := types.BuildTS(10, 0), types.BuildTS(50, 0)
	check := func(ctx context.Context) (bool, error) {
		return tbl.PKPersistedBetween(ctx, state, from, to, keys.Vecs[0], false)
	}
	baseline := proc.Mp().CurrNB()

	t.Run("cancelled-before-entry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(proc.Ctx)
		cancel()
		changed, err := check(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, changed)
		require.Empty(t, pkCheckSemaphore)
	})

	t.Run("cancelled-during-admission", func(t *testing.T) {
		guard, stop := context.WithTimeout(proc.Ctx, 10*time.Second)
		defer stop()
		ctx, cancel := context.WithCancel(guard)
		entered := make(chan struct{})
		for i := 0; i < cap(pkCheckSemaphore); i++ {
			pkCheckSemaphore <- struct{}{}
		}
		var once sync.Once
		releaseHeld := func() {
			once.Do(func() {
				for i := 0; i < cap(pkCheckSemaphore); i++ {
					releasePKCheckSemaphore()
				}
			})
		}
		defer releaseHeld()
		done := make(chan error, 1)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			_, err := check(&tombstoneWaitContext{Context: ctx, entered: entered})
			done <- err
		}()
		defer func() { cancel(); releaseHeld(); <-finished }()
		select {
		case <-entered:
		case <-guard.Done():
			t.Fatal("probe did not reach admission", guard.Err())
		}
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-guard.Done():
			t.Fatal("cancelled admission did not finish", guard.Err())
		}
		require.Len(t, pkCheckSemaphore, cap(pkCheckSemaphore))
	})

	t.Run("cancelled-during-metadata-read", func(t *testing.T) {
		iter, err := state.NewObjectsIter(types.MaxTs(), false, false)
		require.NoError(t, err)
		require.True(t, iter.Next())
		stats := iter.Entry().ObjectStats
		require.NoError(t, iter.Close())
		// A unique name guarantees this metadata is not already cached. The
		// callback ends the read before a backing file is needed.
		require.NoError(t, objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectName(objectio.NewSegmentid(), 0)))
		require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&stats, index.NewZM(types.T_int64, 0)))
		uncached := logtailreplay.NewPartitionState("", false, 0, false)
		require.NoError(t, uncached.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
			ObjectStats: stats, CreateTime: types.BuildTS(40, 0),
		}, false))
		ctx, cancel := context.WithCancel(proc.Ctx)
		defer cancel()
		called, heldPermit := false, false
		eng.fs = tombstoneReadFS{FileService: fs, read: func(ioCtx context.Context, _ *fileservice.IOVector) error {
			called = true
			heldPermit = len(pkCheckSemaphore) > 0
			cancel()
			return ioCtx.Err()
		}}
		defer func() { eng.fs = fs }()
		changed, err := tbl.PKPersistedBetween(ctx, uncached, from, to, keys.Vecs[0], false)
		require.True(t, called, "metadata read must be reached")
		require.True(t, heldPermit, "metadata read must be admitted")
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, changed)
		require.Empty(t, pkCheckSemaphore)
	})

	t.Run("read-error-wins-over-later-cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(proc.Ctx)
		defer cancel()
		failure := errors.New("injected block read failure")
		called := false
		eng.fs = tombstoneReadFS{FileService: fs, read: func(ioCtx context.Context, v *fileservice.IOVector) error {
			if len(v.Entries) > 1 {
				called = true
				cancel()
				return failure
			}
			return fs.Read(ioCtx, v)
		}}
		defer func() { eng.fs = fs }()
		changed, err := check(ctx)
		require.True(t, called, "the actual block read must be reached")
		require.ErrorIs(t, err, failure)
		require.False(t, changed)
		require.Empty(t, pkCheckSemaphore)
	})

	changed, err := check(proc.Ctx)
	require.NoError(t, err)
	require.True(t, changed, "the probe remains usable after cancellation and failure")
	require.Equal(t, baseline, proc.Mp().CurrNB())
	require.Empty(t, pkCheckSemaphore)
}

func TestUpsertProbeManyCandidateBlocks(t *testing.T) {
	tbl, _, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
	proc, mp := tbl.proc.Load(), tbl.proc.Load().Mp()
	data := batch.NewWithSize(3)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	data.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	data.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	defer data.Clean(mp)
	writer := ioutil.ConstructWriter(0, []uint16{0, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}, 0, true, false, fs)
	for i := 0; i < 34; i++ {
		commit := types.BuildTS(5, 0)
		if i >= 32 {
			commit = types.BuildTS(20, 0)
		}
		require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(7), false, mp))
		require.NoError(t, vector.AppendFixed(data.Vecs[1], commit, false, mp))
		require.NoError(t, vector.AppendFixed(data.Vecs[2], i == 32, false, mp))
		data.SetRowCount(1)
		_, err := writer.WriteBatch(data)
		require.NoError(t, err)
		data.CleanOnlyData()
	}
	_, _, err := writer.Sync(proc.Ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	require.EqualValues(t, 34, stats.BlkCnt())
	for _, count := range []uint32{32, 33, 34} {
		state := logtailreplay.NewPartitionState("", false, 0, false)
		selected := stats
		require.NoError(t, objectio.SetObjectStatsBlkCnt(&selected, count))
		require.NoError(t, objectio.SetObjectStatsRowCnt(&selected, count))
		require.NoError(t, state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
			ObjectStats: selected, CreateTime: types.BuildTS(40, 0),
		}, false))
		for _, to := range []int64{15, 20} {
			baseline := mp.CurrNB()
			changed, err := tbl.PKPersistedBetween(proc.Ctx, state, types.BuildTS(10, 0), types.BuildTS(to, 0), keys.Vecs[0], false)
			require.NoError(t, err)
			require.Equal(t, count == 34 && to == 20, changed, "blocks=%d to=%d", count, to)
			require.Equal(t, baseline, mp.CurrNB())
			require.Empty(t, pkCheckSemaphore)
		}
	}
}

func TestUpsertProbeSortsOwnedKeys(t *testing.T) {
	tbl, _, _, _ := newPersistedPKCheckTable(t, types.T_int64, false)
	proc, mp := tbl.proc.Load(), tbl.proc.Load().Mp()
	keys := batch.NewWithSize(1)
	keys.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer keys.Clean(mp)
	want := []int64{100, 7, 1, 50}
	for _, key := range want {
		require.NoError(t, vector.AppendFixed(keys.Vecs[0], key, false, mp))
	}
	keys.SetRowCount(len(want))
	baseline := mp.CurrNB()
	changed, err := tbl.PrimaryKeysMayBeUpserted(proc.Ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, want, vector.MustFixedColWithTypeCheck[int64](keys.Vecs[0]))
	require.Equal(t, baseline, mp.CurrNB())
	require.Empty(t, pkCheckSemaphore)
}

func TestUpsertProbeMissingZoneMapsAndDeletedObject(t *testing.T) {
	tbl, _, fs, _ := newPersistedPKCheckTable(t, types.T_int64, false)
	proc, mp := tbl.proc.Load(), tbl.proc.Load().Mp()
	// A dropped/reordered column can make the physical sequence differ from
	// the current table-definition ordinal.
	tbl.tableDef.Cols[0].Seqnum = 5
	data := batch.NewWithSize(4)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	data.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	data.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	data.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	defer data.Clean(mp)
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(7), false, mp))
	require.NoError(t, vector.AppendFixed(data.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(data.Vecs[2], types.BuildTS(20, 0), false, mp))
	require.NoError(t, vector.AppendFixed(data.Vecs[3], false, false, mp))
	data.SetRowCount(1)
	writer := ioutil.ConstructWriter(0, []uint16{5, 0, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}, 0, true, false, fs)
	block, err := writer.WriteBatch(data)
	require.NoError(t, err)
	// Clear the block summary before serialization while retaining its Bloom
	// filter. A >3-key probe must use the full BF range when this map is absent.
	block.MustGetColumn(5).SetZoneMap(index.NewZM(types.T_int64, 0))
	_, _, err = writer.Sync(proc.Ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(proc.Ctx, &location, false, fs)
	require.NoError(t, err)
	dataMeta := meta.MustDataMeta()
	require.Greater(t, dataMeta.BlockHeader().BFExtent().Length(), uint32(0))
	require.True(t, dataMeta.MustGetColumn(0).ZoneMap().IsInited(), "the unrelated ordinal-0 summary must exist")
	require.Equal(t, int64(1), dataMeta.MustGetColumn(0).ZoneMap().GetMin())
	require.Equal(t, int64(1), dataMeta.MustGetColumn(0).ZoneMap().GetMax())
	require.False(t, dataMeta.GetBlockMeta(0).MustGetColumn(5).ZoneMap().IsInited(), "the persisted block ZM must actually be absent")
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&stats, index.NewZM(types.T_int64, 0)))
	state := logtailreplay.NewPartitionState("", false, 0, false)
	require.NoError(t, state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
		ObjectStats: stats, CreateTime: types.BuildTS(5, 0), DeleteTime: types.BuildTS(40, 0),
	}, false))
	keys := vector.NewVec(types.T_int64.ToType())
	defer keys.Free(mp)
	for _, key := range []int64{100, 7, 2, 50} {
		require.NoError(t, vector.AppendFixed(keys, key, false, mp))
	}
	baseline := mp.CurrNB()
	for _, to := range []int64{20, 50} {
		changed, err := tbl.PKPersistedBetween(proc.Ctx, state, types.BuildTS(10, 0), types.BuildTS(to, 0), keys, false)
		require.NoError(t, err)
		require.True(t, changed, "a deleted historical object with a real hit must be checked, to=%d", to)
		require.Equal(t, baseline, mp.CurrNB())
		require.Empty(t, pkCheckSemaphore)
	}
}

func TestUpsertProbeMissingCommitTimestampRequiresAKeyMatch(t *testing.T) {
	tbl, _, fs, _ := newPersistedPKCheckTable(t, types.T_int64, false)
	proc, mp := tbl.proc.Load(), tbl.proc.Load().Mp()
	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer data.Clean(mp)
	for _, key := range []int64{1, 9} {
		require.NoError(t, vector.AppendFixed(data.Vecs[0], key, false, mp))
	}
	data.SetRowCount(2)
	// This object has no commit-TS column or Bloom filter. A key between the
	// zone-map endpoints therefore reaches exact search without matching.
	writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
	_, err := writer.WriteBatch(data)
	require.NoError(t, err)
	_, _, err = writer.Sync(proc.Ctx)
	require.NoError(t, err)
	state := logtailreplay.NewPartitionState("", false, 0, false)
	require.NoError(t, state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
		ObjectStats: writer.GetObjectStats(), CreateTime: types.BuildTS(40, 0),
	}, false))
	keys := vector.NewVec(types.T_int64.ToType())
	defer keys.Free(mp)
	for _, tc := range []struct {
		key     int64
		changed bool
	}{{7, false}, {1, true}} {
		keys.CleanOnlyData()
		require.NoError(t, vector.AppendFixed(keys, tc.key, false, mp))
		baseline := mp.CurrNB()
		changed, err := tbl.PKPersistedBetween(proc.Ctx, state, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, false)
		require.NoError(t, err)
		require.Equal(t, tc.changed, changed, "key=%d", tc.key)
		require.Equal(t, baseline, mp.CurrNB())
		require.Empty(t, pkCheckSemaphore)
	}
}

func BenchmarkUpsertProbe(b *testing.B) {
	for _, tc := range []struct {
		name    string
		target  bool
		to      int64
		changed bool
	}{
		{"65-unrelated-objects", false, 50, false},
		{"65-unrelated-then-old-match", true, 15, false},
		{"65-unrelated-then-live-match", true, 50, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			tbl, eng, fs, keys := newPersistedPKCheckTable(b, types.T_int64, false)
			proc := tbl.proc.Load()
			part := eng.GetOrCreateLatestPart(proc.Ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
			iter, err := part.Snapshot().NewObjectsIter(types.MaxTs(), false, false)
			require.NoError(b, err)
			require.True(b, iter.Next())
			target := iter.Entry().ObjectStats
			require.NoError(b, iter.Close())
			state := logtailreplay.NewPartitionState("", false, 0, false)
			zm := index.NewZM(types.T_int64, 0)
			other := int64(1)
			index.UpdateZM(zm, types.EncodeInt64(&other))
			for i := 0; i < 65; i++ {
				stats := target
				require.NoError(b, objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectName(objectio.NewSegmentid(), uint16(i))))
				require.NoError(b, objectio.SetObjectStatsSortKeyZoneMap(&stats, zm))
				require.NoError(b, state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
					ObjectStats: stats, CreateTime: types.BuildTS(int64(11+i), 0),
				}, false))
			}
			if tc.target {
				require.NoError(b, state.HandleObjectEntry(proc.Ctx, fs, objectio.ObjectEntry{
					ObjectStats: target, CreateTime: types.BuildTS(100, 0),
				}, false))
			}
			var reads uint64
			eng.fs = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, v *fileservice.IOVector) error {
				reads++
				return fs.Read(ctx, v)
			}}
			defer func() { eng.fs = fs }()
			check := func() (bool, error) {
				return tbl.PKPersistedBetween(proc.Ctx, state, types.BuildTS(10, 0), types.BuildTS(tc.to, 0), keys.Vecs[0], false)
			}
			// Warm metadata/cache separately; the benchmark measures steady-state
			// read requests and allocations rather than fixture construction.
			changed, err := check()
			require.NoError(b, err)
			require.Equal(b, tc.changed, changed)
			reads = 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				changed, err = check()
				if err != nil || changed != tc.changed {
					b.Fatalf("changed=%t err=%v", changed, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(reads)/float64(b.N), "fs-reads/op")
		})
	}
}
