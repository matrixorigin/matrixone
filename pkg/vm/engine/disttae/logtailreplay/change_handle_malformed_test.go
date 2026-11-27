// Copyright 2026 Matrix Origin
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

package logtailreplay

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestChangeRangeMergeFailureClosesInputAndOutputSpills(t *testing.T) {
	type mergeFn func(context.Context, *rangeSpillFile, []rangeSpillRun,
		engine.ChangeRangeSpillConfig, *mpool.MPool) (*rangeSpillFile, []rangeSpillRun, error)
	for _, strategy := range []struct {
		name  string
		merge mergeFn
	}{
		{name: "commit time", merge: func(ctx context.Context, input *rangeSpillFile, runs []rangeSpillRun,
			config engine.ChangeRangeSpillConfig, mp *mpool.MPool) (*rangeSpillFile, []rangeSpillRun, error) {
			return mergeRangeSpillRuns(ctx, input, runs, config, mp, 1, 0)
		}},
		{name: "primary key", merge: func(ctx context.Context, input *rangeSpillFile, runs []rangeSpillRun,
			config engine.ChangeRangeSpillConfig, mp *mpool.MPool) (*rangeSpillFile, []rangeSpillRun, error) {
			return mergeRangeSpillRunsByPrimaryKey(ctx, input, runs, config, 0, mp, 1, 0)
		}},
	} {
		for _, failure := range []string{"cancel", "disk quota"} {
			t.Run(strategy.name+"/"+failure, func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				inputConfig, inputTracker := newTestChangeRangeSpillConfig(t)
				input, err := newRangeSpillFile(context.Background(), inputConfig)
				require.NoError(t, err)
				for i := 0; i < 2; i++ {
					b := batch.NewWithSize(2)
					b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					b.Vecs[1] = vector.NewVec(types.T_TS.ToType())
					require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(2-i), false, mp))
					require.NoError(t, vector.AppendFixed(b.Vecs[1], types.BuildTS(int64(2-i), 0), false, mp))
					b.SetRowCount(1)
					require.NoError(t, input.Append(b))
					b.Clean(mp)
				}
				runs := []rangeSpillRun{{firstRecord: 0, endRecord: 1}, {firstRecord: 1, endRecord: 2}}
				outputConfig, outputTracker := newTestChangeRangeSpillConfig(t)
				ctx := context.Background()
				var want error
				if failure == "cancel" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
					want = context.Canceled
				} else {
					want = errors.New("spill disk quota exhausted")
					reserveDisk := outputConfig.ReserveDisk
					outputConfig.ReserveDisk = func(size uint64) (engine.ChangeRangeGrowingSpillReservation, error) {
						reservation, err := reserveDisk(size)
						reservation.(*testChangeRangeSpillReservation).growErr = want
						return reservation, err
					}
				}
				merged, _, err := strategy.merge(ctx, input, runs, outputConfig, mp)
				require.Nil(t, merged)
				require.ErrorIs(t, err, want)
				for _, tracker := range []*testChangeRangeSpillTracker{inputTracker, outputTracker} {
					require.Len(t, tracker.diskReservations, 1)
					require.True(t, tracker.diskReservations[0].released)
					require.True(t, tracker.fileReservations[0].released)
				}
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func TestCNObjectHandleDiscardsEmptyAndRejectedCachedBatches(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*mpool.MPool) *batch.Batch
		want  string
	}{
		{name: "missing data vector", build: func(*mpool.MPool) *batch.Batch {
			return batch.NewWithSize(0)
		}, want: "no vectors"},
		{name: "empty data window", build: func(*mpool.MPool) *batch.Batch {
			b := batch.NewWithSize(1)
			b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			return b
		}, want: "EOF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			b := tc.build(mp)
			h := &CNObjectHandle{
				mp: mp, cache: []*batch.Batch{b}, TSs: []types.TS{types.BuildTS(10, 0)},
				base: &baseHandle{changesHandle: &ChangeHandler{}},
			}
			var output *batch.Batch
			err := h.Next(context.Background(), &output, mp)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, output)
			require.Empty(t, h.cache)
			h.Close()
			require.Zero(t, mp.CurrNB())
		})
	}

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(7), false, mp))
	source.SetRowCount(1)
	h := &CNObjectHandle{
		mp: mp, cache: []*batch.Batch{source}, prepared: []bool{true},
		TSs:  []types.TS{types.BuildTS(10, 0)},
		base: &baseHandle{changesHandle: &ChangeHandler{}},
	}
	output := batch.NewWithSize(1)
	output.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	err := h.Next(context.Background(), &output, mp)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	require.Len(t, h.cache, 1, "schema boundary leaves the source for the next output")
	output.Clean(mp)
	h.Close()
	require.Zero(t, mp.CurrNB())
}

func TestChangeRangeSpillRejectsCorruptRecordsAndReleasesReservations(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	config, tracker := newTestChangeRangeSpillConfig(t)
	file, err := newRangeSpillFile(context.Background(), config)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, file.Close())
		tracker.requireReleased(t, false)
		require.Zero(t, mp.CurrNB())
	}()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(7), false, mp))
	input.SetRowCount(1)
	require.NoError(t, file.Append(input))
	input.Clean(mp)

	read, err := file.Read(0, mp)
	require.NoError(t, err)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](read.Vecs[0], 0))
	read.Clean(mp)
	_, err = file.Read(1, mp)
	require.ErrorContains(t, err, "invalid change range spill record")

	record := file.records[0]
	file.records[0].rows++
	_, err = file.Read(0, mp)
	require.ErrorContains(t, err, "row count mismatch")
	file.records[0] = record
	file.records[0].size = -1
	_, err = file.Read(0, mp)
	require.ErrorContains(t, err, "invalid change range spill record size")
	file.records[0] = record

	// A complete but malformed payload must be rejected after the disk read.
	_, err = file.file.WriteAt(make([]byte, record.size), record.offset)
	require.NoError(t, err)
	_, err = file.Read(0, mp)
	require.Error(t, err)
	require.Zero(t, mp.CurrNB())

	require.NoError(t, file.file.Truncate(record.offset))
	_, err = file.Read(0, mp)
	require.ErrorIs(t, err, io.EOF)
}

func TestPersistedDataReplayRejectsMalformedSpecialColumns(t *testing.T) {
	start, end := types.BuildTS(10, 0), types.BuildTS(20, 0)
	for _, tc := range []struct {
		name   string
		change func(*batch.Batch, *objectio.SpecialColumnLayout, *mpool.MPool)
		seqs   []uint16
		retain bool
		want   string
	}{
		{name: "sequence count", seqs: []uint16{0}, want: "sequence metadata mismatch"},
		{name: "missing commit", change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.CommitTS = objectio.InvalidSpecialColumnPosition
		}, want: "no valid commit-ts"},
		{name: "commit type", change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.CommitTS = 0
		}, want: "no valid commit-ts"},
		{name: "abort type", change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.Abort = 0
		}, want: "invalid abort"},
		{name: "rowid type", change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.PhysicalAddr = 0
		}, want: "invalid rowid"},
		{name: "null commit vector", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, mp *mpool.MPool) {
			b.Vecs[1].Free(mp)
			b.Vecs[1] = vector.NewConstNull(types.T_TS.ToType(), 1, mp)
		}, want: "commit-ts column is null"},
		{name: "null abort vector", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, mp *mpool.MPool) {
			b.Vecs[2].Free(mp)
			b.Vecs[2] = vector.NewConstNull(types.T_bool.ToType(), 1, mp)
		}, want: "abort column is null"},
		{name: "short abort vector", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, mp *mpool.MPool) {
			b.Vecs[2].Free(mp)
			b.Vecs[2] = vector.NewVec(types.T_bool.ToType())
		}, want: "abort column length mismatch"},
		{name: "null commit row", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			b.Vecs[1].SetNull(0)
		}, want: "special column contains null"},
		{name: "null abort row", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			b.Vecs[2].SetNull(0)
		}, want: "special column contains null"},
		{name: "rowid synthesis without block", retain: true, want: "cannot synthesize rowid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			b := batch.NewWithSize(3)
			b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			b.Vecs[1] = vector.NewVec(types.T_TS.ToType())
			b.Vecs[2] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(7), false, mp))
			require.NoError(t, vector.AppendFixed(b.Vecs[1], start, false, mp))
			require.NoError(t, vector.AppendFixed(b.Vecs[2], false, false, mp))
			b.SetRowCount(1)
			layout := objectio.SpecialColumnLayout{PhysicalAddr: objectio.InvalidSpecialColumnPosition,
				CommitTS: 1, Abort: 2}
			if tc.change != nil {
				tc.change(b, &layout, mp)
			}
			err := updatePersistedDataBatch(b, start, end, nil, layout, tc.seqs, tc.retain, mp)
			require.ErrorContains(t, err, tc.want)
			b.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestPersistedTombstoneReplayRejectsMalformedSpecialColumns(t *testing.T) {
	start, end := types.BuildTS(10, 0), types.BuildTS(20, 0)
	for _, tc := range []struct {
		name   string
		change func(*batch.Batch, *objectio.SpecialColumnLayout, *mpool.MPool)
		seqs   []uint16
		want   string
	}{
		{name: "sequence count", seqs: []uint16{0}, want: "sequence metadata mismatch"},
		{name: "missing commit", change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.CommitTS = objectio.InvalidSpecialColumnPosition
		}, want: "no valid commit-ts"},
		{name: "commit type", change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.CommitTS = 1
		}, want: "no valid commit-ts"},
		{name: "abort type", change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.Abort = 1
		}, want: "invalid abort"},
		{name: "missing key", seqs: []uint16{3, 4, 2, 5}, change: func(_ *batch.Batch, l *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			l.Abort = objectio.InvalidSpecialColumnPosition
		}, want: "invalid key columns"},
		{name: "rowid type", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, mp *mpool.MPool) {
			b.Vecs[0].Free(mp)
			b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		}, want: "invalid persisted tombstone special columns"},
		{name: "null commit vector", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, mp *mpool.MPool) {
			b.Vecs[2].Free(mp)
			b.Vecs[2] = vector.NewConstNull(types.T_TS.ToType(), 1, mp)
		}, want: "invalid persisted tombstone special columns"},
		{name: "null abort vector", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, mp *mpool.MPool) {
			b.Vecs[3].Free(mp)
			b.Vecs[3] = vector.NewConstNull(types.T_bool.ToType(), 1, mp)
		}, want: "abort column is null"},
		{name: "short abort vector", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, mp *mpool.MPool) {
			b.Vecs[3].Free(mp)
			b.Vecs[3] = vector.NewVec(types.T_bool.ToType())
		}, want: "abort column length mismatch"},
		{name: "null commit row", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			b.Vecs[2].SetNull(0)
		}, want: "special column contains null"},
		{name: "null abort row", change: func(b *batch.Batch, _ *objectio.SpecialColumnLayout, _ *mpool.MPool) {
			b.Vecs[3].SetNull(0)
		}, want: "special column contains null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			b := batch.NewWithSize(4)
			b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			b.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			b.Vecs[2] = vector.NewVec(types.T_TS.ToType())
			b.Vecs[3] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[0], types.Rowid{}, false, mp))
			require.NoError(t, vector.AppendFixed(b.Vecs[1], int64(7), false, mp))
			require.NoError(t, vector.AppendFixed(b.Vecs[2], start, false, mp))
			require.NoError(t, vector.AppendFixed(b.Vecs[3], false, false, mp))
			b.SetRowCount(1)
			layout := objectio.SpecialColumnLayout{PhysicalAddr: 0, CommitTS: 2, Abort: 3}
			if tc.change != nil {
				tc.change(b, &layout, mp)
			}
			err := updatePersistedTombstoneBatch(b, start, end, nil, false, layout, tc.seqs, false, mp)
			require.ErrorContains(t, err, tc.want)
			b.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}
