// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestOptimisticRCWriteDependencies(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "tombstone"}[tombstone], func(t *testing.T) {
			tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, tombstone)
			mp := tbl.proc.Load().Mp()
			keys.Attrs = []string{"pk"}
			for _, typ := range []int{INSERT, DELETE} {
				e := Entry{typ: typ, tableId: tbl.tableId, bat: keys}
				if typ == DELETE {
					e.bat = &batch.Batch{Attrs: []string{"rowid", "pk"}, Vecs: []*vector.Vector{nil, keys.Vecs[0]}}
				}
				require.Error(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(10, 0), types.BuildTS(50, 0)))
				require.NoError(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(50, 0), types.BuildTS(60, 0)))
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				require.ErrorIs(t, validateOptimisticWriteEntry(ctx, tbl, e, types.BuildTS(10, 0), types.BuildTS(50, 0)), context.Canceled)
			}
			// A CN spill has no commit-TS column. Validate its PK column one block
			// at a time against the same committed history as the in-memory entry.
			writer := ioutil.ConstructWriter(0, []uint16{0}, 0, true, false, fs)
			_, err := writer.WriteBatch(keys)
			require.NoError(t, err)
			_, _, err = writer.Sync(t.Context())
			require.NoError(t, err)
			metadata := batch.NewWithSize(2)
			defer metadata.Clean(mp)
			metadata.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			metadata.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			stats := writer.GetObjectStats()
			require.NoError(t, vector.AppendBytes(metadata.Vecs[1], stats.Marshal(), false, mp))
			metadata.SetRowCount(1)
			e := Entry{typ: INSERT, tableId: tbl.tableId, bat: metadata, fileName: "spill"}
			err = validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(10, 0), types.BuildTS(50, 0))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
			require.NoError(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(50, 0), types.BuildTS(60, 0)))

			part := eng.GetOrCreateLatestPart(t.Context(), uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
			state, done := part.MutateState()
			state.UpdateDuration(types.BuildTS(70, 0), types.MaxTs())
			done()
			err = validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(60, 0), types.BuildTS(80, 0))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "truncated dependency history must fail closed")
		})
	}
}

func TestOptimisticRCSpilledDelete(t *testing.T) {
	tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
	mp := tbl.proc.Load().Mp()
	data := batch.NewWithSize(2)
	defer data.Clean(mp)
	data.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	var err error
	data.Vecs[1], err = keys.Vecs[0].Dup(mp)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(data.Vecs[0], types.RandomRowid(), false, mp))
	data.SetRowCount(1)
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	for range 2 {
		_, err = writer.WriteBatch(data)
		require.NoError(t, err)
	}
	_, _, err = writer.Sync(t.Context())
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	require.EqualValues(t, 2, stats.BlkCnt())
	metadata := batch.NewWithSize(1)
	defer metadata.Clean(mp)
	metadata.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(metadata.Vecs[0], stats.Marshal(), false, mp))
	metadata.SetRowCount(1)
	e := Entry{typ: DELETE, tableId: tbl.tableId, bat: metadata, fileName: "cn-tombstone"}
	err = validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(10, 0), types.BuildTS(50, 0))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	var offsets []int64
	eng.fs = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, v *fileservice.IOVector) error {
		if len(v.Entries) == 1 {
			offsets = append(offsets, v.Entries[0].Offset)
		}
		return fs.Read(ctx, v)
	}}
	require.NoError(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(50, 0), types.BuildTS(60, 0)))
	require.GreaterOrEqual(t, len(offsets), 2)
	last := offsets[len(offsets)-1]
	failure := errors.New("second CN tombstone block read failed")
	completed := 0
	baseline := mp.CurrNB()
	acquired, released := 0, 0
	eng.fs = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, v *fileservice.IOVector) error {
		if len(v.Entries) == 1 && v.Entries[0].Offset == last {
			return failure
		}
		err := fs.Read(ctx, v)
		if err == nil {
			completed++
			for i := range v.Entries {
				if v.Entries[i].CachedData != nil {
					acquired++
					v.Entries[i].CachedData = &optimisticRCReleaseData{Data: v.Entries[i].CachedData, released: &released}
				}
			}
		}
		return err
	}}
	require.ErrorIs(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(50, 0), types.BuildTS(60, 0)), failure)
	require.Positive(t, completed, "an earlier block completed before the later read failed")
	require.Positive(t, acquired)
	require.Equal(t, acquired, released, "all earlier block read results must be released exactly once")
	require.Equal(t, baseline, mp.CurrNB(), "earlier block vectors must be released")
	eng.fs = fs
	require.NoError(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(50, 0), types.BuildTS(60, 0)))
	require.Equal(t, baseline, mp.CurrNB())
}

func TestOptimisticRCFailedAdvanceWatermark(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "read-error", true: "cancelled"}[cancelled], func(t *testing.T) {
			enableReenterSnapshotOffsetFault(t)
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			initial := timestamp.Timestamp{PhysicalTime: 10}
			current := initial
			op.EXPECT().Txn().Return(pbtxn.TxnMeta{Mode: pbtxn.TxnMode_Optimistic, Isolation: pbtxn.TxnIsolation_RC}).AnyTimes()
			op.EXPECT().SnapshotTS().DoAndReturn(func() timestamp.Timestamp { return current }).AnyTimes()
			op.EXPECT().UpdateSnapshot(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, timestamp.Timestamp) error {
				current = timestamp.Timestamp{PhysicalTime: 20}
				return nil
			})
			bat := batch.NewWithSize(0)
			bat.SetRowCount(1)
			ws := &Transaction{op: op, engine: &Engine{}, start: time.Now(), writes: []Entry{{typ: INSERT, bat: bat, tableName: "t", databaseName: "db", tableId: 42}}}
			ws.engine.config.cnTransferTxnLifespanThreshold = time.Hour
			ctx, stop := context.WithCancel(t.Context())
			defer stop()
			if cancelled {
				stop()
			}
			ws.Lock()
			err := ws.advanceSnapshot(ctx, timestamp.Timestamp{})
			ws.Unlock()
			require.Error(t, err)
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Equal(t, int64(20), current.PhysicalTime)
			require.Equal(t, initial, ws.optimisticWriteSnapshot)
			// Fresh context and skipTransfer=true isolate the added pre-commit guard.
			require.Error(t, ws.transferTombstonesByCommit(t.Context()))
			require.Equal(t, initial, ws.optimisticWriteSnapshot)
		})
	}
}

// Observe the actual cache-backed block ownership, independently of the PK
// checker's mpool copies. Losing the per-block release makes this oracle fail.
type optimisticRCReleaseData struct {
	fscache.Data
	released *int
}

func (d *optimisticRCReleaseData) Release() { *d.released++; d.Data.Release() }
