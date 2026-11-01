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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestPersistedPKCheckUnavailableObjectHistory(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		deleted, modified, block bool
		to                       int64
		want                     uint16
	}{
		{"obsolete_metadata", true, false, false, 50, moerr.ErrTxnNeedRetry},
		{"obsolete_block", true, false, true, 50, moerr.ErrTxnNeedRetry},
		{"current_metadata", false, false, false, 50, moerr.ErrFileNotFound},
		{"current_block", false, false, true, 50, moerr.ErrFileNotFound},
		{"deleted_after_snapshot", true, false, false, 30, moerr.ErrFileNotFound},
		{"lock_probe", true, true, false, 50, moerr.ErrFileNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, eng, fs, keys := newPersistedPKCheckTableBlocks(t, types.T_int64, false, 1, tc.deleted)
			ctx := tbl.proc.Load().Ctx
			reads := 0
			eng.fs = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, v *fileservice.IOVector) error {
				if !tc.block || len(pkCheckSemaphore) > 0 {
					reads++
					return moerr.NewFileNotFoundNoCtx(v.FilePath)
				}
				return fs.Read(ctx, v)
			}}
			baseline := tbl.proc.Load().Mp().CurrNB()
			var err error
			if tc.modified {
				_, err = tbl.PrimaryKeysMayBeModified(ctx, types.BuildTS(10, 0), types.BuildTS(tc.to, 0), keys, 0, -1)
			} else {
				_, err = tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(tc.to, 0), keys, 0)
			}
			require.True(t, moerr.IsMoErrCode(err, tc.want), "%v", err)
			require.Positive(t, reads)
			require.Empty(t, pkCheckSemaphore)
			require.Equal(t, baseline, tbl.proc.Load().Mp().CurrNB())
		})
	}
}

func TestPersistedPKCheckHistoryControls(t *testing.T) {
	for _, mode := range []string{"retained", "disjoint", "fatal"} {
		t.Run(mode, func(t *testing.T) {
			tbl, eng, fs, keys := newPersistedPKCheckTableBlocks(t, types.T_int64, false, 1, true)
			ctx := tbl.proc.Load().Ctx
			addPKCheckObjects(t, tbl, eng, fs, 65, "disjoint", "created")
			if mode == "disjoint" {
				require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(99)))
			}
			reads := 0
			sentinel := errors.New("historical metadata transport failed")
			eng.fs = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, v *fileservice.IOVector) error {
				reads++
				if mode == "fatal" {
					return sentinel
				}
				return fs.Read(ctx, v)
			}}
			changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
			if mode == "fatal" {
				require.ErrorIs(t, err, sentinel)
			} else {
				require.NoError(t, err)
				require.Equal(t, mode == "retained", changed)
			}
			if mode == "disjoint" {
				require.Zero(t, reads)
			} else {
				require.Positive(t, reads)
			}
		})
	}
}

func TestPersistedPKCheckUnavailableHistoryWindow(t *testing.T) {
	tbl, eng, _, keys := newPersistedPKCheckTable(t, types.T_int64, false)
	ctx := tbl.proc.Load().Ctx
	require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(99)))
	part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state, done := part.MutateState()
	state.UpdateDuration(types.BuildTS(25, 0), types.MaxTs())
	done()
	changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
	require.True(t, changed)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
	changed, err = tbl.PrimaryKeysMayBeModified(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0, -1)
	require.NoError(t, err)
	require.False(t, changed, "ordinary modification probes still exclude an absent key")
	require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(7)))
	changed, err = tbl.PrimaryKeysMayBeModified(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0, -1)
	require.NoError(t, err)
	require.True(t, changed)
	require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(99)))
	changed, err = tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(10, 0), keys, 0)
	require.NoError(t, err)
	require.False(t, changed)
	tbl.fake = false
	tbl.remoteWorkspace = true
	tbl.createdInTxn = true
	state, done = part.MutateState()
	state.UpdateDuration(types.MaxTs(), types.MaxTs())
	done()
	changed, err = tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
	require.NoError(t, err)
	require.False(t, changed, "transaction-created table has no remote history")
}

func TestAutoIncrementPKCheckEmptyPartition(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	ctx := tbl.proc.Load().Ctx
	keys := makeBatchForTest(tbl.proc.Load().Mp(), 7)
	defer keys.Clean(tbl.proc.Load().Mp())
	part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state, done := part.MutateState()
	// A CN that has not subscribed to a new table has no checkpoint history.
	// MaxTs is the empty-state sentinel, not evidence of reclaimed history.
	state.UpdateDuration(types.MaxTs(), types.MaxTs())
	done()
	changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
	require.NoError(t, err)
	require.False(t, changed, "an empty partition must not discard the first reserved range")
}
