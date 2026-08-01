// Copyright 2021 - 2024 Matrix Origin
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

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

// TestHandleTTLChecker exercises the disk-cleaner TTL checker predicate built
// by HandleDiskCleaner: a checkpoint within the TTL window is protected
// (returns false), an older one is consumable (returns true). The cutoff is
// derived from the engine's HLC clock.
func TestHandleTTLChecker(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	chk := h.ttlChecker(time.Hour)

	// endTS within the TTL window (just now) -> protected.
	recent := checkpoint.NewCheckpointEntry("", types.TS{}, h.db.TxnMgr.Now(), checkpoint.ET_Incremental)
	require.False(t, chk(recent))

	// endTS far older than the TTL cutoff -> consumable.
	old := checkpoint.NewCheckpointEntry("", types.TS{}, types.BuildTS(1, 0), checkpoint.ET_Incremental)
	require.True(t, chk(old))
}

func TestHandleBackup(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	// The success path must not encode a machine-speed assumption as the
	// request's flush deadline. The outer context bounds the test itself while
	// HandleBackup remains responsible for completing all checkpoint phases.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req := &cmd_util.Checkpoint{}
	resp := &api.SyncLogTailResp{}

	cb, err := h.HandleBackup(ctx, txn.TxnMeta{}, req, resp)
	require.NoError(t, err)
	require.NotEmpty(t, resp.CkpLocation)
	if cb != nil {
		cb()
	}
}

func TestContextForBackupCheckpoint(t *testing.T) {
	t.Run("inherit caller cancellation without an explicit timeout", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		ctx, cancel := contextForBackupCheckpoint(parent, 0)
		defer cancel()

		_, hasDeadline := ctx.Deadline()
		require.False(t, hasDeadline)

		cancelParent()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("honor an explicit timeout", func(t *testing.T) {
		ctx, cancel := contextForBackupCheckpoint(context.Background(), time.Minute)
		defer cancel()

		deadline, hasDeadline := ctx.Deadline()
		require.True(t, hasDeadline)
		require.WithinDuration(t, time.Now().Add(time.Minute), deadline, time.Second)
	})
}

func TestTryGetChangedListFromTableIDBatchReadsCompleteIndex(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})
	defer func() { require.NoError(t, h.HandleClose(context.Background())) }()

	end := h.db.TxnMgr.Now()
	start := types.BuildTS(end.Physical()-time.Hour.Nanoseconds(), 0)
	rowCount := logtail.BatchRowCountThreshold + 17
	locations, err := logtail.MockTableIDBatch(
		context.Background(),
		start,
		end,
		64,
		rowCount,
		h.m,
		h.db.Runtime.Fs,
	)
	require.NoError(t, err)

	historyStart, historyEnd, historyKnown, err := logtail.ReadTableIDHistoryRange(
		context.Background(), locations, h.m, h.db.Runtime.Fs,
	)
	require.NoError(t, err)
	require.True(t, historyKnown)
	require.Equal(t, start, historyStart)
	require.Equal(t, end, historyEnd)

	acceptAll := func([]uint64, uint64, types.TS, types.TS) bool { return true }
	_, _, _, _, ok := tryGetChangedListFromTableIDBatch(
		context.Background(), start.Prev(), end, locations, h.Handle, acceptAll,
	)
	require.False(t, ok, "an index must not be trusted before its declared history range")
	_, _, _, _, ok = tryGetChangedListFromTableIDBatch(
		context.Background(), start, end.Next(), locations, h.Handle, acceptAll,
	)
	require.False(t, ok, "an index must not be trusted past its declared history range")

	accIDs, dbIDs, tableIDs, oldest, ok := tryGetChangedListFromTableIDBatch(
		context.Background(), start, end, locations, h.Handle, acceptAll,
	)
	require.True(t, ok)
	require.Equal(t, start, oldest)
	require.Len(t, accIDs, rowCount)
	require.Len(t, dbIDs, rowCount)
	require.Len(t, tableIDs, rowCount)
	require.Equal(t, uint64(1000), tableIDs[0])
	require.Equal(t, uint64(1000+rowCount-1), tableIDs[rowCount-1])
}

func TestHandleDiskCleaner_AddCheckerTTL(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	resp := &api.TNStringResponse{}

	// valid ttl
	req := &cmd_util.DiskCleaner{
		Op:    cmd_util.AddChecker,
		Key:   cmd_util.CheckerKeyTTL,
		Value: "2h",
	}
	cb, err := h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.NoError(t, err)
	if cb != nil {
		cb()
	}

	// invalid ttl
	req.Value = "invalid"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)

	// less than 1 hour
	req.Value = "30m"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)
}

func TestHandleDiskCleaner_AddCheckerMinTS(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	resp := &api.TNStringResponse{}

	// valid minTS
	req := &cmd_util.DiskCleaner{
		Op:    cmd_util.AddChecker,
		Key:   cmd_util.CheckerKeyMinTS,
		Value: "1234567890-1",
	}
	cb, err := h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.NoError(t, err)
	if cb != nil {
		cb()
	}

	// invalid format
	req.Value = "1234567890"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)

	// invalid time
	req.Value = "invalid-1"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)

	// invalid logic time
	req.Value = "1234567890-invalid"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)
}
