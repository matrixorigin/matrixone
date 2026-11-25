// Copyright 2021 - 2022 Matrix Origin
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
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestScopedChangedTablesAdmissionWaitExit(t *testing.T) {
	for _, test := range []struct {
		name     string
		deadline bool
		release  bool
		want     error
	}{
		{name: "canceled", want: context.Canceled},
		{name: "deadline", deadline: true, want: context.DeadlineExceeded},
		{name: "cancellation coincides with release", release: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var admission scopedChangedTablesAdmission
				require.NoError(t, admission.acquire(t.Context()))
				firstActive := true
				defer func() {
					if firstActive {
						admission.release()
					}
				}()
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx, stop := context.WithTimeoutCause(parent, scopedChangedTablesTimeout, errScopedChangedTablesDeadline)
				defer stop()
				done := make(chan error, 1)
				go func() {
					err := admission.acquire(ctx)
					if err == nil {
						admission.release()
					}
					done <- err
				}()
				synctest.Wait()
				require.Len(t, admission.admitted, 2)
				require.Len(t, admission.active, 1)

				if test.deadline {
					time.Sleep(scopedChangedTablesTimeout) // advances the synctest clock
				} else {
					cancel()
				}
				if test.release {
					admission.release()
					firstActive = false
				}
				require.ErrorIs(t, <-done, test.want)
				if firstActive {
					require.Len(t, admission.admitted, 1)
					admission.release()
					firstActive = false
				}
				require.Empty(t, admission.admitted)
				require.Empty(t, admission.active)
				require.NoError(t, admission.acquire(t.Context()))
				admission.release()
			})
		})
	}
}

func TestScopedChangedTablesConcurrentDisjointScopes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	opts := config.WithLongScanAndCKPOpts(nil)
	fs := &scopedIndexReadFS{FileService: objectio.TmpNewFileservice(ctx, t.TempDir())}
	opts.Fs = fs
	h := mockTAEHandle(ctx, t, opts)
	t.Cleanup(func() { require.NoError(t, h.HandleClose(context.Background())) })
	registerScopedChangedTablesPublisher(t, h.Handle)

	publish := startScopedChangedTablesTxn(t, h.Handle)
	_, err := testutil.CreateDatabase2(ctx, publish, "publish_admission")
	require.NoError(t, err)
	require.NoError(t, publish.Commit(ctx))
	published := publish.GetCommitTS()
	end := published.Prev()
	start := types.BuildTS(end.Physical()-time.Hour.Nanoseconds(), 0)
	locations, err := logtail.MockTableIDBatch(ctx, start, end, 64, 2, h.m, h.db.Runtime.Fs)
	require.NoError(t, err)
	entry := checkpoint.NewCheckpointEntry("", types.TS{}, end, checkpoint.ET_Global)
	entry.SetTableIDLocation(locations)
	original := h.db.BGCheckpointRunner
	h.db.BGCheckpointRunner = &checkpointRunnerWithMax{Runner: original, entry: entry}
	defer func() { h.db.BGCheckpointRunner = original }()
	h.db.LogtailMgr.SetReplayHistoryFloor(end)

	gate := &scopedIndexReadGate{
		path:    locations.Get(0).Name().UnsafeString(),
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	fs.gate.Store(gate)
	defer fs.gate.Store(nil)

	type outcome struct {
		resp cmd_util.GetScopedChangedTablesResp
		err  error
	}
	startScan := func(req cmd_util.GetScopedChangedTablesReq) <-chan outcome {
		done := make(chan outcome, 1)
		go func() {
			var result outcome
			_, result.err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &req, &result.resp)
			done <- result
		}()
		return done
	}
	firstTable := cmd_util.ScopedChangedTable{DatabaseID: 3000, TableID: 1000}
	secondTable := cmd_util.ScopedChangedTable{DatabaseID: 3000, TableID: 1001}
	first := cmd_util.GetScopedChangedTablesReq{
		From: start.Prev().ToTimestamp(), To: published.ToTimestamp(),
		Tables: []cmd_util.ScopedChangedTable{firstTable},
	}
	second := first
	second.Tables = []cmd_util.ScopedChangedTable{secondTable}
	firstDone := startScan(first)
	var secondDone <-chan outcome
	defer func() {
		cancel()
		// Drain any result that was not already collected before restoring the
		// checkpoint runner or closing the test handle.
		if firstDone != nil {
			<-firstDone
		}
		if secondDone != nil {
			<-secondDone
		}
	}()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	secondDone = startScan(second)
	require.Eventually(t, func() bool {
		return len(h.scopedChangedTablesAdmission.admitted) == 2
	}, time.Second, time.Millisecond)
	require.Len(t, h.scopedChangedTablesAdmission.active, 1)

	var resp cmd_util.GetScopedChangedTablesResp
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &first, &resp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrChangeWatchRetry), "%v", err)
	require.Empty(t, resp)

	probe := cmd_util.GetScopedChangedTablesReq{From: first.To, To: first.To}
	_, err = h.HandleGetScopedChangedTables(ctx, txn.TxnMeta{}, &probe, &resp)
	require.NoError(t, err)
	require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &probe, &resp))
	require.Empty(t, resp.Tables)
	require.Len(t, h.scopedChangedTablesAdmission.admitted, 2)

	close(gate.resume)
	a, b := <-firstDone, <-secondDone
	firstDone, secondDone = nil, nil
	require.NoError(t, a.err)
	require.NoError(t, b.err)
	require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &first, &a.resp))
	require.NoError(t, cmd_util.ValidateScopedChangedTablesResponse(ctx, &second, &b.resp))
	require.Equal(t, []cmd_util.ScopedChangedTable{firstTable}, a.resp.Tables)
	require.Equal(t, []cmd_util.ScopedChangedTable{secondTable}, b.resp.Tables)
	require.Empty(t, h.scopedChangedTablesAdmission.admitted)
}
