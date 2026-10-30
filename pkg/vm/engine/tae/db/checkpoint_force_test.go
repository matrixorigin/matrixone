// Copyright 2021 Matrix Origin
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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestForceGCKPRetryBackoffIsBoundedAndCancelable(t *testing.T) {
	previous := time.Duration(0)
	for retry := 1; retry < 32; retry++ {
		delay := forceGCKPRetryDelay(retry)
		require.Greater(t, delay, time.Duration(0))
		require.GreaterOrEqual(t, delay, previous)
		require.LessOrEqual(t, delay, forceGCKPRetryMaximumDelay)
		previous = delay
	}
	require.Equal(t, forceGCKPRetryMaximumDelay, previous)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := waitForceGCKPRetry(ctx, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(started), time.Second)
}

// The embedded interfaces make unexpected consumer calls fail instead of
// providing a second implementation of the checkpoint machinery.
type checkpointFlushSpy struct {
	checkpoint.Flusher
	results []error
	calls   int
}

func (s *checkpointFlushSpy) ForceFlush(context.Context, types.TS) error {
	result := s.results[s.calls]
	s.calls++
	return result
}

type checkpointRunnerSpy struct {
	checkpoint.Runner
	entry        *checkpoint.CheckpointEntry
	ickpCalls    int
	gckpCalls    int
	maxCalls     int
	specialCalls int
	specialStart types.TS
	specialEnd   types.TS
}

func (s *checkpointRunnerSpy) ForceICKP(context.Context, *types.TS) error {
	s.ickpCalls++
	return nil
}

func (s *checkpointRunnerSpy) ForceGCKP(context.Context, types.TS, time.Duration) error {
	s.gckpCalls++
	return nil
}

func (s *checkpointRunnerSpy) MaxCheckpoint() *checkpoint.CheckpointEntry {
	s.maxCalls++
	return s.entry
}

func (s *checkpointRunnerSpy) CreateSpecialCheckpointFile(_ context.Context, start, end types.TS) (string, error) {
	s.specialCalls++
	s.specialStart, s.specialEnd = start, end
	return "backup-location", nil
}

func TestCheckpointStopsAfterFlushFailure(t *testing.T) {
	cause := moerr.NewInternalErrorNoCtx("force flush failed: closed")
	ts := types.BuildTS(100, 0)

	t.Run("incremental", func(t *testing.T) {
		flusher := &checkpointFlushSpy{results: []error{cause}}
		runner := &checkpointRunnerSpy{}
		db := &DB{BGFlusher: flusher, BGCheckpointRunner: runner}

		require.ErrorIs(t, db.ForceCheckpoint(t.Context(), ts), cause)
		require.Equal(t, 1, flusher.calls)
		require.Zero(t, runner.ickpCalls)
	})

	t.Run("global", func(t *testing.T) {
		flusher := &checkpointFlushSpy{results: []error{cause}}
		runner := &checkpointRunnerSpy{}
		db := &DB{BGFlusher: flusher, BGCheckpointRunner: runner}

		require.ErrorIs(t, db.ForceGlobalCheckpoint(t.Context(), ts, 0), cause)
		require.Equal(t, 1, flusher.calls)
		require.Zero(t, runner.gckpCalls)
	})

	t.Run("backup_first_flush", func(t *testing.T) {
		flusher := &checkpointFlushSpy{results: []error{cause}}
		runner := &checkpointRunnerSpy{}
		db := &DB{BGFlusher: flusher, BGCheckpointRunner: runner}

		location, err := db.ForceCheckpointForBackup(t.Context(), ts)
		require.ErrorIs(t, err, cause)
		require.Empty(t, location)
		require.Equal(t, 1, flusher.calls)
		require.Zero(t, runner.ickpCalls)
		require.Zero(t, runner.maxCalls)
		require.Zero(t, runner.specialCalls)
	})

	for _, secondFlushFails := range []bool{true, false} {
		name := "backup_success"
		if secondFlushFails {
			name = "backup_second_flush"
		}
		t.Run(name, func(t *testing.T) {
			var secondError error
			if secondFlushFails {
				secondError = cause
			}
			flusher := &checkpointFlushSpy{results: []error{nil, secondError}}
			entry := checkpoint.NewCheckpointEntry("", types.TS{}, ts, checkpoint.ET_Incremental)
			entry.SetState(checkpoint.ST_Finished)
			runner := &checkpointRunnerSpy{entry: entry}
			mgr := txnbase.NewTxnManager(nil, nil, types.NewMockHLCClock(1000))
			t.Cleanup(mgr.Stop)
			db := &DB{BGFlusher: flusher, BGCheckpointRunner: runner, TxnMgr: mgr}

			location, err := db.ForceCheckpointForBackup(t.Context(), ts)
			require.Equal(t, 2, flusher.calls)
			require.Equal(t, 1, runner.ickpCalls)
			require.Equal(t, 1, runner.maxCalls)
			if secondFlushFails {
				require.ErrorIs(t, err, cause)
				require.Empty(t, location)
				require.Zero(t, runner.specialCalls)
			} else {
				require.NoError(t, err)
				require.Equal(t, "backup-location", location)
				require.Equal(t, 1, runner.specialCalls)
				require.Equal(t, ts.Next(), runner.specialStart)
				require.True(t, runner.specialEnd.GT(&runner.specialStart))
			}
		})
	}
}
