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

package gc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/stretchr/testify/require"
)

// Only the real queue's replay, processing and shutdown boundaries are faked.
// An unexpected call through Cleaner fails instead of silently succeeding.
type cleanerProbe struct {
	Cleaner
	replayed, processed, stopped int
	replayErr                    error
	processFn                    func(context.Context) error
}

func (c *cleanerProbe) Replay(context.Context) error {
	c.replayed++
	return c.replayErr
}

func (c *cleanerProbe) Process(ctx context.Context, _ func(*checkpoint.CheckpointEntry) bool) error {
	c.processed++
	if c.processFn != nil {
		return c.processFn(ctx)
	}
	return nil
}

func (c *cleanerProbe) Stop() { c.stopped++ }

func TestDiskCleaner_ReplayToWrite(t *testing.T) {
	cleaner := &cleanerProbe{}
	diskCleaner := NewDiskCleaner(cleaner, false)
	require.True(t, diskCleaner.IsReplayMode())
	require.Zero(t, cleaner.replayed)
	require.Zero(t, cleaner.processed)

	diskCleaner.Start()
	t.Cleanup(func() {
		diskCleaner.CancelRunning(CauseStopGC)
		diskCleaner.Stop()
		require.Equal(t, 1, cleaner.stopped)
	})
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.Equal(t, 1, cleaner.replayed)
	require.Zero(t, cleaner.processed)
	require.Error(t, diskCleaner.GC(t.Context()))

	require.NoError(t, diskCleaner.SwitchToWriteMode(t.Context()))
	require.True(t, diskCleaner.IsWriteMode())
	require.NoError(t, diskCleaner.GC(t.Context()))
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.Equal(t, 1, cleaner.replayed)
	require.Equal(t, 1, cleaner.processed)
}

func TestDiskCleaner_WriteToReplay(t *testing.T) {
	cleaner := &cleanerProbe{}
	diskCleaner := NewDiskCleaner(cleaner, true)
	require.True(t, diskCleaner.IsWriteMode())
	require.Zero(t, cleaner.replayed)
	require.Zero(t, cleaner.processed)

	diskCleaner.Start()
	t.Cleanup(func() {
		diskCleaner.CancelRunning(CauseStopGC)
		diskCleaner.Stop()
		require.Equal(t, 1, cleaner.stopped)
	})
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.Equal(t, 1, cleaner.replayed)
	require.Zero(t, cleaner.processed)
	require.NoError(t, diskCleaner.GC(t.Context()))
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.Equal(t, 1, cleaner.processed)

	const queued = 10
	entered := make(chan struct{})
	causes := make(chan error, queued)
	cleaner.processFn = func(ctx context.Context) error {
		if cleaner.processed == 2 {
			close(entered)
		}
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return context.Cause(ctx)
	}
	for range queued {
		require.NoError(t, diskCleaner.GC(t.Context()))
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("queued GC did not enter processing")
	}
	// The first job is in flight and the remaining jobs are queued. Switching
	// must cancel that generation and join every accepted job before returning.
	require.NoError(t, diskCleaner.SwitchToReplayMode(t.Context()))
	require.True(t, diskCleaner.IsReplayMode())
	require.Equal(t, queued+1, cleaner.processed)
	for range queued {
		require.ErrorIs(t, <-causes, CauseSwitchWrite2Replay)
	}
	err := diskCleaner.GC(t.Context())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnControl))

	// FlushQueue installs a fresh running context; cancellation must not leak
	// into the next write generation.
	var resumedErr error
	cleaner.processFn = func(ctx context.Context) error {
		resumedErr = ctx.Err()
		return resumedErr
	}
	require.NoError(t, diskCleaner.SwitchToWriteMode(t.Context()))
	require.NoError(t, diskCleaner.GC(t.Context()))
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.NoError(t, resumedErr)
	require.Equal(t, queued+2, cleaner.processed)
}

func TestDiskCleaner_ReplayFailureRecovery(t *testing.T) {
	replayErr := errors.New("replay failed")
	cleaner := &cleanerProbe{replayErr: replayErr}
	diskCleaner := NewDiskCleaner(cleaner, true)
	diskCleaner.Start()
	t.Cleanup(func() {
		diskCleaner.CancelRunning(CauseStopGC)
		diskCleaner.Stop()
		require.Equal(t, 1, cleaner.stopped)
	})
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.NotNil(t, diskCleaner.replayError.Load())
	require.ErrorIs(t, *diskCleaner.replayError.Load(), replayErr)

	require.NoError(t, diskCleaner.GC(t.Context()))
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.Equal(t, 2, cleaner.replayed)
	require.Zero(t, cleaner.processed, "failed replay must prevent GC processing")
	require.ErrorIs(t, *diskCleaner.replayError.Load(), replayErr)

	cleaner.replayErr = nil
	require.NoError(t, diskCleaner.GC(t.Context()))
	require.NoError(t, diskCleaner.FlushQueue(t.Context()))
	require.Equal(t, 3, cleaner.replayed)
	require.Equal(t, 1, cleaner.processed)
	require.Nil(t, diskCleaner.replayError.Load())
	require.True(t, diskCleaner.IsWriteMode())
}
