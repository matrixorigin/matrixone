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

package db

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/stretchr/testify/require"
)

func TestStopForWriteCancellationBeforeReplayJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctl := newReplayCtl(nil, driver.ReplayMode_ReplayForever, nil)
		workerCtx, cancelWorker := context.WithCancelCause(t.Context())
		defer cancelWorker(nil)
		ctl.causeCancel = cancelWorker
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		defer func() {
			// Keep the replay unfinished until the cancellation oracle, and
			// release a regressed join even when an assertion calls FailNow.
			ctl.Done(context.Canceled)
			synctest.Wait()
			require.ErrorIs(t, ctl.Wait(), context.Canceled)
		}()
		result := make(chan error, 1)
		go func() { result <- ctl.StopForWrite(ctx) }()
		synctest.Wait()
		require.Equal(t, driver.ReplayMode_ReplayForWrite, ctl.GetMode())
		require.NoError(t, context.Cause(workerCtx))
		select {
		case err := <-result:
			t.Fatalf("replay handoff returned before cancellation or replay completion: %v", err)
		default:
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		default:
			t.Fatal("canceled replay handoff did not return before replay joined")
		}
		require.ErrorIs(t, context.Cause(workerCtx), context.Canceled)
	})
}
