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
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/stretchr/testify/require"
)

func TestStopForWriteCancellationBeforeReplayJoin(t *testing.T) {
	ctl := newReplayCtl(nil, driver.ReplayMode_ReplayForever, nil)
	workerCtx, cancelWorker := context.WithCancelCause(t.Context())
	ctl.causeCancel = cancelWorker
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- ctl.StopForWrite(ctx) }()
	require.Eventually(t, func() bool {
		return ctl.GetMode() == driver.ReplayMode_ReplayForWrite
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled replay handoff did not return")
	}
	require.ErrorIs(t, context.Cause(workerCtx), context.Canceled)
	ctl.Done(context.Canceled)
	require.ErrorIs(t, ctl.Wait(), context.Canceled)
}
