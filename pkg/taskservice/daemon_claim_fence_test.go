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

package taskservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatDaemonTaskFencesSupersededClaim(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage()
	firstClaim := task.DaemonTask{
		ID:            1,
		Metadata:      task.TaskMetadata{ID: t.Name()},
		TaskRunner:    "cn-a",
		TaskStatus:    task.TaskStatus_Running,
		LastRun:       time.Unix(100, 1),
		LastHeartbeat: time.Unix(100, 2),
	}
	added, err := store.AddDaemonTask(ctx, firstClaim)
	require.NoError(t, err)
	require.Equal(t, 1, added)

	secondClaim := firstClaim
	secondClaim.TaskRunner = "cn-b"
	secondClaim.LastRun = time.Unix(200, 1)
	updated, err := store.UpdateDaemonTask(ctx, []task.DaemonTask{secondClaim}, WithTaskIDCond(EQ, firstClaim.ID))
	require.NoError(t, err)
	require.Equal(t, 1, updated)

	firstClaim.LastHeartbeat = time.Unix(300, 1)
	renewed, err := store.HeartbeatDaemonTask(ctx, []task.DaemonTask{firstClaim})
	require.NoError(t, err)
	require.Equal(t, 0, renewed, "the stale claim must not renew after takeover")

	secondClaim.LastHeartbeat = time.Unix(300, 2)
	renewed, err = store.HeartbeatDaemonTask(ctx, []task.DaemonTask{secondClaim})
	require.NoError(t, err)
	require.Equal(t, 1, renewed)
}

func TestNextDaemonClaimTimeIsMonotonicAcrossClockRollback(t *testing.T) {
	previous := time.Unix(200, 123000)
	next := nextDaemonClaimTime(previous, time.Unix(100, 0))
	require.True(t, next.After(previous))
	require.Equal(t, previous.Add(time.Microsecond), next)
}

func TestNextDaemonClaimTimeSurvivesSQLPrecision(t *testing.T) {
	previous := time.Unix(200, 123456000)
	for _, now := range []time.Time{
		previous.Add(-time.Second), previous, previous.Add(time.Nanosecond),
		previous.Add(999 * time.Nanosecond), previous.Add(time.Second + 999*time.Nanosecond),
	} {
		next := nextDaemonClaimTime(previous, now)
		require.True(t, next.After(previous))
		require.Equal(t, next.Truncate(time.Microsecond), next)
	}
	// Legacy in-memory claims may still carry nanoseconds during transition.
	next := nextDaemonClaimTime(previous.Add(999*time.Nanosecond), previous)
	require.Equal(t, previous.Add(time.Microsecond), next)
}
