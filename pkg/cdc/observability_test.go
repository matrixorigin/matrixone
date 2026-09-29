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

package cdc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProgressTrackerCheckStuckUsesActualProgress(t *testing.T) {
	tracker := NewProgressTracker(1, "task", "db", "table")
	tracker.SetState("waiting_for_initial_snapshot_batch_slot")

	tracker.mu.Lock()
	tracker.lastStateChange = time.Now().Add(-time.Hour)
	tracker.currentRoundStartTime = time.Now().Add(-time.Hour)
	tracker.lastWatermarkUpdate = time.Now().Add(-time.Hour)
	tracker.mu.Unlock()

	tracker.RecordBatch(1, 100)
	stuck, reason := tracker.CheckStuck(time.Minute)
	assert.False(t, stuck)
	assert.Empty(t, reason)
}

func TestProgressTrackerCheckStuckReportsInactivity(t *testing.T) {
	tracker := NewProgressTracker(1, "task", "db", "table")
	tracker.SetState("processing")
	tracker.lastProgressTime.Store(time.Now().Add(-time.Hour).UnixNano())

	stuck, reason := tracker.CheckStuck(time.Minute)
	assert.True(t, stuck)
	assert.Contains(t, reason, "no progress in state 'processing'")
}

func TestProgressTrackerCheckStuckIgnoresIdle(t *testing.T) {
	tracker := NewProgressTracker(1, "task", "db", "table")
	tracker.lastProgressTime.Store(time.Now().Add(-time.Hour).UnixNano())

	stuck, reason := tracker.CheckStuck(time.Minute)
	assert.False(t, stuck)
	assert.Empty(t, reason)
}
