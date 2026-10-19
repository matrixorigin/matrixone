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
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/stretchr/testify/require"
)

// Only the checkpoint snapshot needed before an absent global boundary. Any
// unexpected runner operation still fails through the embedded interface.
type checkSnapshotRunner struct{ checkpoint.Runner }

func (*checkSnapshotRunner) GetAllIncrementalCheckpoints() []*checkpoint.CheckpointEntry { return nil }
func (*checkSnapshotRunner) GetAllGlobalCheckpoints() []*checkpoint.CheckpointEntry      { return nil }
func (*checkSnapshotRunner) GetCompacted() *checkpoint.CheckpointEntry                   { return nil }
func (*checkSnapshotRunner) MaxGlobalCheckpoint() *checkpoint.CheckpointEntry            { return nil }

func TestCheckpointCheckDoesNotWaitForLockedGCProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cleaner := &checkpointCleaner{ctx: t.Context(), checkpointCli: &checkSnapshotRunner{}}
		cleaner.updateScanWaterMark(createMockCheckpointEntry(0, 1))
		before := time.Now()
		require.NoError(t, cleaner.DoCheck(context.Background()))
		// GC cannot publish a watermark while DoCheck owns the mutation lock.
		// Virtual time proves this boundary does not create a polling delay, without
		// imposing a wall-clock performance threshold on CI.
		require.Equal(t, before, time.Now(), "inspection must not wait for the lock owner it excludes")
		require.True(t, cleaner.mutation.TryLock(), "inspection must release its mutation ownership")
		cleaner.mutation.Unlock()
	})
}
