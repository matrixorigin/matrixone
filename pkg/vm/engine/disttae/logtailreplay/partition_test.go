// Copyright 2022 Matrix Origin
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

package logtailreplay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

func TestConsumeSnapCkpsWithPredecessorBeforeGlobal(t *testing.T) {
	predecessorEnd := types.BuildTS(300, 0)
	globalEnd := predecessorEnd.Next()
	continuationEnd := types.BuildTS(400, 0)
	entries := []*checkpoint.CheckpointEntry{
		checkpoint.NewCheckpointEntry(
			"", types.BuildTS(200, 0), predecessorEnd, checkpoint.ET_Incremental,
		),
		checkpoint.NewCheckpointEntry(
			"", types.TS{}, globalEnd, checkpoint.ET_Global,
		),
		checkpoint.NewCheckpointEntry(
			"", globalEnd, continuationEnd, checkpoint.ET_Incremental,
		),
	}

	partition := NewPartition("", nil, 0, 0, 42, nil)
	require.NoError(t, partition.ConsumeSnapCkps(
		context.Background(), entries,
		func(*checkpoint.CheckpointEntry, *PartitionState) error { return nil },
	))
	start, end := partition.Snapshot().GetDuration()
	require.Equal(t, globalEnd, start)
	require.Equal(t, continuationEnd, end)
}

func TestSnapshotTableInfoCancellation(t *testing.T) {
	partition := NewPartition("", nil, 0, 1, 42, nil)
	require.NoError(t, partition.Lock(t.Context()))
	partition.TableInfo = TableInfo{ID: 42, Name: "published", PrimarySeqnum: 3}
	partition.TableInfoOK = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := partition.SnapshotTableInfo(ctx)
		done <- err
	}()
	cancel()
	// Cancellation terminates without waiting for the publishing owner.
	err := <-done
	partition.Unlock()
	require.ErrorIs(t, err, context.Canceled)
	header, known, err := partition.SnapshotTableInfo(t.Context())
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, TableInfo{ID: 42, Name: "published", PrimarySeqnum: 3}, header)
}

func BenchmarkPartitonConsumeCheckpoint(b *testing.B) {
	partition := NewPartition("", nil, 0, 0, 42, nil)

	state, done := partition.MutateState()
	state.checkpoints = append(state.checkpoints, "a", "b", "c")
	done()

	b.ResetTimer()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		err := partition.ConsumeCheckpoints(ctx, func(checkpoint string, state *PartitionState) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentPartitionConsumeCheckpoint(b *testing.B) {
	partition := NewPartition("", nil, 0, 0, 42, nil)

	state, done := partition.MutateState()
	state.checkpoints = append(state.checkpoints, "a", "b", "c")
	done()

	b.ResetTimer()

	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := partition.ConsumeCheckpoints(ctx, func(checkpoint string, state *PartitionState) error {
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

}

func TestConsumeSnapCkpsStopsAtCancellation(t *testing.T) {
	entries := []*checkpoint.CheckpointEntry{
		checkpoint.NewCheckpointEntry("", types.TS{}, types.BuildTS(1, 0), checkpoint.ET_Global),
		checkpoint.NewCheckpointEntry("", types.BuildTS(1, 0), types.BuildTS(2, 0), checkpoint.ET_Incremental),
	}
	partition := NewPartition("", nil, 0, 0, 42, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := partition.ConsumeSnapCkps(ctx, entries, func(*checkpoint.CheckpointEntry, *PartitionState) error {
		calls++
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)

	ctx, cancel = context.WithCancel(context.Background())
	calls = 0
	err = partition.ConsumeSnapCkps(ctx, entries, func(*checkpoint.CheckpointEntry, *PartitionState) error {
		calls++
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
	start, end := partition.Snapshot().GetDuration()
	require.Equal(t, types.MaxTs(), start)
	require.True(t, end.IsEmpty())
}
