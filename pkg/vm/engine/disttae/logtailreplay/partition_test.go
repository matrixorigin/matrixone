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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

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

func TestPartitionContentPublication(t *testing.T) {
	p := NewPartition("", nil, 0, 0, 42, nil)
	original := p.Snapshot()
	state, publish := p.MutateState()
	state.UpdateAppliedTo(types.BuildTS(10, 0))
	publish()
	version, upper := p.Snapshot().ContentVersion()
	require.Zero(t, version, "an empty logtail must not invalidate reusable reads")
	require.True(t, upper.IsEmpty())

	state, publish = p.MutateState()
	state.RecordContentChange(types.BuildTS(10, 0))
	publish()
	version, upper = p.Snapshot().ContentVersion()
	require.Equal(t, uint64(1), version)
	require.Equal(t, types.BuildTS(10, 0), upper)
	oldVersion, oldUpper := original.ContentVersion()
	require.Zero(t, oldVersion, "publication must not relabel an older read")
	require.True(t, oldUpper.IsEmpty())

	// Lazy checkpoint completion publishes its own content change. Failure
	// must not expose a partially filled state or a new usable version.
	state, publish = p.MutateState()
	state.AppendCheckpoint("checkpoint", p)
	publish()
	before := p.Snapshot()
	failure := context.Canceled
	err := p.ConsumeCheckpoints(context.Background(), func(_ string, state *PartitionState) error {
		state.RecordContentChange(types.BuildTS(20, 0))
		return failure
	})
	require.ErrorIs(t, err, failure)
	require.Same(t, before, p.Snapshot())
	require.NoError(t, p.ConsumeCheckpoints(context.Background(), func(_ string, _ *PartitionState) error { return nil }))
	version, upper = p.Snapshot().ContentVersion()
	require.Equal(t, uint64(2), version)
	require.Equal(t, types.BuildTS(10, 0), upper)
	before = p.Snapshot()
	require.NoError(t, p.ConsumeCheckpoints(context.Background(), func(_ string, _ *PartitionState) error {
		t.Fatal("completed checkpoints must not load again")
		return nil
	}))
	require.Same(t, before, p.Snapshot())
}
