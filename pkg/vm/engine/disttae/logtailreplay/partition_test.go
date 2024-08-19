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
)

func BenchmarkPartitonConsumeCheckpoint(b *testing.B) {
	partition := NewPartition("", 42)

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
	partition := NewPartition("", 42)

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
