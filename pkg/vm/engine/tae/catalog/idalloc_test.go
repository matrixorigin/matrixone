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

package catalog

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplayIDAllocatorMonotonic(t *testing.T) {
	allocator := NewIDAllocator()
	for _, field := range []struct {
		name    string
		replay  func(uint64)
		current func() uint64
		next    func() uint64
	}{
		{"database", allocator.OnReplayDBID, allocator.CurrDB, allocator.NextDB},
		{"table", allocator.OnReplayTableID, allocator.CurrTable, allocator.NextTable},
		{"object", allocator.OnReplayObjectID, allocator.CurrObject, allocator.NextObject},
		{"block", allocator.OnReplayBlockID, allocator.CurrBlock, allocator.NextBlock},
	} {
		t.Run(field.name, func(t *testing.T) {
			// Different replay workers can observe the same old watermark. The
			// last worker to publish must not win over a larger recovered ID.
			const workers = 16
			for range 256 {
				base := field.current()
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range workers {
					wg.Go(func() {
						<-start
						field.replay(base + uint64(i) + 1)
					})
				}
				close(start)
				wg.Wait()
				require.Equal(t, base+workers, field.current())
			}
			maximum := field.current()
			field.replay(maximum)
			field.replay(maximum - 1)
			require.Equal(t, maximum+1, field.next(), "allocation must follow every recovered ID")
		})
	}
}
