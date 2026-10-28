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

package disttae

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// P2: retryDurableOrphanGC's rejection terminals (a closed engine, an
// unavailable GC pool) must read the retained-name count while holding
// the registry lock — moving that read past the unlock races every
// concurrent registration mutating ent.set, and losing it also corrupts
// the operator-visible ownership log. This test hammers both rejecting
// branches against concurrent registrations: under -race the pre-fix
// shape is detected reading len(ent.set) unlocked, and post-fix every
// registered name must survive (a rejection retains ownership; only a
// confirmed deletion deregisters).
func TestRetryDurableOrphanGCRejectionRetainsOwnershipUnderRegistrations(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS(
		ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// Bare engines reach exactly the two rejection terminals: one is
	// closed, the other has no GC pool. Zero-value dynamicCtx and a nil
	// gcPool are the zero-cost stand-ins — retryDurableOrphanGC touches
	// only fs, dynamicCtx.closed, and gcPool before its terminals.
	closedEng := &Engine{fs: fs}
	closedEng.dynamicCtx.closed.Store(true)
	poolLessEng := &Engine{fs: fs}

	// The registry is process-global keyed by this test's private fs;
	// the rejecting engines never sweep, so release the whole entry at
	// the end instead of waiting for a drain that cannot come.
	t.Cleanup(func() {
		durableOrphanGC.mu.Lock()
		delete(durableOrphanGC.entries, fs)
		durableOrphanGC.mu.Unlock()
	})

	const (
		writers       = 4
		namesPerWrite = 100
		retryRounds   = 300
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < namesPerWrite; j++ {
				registerDurableOrphanGC(fs, fmt.Sprintf("SOME/race_%d_%d", id, j))
			}
		}(w)
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func(useClosed bool) {
			defer wg.Done()
			for j := 0; j < retryRounds; j++ {
				if useClosed {
					retryDurableOrphanGC(closedEng)
				} else {
					retryDurableOrphanGC(poolLessEng)
				}
			}
		}(r == 0)
	}
	wg.Wait()

	// Every registered name survived both rejection terminals.
	names := durableOrphanGCNames(&Engine{fs: fs})
	require.Len(t, names, writers*namesPerWrite)
	for w := 0; w < writers; w++ {
		for j := 0; j < namesPerWrite; j++ {
			require.Contains(t, names, fmt.Sprintf("SOME/race_%d_%d", w, j))
		}
	}
}
