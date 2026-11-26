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

package fileservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexPrefetchPreservesConcurrentDiskCacheFill(t *testing.T) {
	for _, tc := range []struct {
		name           string
		prefetchPolicy Policy
	}{
		{name: "prefetch_into_disk_cache"},
		{name: "prefetch_into_index_cache", prefetchPolicy: IndexFileObject},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			gated := newBlackBoxStorage(storage, func(_ string, offset int64) bool { return offset == 0 })
			fs.server.raw = gated
			unblock := sync.OnceFunc(func() { close(gated.gate) })
			prefetchDone := make(chan struct{})
			var prefetchErr error
			go func() {
				defer close(prefetchDone)
				prefetchErr = fs.PrefetchFile(WithFileServicePolicy(ctx, tc.prefetchPolicy), "object")
			}()
			defer func() { unblock(); <-prefetchDone }()
			require.Eventually(t, func() bool {
				active, _, _ := gated.counts("object")
				return active == 1
			}, time.Second, time.Millisecond, "whole prefetch should have opened storage")
			// The first exact read bypasses disk-cache reads but allows its writes.
			require.Equal(t, data[128:144], readIndexObjectRange(t, fs, ExactColumnsRead|SkipDiskCacheReads, 128, 16))
			unblock()
			select {
			case <-prefetchDone:
				require.NoError(t, prefetchErr)
			case <-ctx.Done():
				t.Fatal("whole prefetch did not finish")
			}
			_, indexed := fs.indexObjectCached("object")
			require.Equal(t, tc.prefetchPolicy == IndexFileObject, indexed)
			before := storage.reads.Load()
			require.Equal(t, data[128:144], readIndexObjectRange(t, fs, ExactColumnsRead, 128, 16))
			require.Equal(t, before, storage.reads.Load(), "the normal read should find the whole object or its exact range in the disk cache")
		})
	}
}
