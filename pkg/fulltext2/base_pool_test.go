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

package fulltext2

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentLeaseFreeIsIdempotent(t *testing.T) {
	template := NewSegment("base", 0)
	lease := newSegmentLease(template)
	view := lease.acquire(17)
	require.NotNil(t, view)
	require.Equal(t, int64(17), view.Recency)

	view.Free()
	view.Free()
	require.NotNil(t, lease.template, "the pool reference still owns the template")
	lease.retire()
	require.Nil(t, lease.template)
	lease.retire()
}

func TestImmutableBasePoolSingleflightAndRetire(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}
	var loads atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view, err := p.acquire(key, func() (*Segment, error) {
				loads.Add(1)
				return NewSegment("base-0", 0), nil
			}, 3)
			if err != nil {
				errs <- err
				return
			}
			view.Free()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), loads.Load())

	p.commit(key.index, nil)
	require.Empty(t, p.entries)
}

func TestLoadReasonRegistryIsDatabaseQualified(t *testing.T) {
	key1 := loadReasonKey("db1", "store")
	key2 := loadReasonKey("db2", "store")
	rememberLoadReason(key1, LoadMissCDCFlush)
	rememberLoadReason(key2, LoadMissMerge)
	require.Equal(t, LoadMissCDCFlush, takeLoadReason(key1))
	require.Equal(t, LoadMissMerge, takeLoadReason(key2))
	require.Empty(t, takeLoadReason(key1))
}
