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

package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVectorIndexHooks(t *testing.T) {
	oldSetter, oldCounter, oldEvictor := vectorIndexStaleCheckIntervalSetter, vectorIndexCacheKeyCounter, vectorIndexCacheEvictor
	t.Cleanup(func() {
		vectorIndexStaleCheckIntervalSetter = oldSetter
		vectorIndexCacheKeyCounter = oldCounter
		vectorIndexCacheEvictor = oldEvictor
	})

	// Unregistered: setter is a no-op, get/evict report not-linked.
	vectorIndexStaleCheckIntervalSetter, vectorIndexCacheKeyCounter, vectorIndexCacheEvictor = nil, nil, nil
	SetVectorIndexStaleCheckInterval(time.Second) // must not panic
	n, ok := VectorIndexCacheCountKey("k")
	require.False(t, ok)
	require.Zero(t, n)
	n, ok = EvictVectorIndexCache("k")
	require.False(t, ok)
	require.Zero(t, n)

	// Registered: wrappers dispatch to the registered funcs.
	var gotDur time.Duration
	var gotCountKey, gotEvictKey string
	RegisterVectorIndexStaleCheckIntervalSetter(func(d time.Duration) { gotDur = d })
	RegisterVectorIndexCacheKeyCounter(func(key string) int64 { gotCountKey = key; return 7 })
	RegisterVectorIndexCacheEvictor(func(key string) int64 { gotEvictKey = key; return 3 })

	SetVectorIndexStaleCheckInterval(2 * time.Second)
	require.Equal(t, 2*time.Second, gotDur)

	n, ok = VectorIndexCacheCountKey("idx")
	require.True(t, ok)
	require.Equal(t, int64(7), n)
	require.Equal(t, "idx", gotCountKey)

	n, ok = EvictVectorIndexCache("idx")
	require.True(t, ok)
	require.Equal(t, int64(3), n)
	require.Equal(t, "idx", gotEvictKey)
}
