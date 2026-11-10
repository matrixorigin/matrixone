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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gotEvictKeyProbe struct{}

func TestVectorIndexHooks(t *testing.T) {
	oldSetter, oldCounter, oldEvictor, oldLister := vectorIndexStaleCheckIntervalSetter, vectorIndexCacheKeyCounter, vectorIndexCacheEvictor, vectorIndexCacheKeyLister
	t.Cleanup(func() {
		vectorIndexStaleCheckIntervalSetter = oldSetter
		vectorIndexCacheKeyCounter = oldCounter
		vectorIndexCacheEvictor = oldEvictor
		vectorIndexCacheKeyLister = oldLister
	})

	// Unregistered: setter is a no-op, get/evict/list report not-linked.
	vectorIndexStaleCheckIntervalSetter, vectorIndexCacheKeyCounter, vectorIndexCacheEvictor, vectorIndexCacheKeyLister = nil, nil, nil, nil
	SetVectorIndexStaleCheckInterval(time.Second) // must not panic
	n, ok := VectorIndexCacheCountKey("k")
	require.False(t, ok)
	require.Zero(t, n)
	n, ok = EvictVectorIndexCache(context.Background(), "k")
	require.False(t, ok)
	require.Zero(t, n)
	keys, ok := VectorIndexCacheKeys()
	require.False(t, ok)
	require.Nil(t, keys)

	// Registered: wrappers dispatch to the registered funcs.
	var gotDur time.Duration
	var gotCountKey, gotEvictKey string
	var gotEvictCtx context.Context
	RegisterVectorIndexStaleCheckIntervalSetter(func(d time.Duration) { gotDur = d })
	RegisterVectorIndexCacheKeyCounter(func(key string) int64 { gotCountKey = key; return 7 })
	RegisterVectorIndexCacheEvictor(func(ctx context.Context, key string) int64 { gotEvictCtx, gotEvictKey = ctx, key; return 3 })
	RegisterVectorIndexCacheKeyLister(func() []string { return []string{"a", "b"} })

	SetVectorIndexStaleCheckInterval(2 * time.Second)
	require.Equal(t, 2*time.Second, gotDur)

	n, ok = VectorIndexCacheCountKey("idx")
	require.True(t, ok)
	require.Equal(t, int64(7), n)
	require.Equal(t, "idx", gotCountKey)

	evictCtx := context.WithValue(context.Background(), gotEvictKeyProbe{}, "probe")
	n, ok = EvictVectorIndexCache(evictCtx, "idx")
	require.True(t, ok)
	require.Equal(t, int64(3), n)
	require.Equal(t, "idx", gotEvictKey)
	require.Equal(t, "probe", gotEvictCtx.Value(gotEvictKeyProbe{}), "the caller's context is propagated to the evictor")

	keys, ok = VectorIndexCacheKeys()
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, keys)
}
