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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// evictStubSearch is a minimal VectorIndexSearchIf that loads a resident cache
// entry with no backing index tables, so EvictIdleCache can be exercised without a
// real fulltext2 build. Load/Search/Destroy are no-ops that only count calls.
type evictStubSearch struct {
	destroys atomic.Int32
}

var _ veccache.VectorIndexSearchIf = (*evictStubSearch)(nil)

func (*evictStubSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{0.1}, nil
}
func (*evictStubSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (*evictStubSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}
func (*evictStubSearch) Preload(*sqlexec.SqlProcess) error { return nil }
func (*evictStubSearch) Load(*sqlexec.SqlProcess) error    { return nil }
func (*evictStubSearch) GetIndexSize() (int64, int64)      { return 0, 0 }
func (*evictStubSearch) BuildTS() int64                    { return 0 }
func (s *evictStubSearch) Destroy()                        { s.destroys.Add(1) }

// TestEvictIdleCache covers the helper restored for #30401: a CDC tail flush drops
// the idle warm cache entry so the next query reloads the appended tail. It asserts
// the two outcomes the CDC flush relies on — an idle entry is evicted and destroyed,
// and an absent key is a no-op.
func TestEvictIdleCache(t *testing.T) {
	const idxKey = "__store_evict_idle"

	old := veccache.Cache
	tc := veccache.NewVectorIndexCache()
	veccache.Cache = tc
	t.Cleanup(func() {
		tc.Destroy()
		veccache.Cache = old
	})

	// Absent key: nothing resident, so the post-flush evict is a no-op.
	require.False(t, EvictIdleCache("no_such_index"), "an absent index is not evicted")

	// Load an idle entry (no search in flight), then evict it: the next query reloads.
	stub := &evictStubSearch{}
	_, _, err := tc.Search(nil, idxKey, stub, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	_, resident := tc.IndexMap.Load(idxKey)
	require.True(t, resident, "the entry is warm after a search loads it")

	require.True(t, EvictIdleCache(idxKey), "an idle warm entry is evicted")
	_, resident = tc.IndexMap.Load(idxKey)
	require.False(t, resident, "the evicted entry is gone so the next query reloads the tail")
	require.Equal(t, int32(1), stub.destroys.Load(), "the evicted backend is destroyed exactly once")

	// Idempotent: a second flush on the already-evicted key is a no-op.
	require.False(t, EvictIdleCache(idxKey), "a key with no resident entry is not evicted again")
	require.Equal(t, int32(1), stub.destroys.Load())
}
