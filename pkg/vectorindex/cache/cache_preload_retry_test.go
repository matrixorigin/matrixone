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

// Preload is its own locked section ahead of Load with its own copy of the
// load-failure handling. cache_retry_test.go covers the Load half; these cover the
// Preload half, for Search and SearchInto.
package cache

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// preloadFailSearch fails its first `failures` Preload calls; retryable selects the
// retry marker.
type preloadFailSearch struct {
	failures  int32
	retryable bool
	preloads  atomic.Int32
	loads     atomic.Int32
	destroys  atomic.Int32
}

func (m *preloadFailSearch) Preload(*sqlexec.SqlProcess) error {
	if m.preloads.Add(1) <= m.failures {
		err := moerr.NewInvalidStateNoCtx("preload superseded")
		if m.retryable {
			return NewRetryableLoadError(err)
		}
		return err
	}
	return nil
}

func (m *preloadFailSearch) Load(*sqlexec.SqlProcess) error {
	m.loads.Add(1)
	return nil
}

func (m *preloadFailSearch) GetIndexSize() (int64, int64) { return 0, 0 }
func (m *preloadFailSearch) Destroy()                     { m.destroys.Add(1) }

func (m *preloadFailSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{1}, nil
}

func (m *preloadFailSearch) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	outKeys[0] = 1
	outDists[0] = 1
	return nil
}

func (m *preloadFailSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}

var _ VectorIndexSearchIf = (*preloadFailSearch)(nil)

// A retryable Preload failure destroys the mapped entry and retries.
func TestSearchRetriesSupersededPreload(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &preloadFailSearch{failures: 1, retryable: true}

	keys, distances, err := c.Search(nil, "key", algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, keys)
	require.Equal(t, []float64{1}, distances)

	require.Equal(t, int32(2), algo.preloads.Load(), "one failure, then the retry")
	require.Equal(t, int32(1), algo.loads.Load(), "Load only runs after Preload succeeds")
	require.Equal(t, int32(1), algo.destroys.Load(), "the failed entry is torn down before the retry")

	c.Remove("key")
	require.Equal(t, int32(2), algo.destroys.Load())
}

func TestSearchIntoRetriesSupersededPreload(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &preloadFailSearch{failures: 1, retryable: true}

	var out vectorindex.SearchOutput
	require.NoError(t, c.SearchInto(nil, "key", algo, nil, vectorindex.RuntimeConfig{}, &out))
	require.Equal(t, int32(2), algo.preloads.Load())
	require.Equal(t, int32(1), algo.loads.Load())
	require.Equal(t, int32(1), algo.destroys.Load())

	c.Remove("key")
}

// A Preload failure without the retry marker propagates and the entry is removed.
func TestSearchPropagatesPermanentPreloadError(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &preloadFailSearch{failures: 1, retryable: false}

	_, _, err := c.Search(nil, "key", algo, nil, vectorindex.RuntimeConfig{})
	require.Error(t, err)
	require.Equal(t, int32(1), algo.preloads.Load(), "no retry on a permanent error")
	require.Equal(t, int32(0), algo.loads.Load())
	require.Equal(t, int32(1), algo.destroys.Load())

	_, ok := c.IndexMap.Load("key")
	require.False(t, ok, "the failed entry is not left mapped")
}

func TestSearchIntoPropagatesPermanentPreloadError(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &preloadFailSearch{failures: 1, retryable: false}

	var out vectorindex.SearchOutput
	require.Error(t, c.SearchInto(nil, "key", algo, nil, vectorindex.RuntimeConfig{}, &out))
	require.Equal(t, int32(1), algo.preloads.Load())
	require.Equal(t, int32(0), algo.loads.Load())

	_, ok := c.IndexMap.Load("key")
	require.False(t, ok)
}

// An entry claimed for eviction refuses Preload with ErrInvalidState.
func TestPreloadRefusesEvictingEntry(t *testing.T) {
	s := newVectorIndexSearch(&preloadFailSearch{})
	s.evicting.Store(true)

	err := s.Preload(nil)
	require.Error(t, err)
	require.Equal(t, moerr.ErrInvalidState, err.(*moerr.Error).ErrorCode())
}

// awaitDestroyed on an entry with no destroyed channel returns immediately.
func TestAwaitDestroyedNoChannel(t *testing.T) {
	(&VectorIndexSearch{}).awaitDestroyed()
}

// --- retryableLoadError ----------------------------------------------------

func TestRetryableLoadError(t *testing.T) {
	require.Nil(t, NewRetryableLoadError(nil))

	cause := moerr.NewInvalidStateNoCtx("boom")
	wrapped := NewRetryableLoadError(cause)
	require.Equal(t, cause.Error(), wrapped.Error())
	require.True(t, IsRetryableLoadError(wrapped))

	require.False(t, IsRetryableLoadError(cause), "a bare error is not retryable")
	require.False(t, IsRetryableLoadError(nil))
}
