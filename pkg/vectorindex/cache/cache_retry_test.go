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

package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type retryingLoadSearch struct {
	loads    atomic.Int32
	destroys atomic.Int32
}

type blockedInvalidStateLoadSearch struct {
	started  chan struct{}
	release  chan struct{}
	destroys atomic.Int32
}

type permanentInvalidStateLoadSearch struct {
	loads    atomic.Int32
	destroys atomic.Int32
}

func (m *permanentInvalidStateLoadSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{1}, nil
}

func (m *permanentInvalidStateLoadSearch) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ []int64, _ []float32) error {
	return nil
}

func (m *permanentInvalidStateLoadSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}

func (m *permanentInvalidStateLoadSearch) Load(*sqlexec.SqlProcess) error {
	if m.loads.Add(1) <= 4 {
		return moerr.NewInvalidStateNoCtx("permanent invalid state")
	}
	return moerr.NewInternalErrorNoCtx("bounded probe terminator")
}

func (m *permanentInvalidStateLoadSearch) Destroy() {
	m.destroys.Add(1)
}

func (m *blockedInvalidStateLoadSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{1}, nil
}

func (m *blockedInvalidStateLoadSearch) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ []int64, _ []float32) error {
	return nil
}

func (m *blockedInvalidStateLoadSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}

func (m *blockedInvalidStateLoadSearch) Load(*sqlexec.SqlProcess) error {
	close(m.started)
	<-m.release
	return NewRetryableLoadError(moerr.NewInvalidStateNoCtx("load superseded"))
}

func (m *blockedInvalidStateLoadSearch) Destroy() {
	m.destroys.Add(1)
}

func (m *retryingLoadSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{1}, nil
}

func (m *retryingLoadSearch) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	outKeys[0] = 1
	outDists[0] = 1
	return nil
}

func (m *retryingLoadSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}

func (m *retryingLoadSearch) Load(*sqlexec.SqlProcess) error {
	if m.loads.Add(1) == 1 {
		return NewRetryableLoadError(moerr.NewInvalidStateNoCtx("load superseded"))
	}
	return nil
}

func (m *retryingLoadSearch) Destroy() {
	m.destroys.Add(1)
}

func TestVectorIndexCacheSearchRetriesSupersededLoad(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &retryingLoadSearch{}
	keys, distances, err := c.Search(nil, "key", algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, keys)
	require.Equal(t, []float64{1}, distances)
	require.Equal(t, int32(2), algo.loads.Load())
	require.Equal(t, int32(1), algo.destroys.Load(), "the failed mapped entry must be destroyed before retry")

	c.Remove("key")
	require.Equal(t, int32(2), algo.destroys.Load())
}

func TestVectorIndexCacheSearchIntoRetriesSupersededLoad(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &retryingLoadSearch{}
	err := c.SearchInto(nil, "key", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
	require.NoError(t, err)
	require.Equal(t, int32(2), algo.loads.Load())
	require.Equal(t, int32(1), algo.destroys.Load(), "the failed mapped entry must be destroyed before retry")

	c.Remove("key")
	require.Equal(t, int32(2), algo.destroys.Load())
}

func TestVectorIndexSearchWaitersRetrySupersededLoad(t *testing.T) {
	tests := []struct {
		name   string
		search func(*VectorIndexSearch) error
	}{
		{
			name: "Search",
			search: func(s *VectorIndexSearch) error {
				_, _, err := s.Search(nil, nil, nil, vectorindex.RuntimeConfig{})
				return err
			},
		},
		{
			name: "SearchInto",
			search: func(s *VectorIndexSearch) error {
				return s.SearchInto(nil, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algo := &blockedInvalidStateLoadSearch{
				started: make(chan struct{}),
				release: make(chan struct{}),
			}
			s := &VectorIndexSearch{Algo: algo}
			s.Cond = sync.NewCond(s.Mutex.RLocker())
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() { close(algo.release) })
			}
			t.Cleanup(release)

			loadErr := make(chan error, 1)
			go func() {
				loadErr <- s.Load(nil)
			}()
			<-algo.started

			waiterErr := make(chan error, 1)
			go func() {
				waiterErr <- tt.search(s)
			}()
			require.Eventually(t, func() bool {
				return s.loadWaiters.Load() > 0
			}, time.Second, time.Millisecond)

			release()
			require.Error(t, <-loadErr)
			err := <-waiterErr
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState), err)

			s.destroyFailedLoad()
			require.Equal(t, int32(1), algo.destroys.Load())
		})
	}
}

func TestVectorIndexCacheReturnsPermanentInvalidStateLoadError(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func(*VectorIndexCache, VectorIndexSearchIf) error
	}{
		{
			name: "Search",
			call: func(c *VectorIndexCache, algo VectorIndexSearchIf) error {
				_, _, err := c.Search(nil, "key", algo, nil, vectorindex.RuntimeConfig{})
				return err
			},
		},
		{
			name: "SearchInto",
			call: func(c *VectorIndexCache, algo VectorIndexSearchIf) error {
				return c.SearchInto(nil, "key", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewVectorIndexCache()
			algo := &permanentInvalidStateLoadSearch{}

			err := tt.call(c, algo)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState), err)
			require.Equal(t, int32(1), algo.loads.Load())
			require.Equal(t, int32(1), algo.destroys.Load())
			_, loaded := c.IndexMap.Load("key")
			require.False(t, loaded)
		})
	}
}
