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

package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type teardownRetryAlgo struct {
	loadStarted, releaseLoad, destroyStarted, releaseDestroy chan struct{}
	destroyOnce                                              sync.Once
	loads, destroys                                          atomic.Int32
	destroyFinished                                          atomic.Bool
	loadErr                                                  error
	successFirst                                             bool
}

var errPrematureReload = errors.New("reloaded before teardown finished")

func startCacheSearch(c *VectorIndexCache, proc *sqlexec.SqlProcess, algo VectorIndexSearchIf, into bool) <-chan error {
	result := make(chan error, 1)
	go func() {
		if into {
			result <- c.SearchInto(proc, "key", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
			return
		}
		_, _, err := c.Search(proc, "key", algo, nil, vectorindex.RuntimeConfig{})
		result <- err
	}()
	return result
}

func TestAwaitDestroyedHonorsCanceledContext(t *testing.T) {
	s := newVectorIndexSearch(&MockSearch{})
	s.markDestroyed()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.awaitDestroyed(ctx), context.Canceled)
	require.NoError(t, s.awaitDestroyed(context.Background()))
}

func (a *teardownRetryAlgo) Load(*sqlexec.SqlProcess) error {
	if a.loads.Add(1) == 1 {
		close(a.loadStarted)
		<-a.releaseLoad
		if a.loadErr != nil {
			return a.loadErr
		}
		if a.successFirst {
			return nil
		}
		return NewRetryableLoadError(moerr.NewInvalidStateNoCtx("superseded"))
	}
	return nil
}

func TestLoadErrorPreservedDuringEviction(t *testing.T) {
	for _, into := range []bool{false, true} {
		name := "Search"
		if into {
			name = "SearchInto"
		}
		t.Run(name, func(t *testing.T) {
			c := NewVectorIndexCache()
			want := errors.New("load failed")
			a := &teardownRetryAlgo{loadStarted: make(chan struct{}), releaseLoad: make(chan struct{}), destroyStarted: make(chan struct{}), releaseDestroy: make(chan struct{}), loadErr: want}
			var releaseLoad, releaseDestroy sync.Once
			t.Cleanup(func() {
				releaseLoad.Do(func() { close(a.releaseLoad) })
				releaseDestroy.Do(func() { close(a.releaseDestroy) })
			})
			result := startCacheSearch(c, nil, a, into)
			select {
			case <-a.loadStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("load did not start")
			}
			removed := make(chan struct{})
			go func() { c.Remove("key"); close(removed) }()
			require.Eventually(t, func() bool { _, ok := c.IndexMap.Load("key"); return !ok }, 5*time.Second, time.Millisecond)
			releaseLoad.Do(func() { close(a.releaseLoad) })
			select {
			case err := <-result:
				require.ErrorIs(t, err, want)
			case <-time.After(5 * time.Second):
				t.Fatal("load error did not return")
			}
			releaseDestroy.Do(func() { close(a.releaseDestroy) })
			select {
			case <-removed:
			case <-time.After(5 * time.Second):
				t.Fatal("eviction did not finish")
			}
			require.EqualValues(t, 1, a.loads.Load())
			require.EqualValues(t, 1, a.destroys.Load())
		})
	}
}

func (a *teardownRetryAlgo) Destroy() {
	a.destroys.Add(1)
	a.destroyOnce.Do(func() { close(a.destroyStarted) })
	<-a.releaseDestroy
	a.destroyFinished.Store(true)
}

func (a *teardownRetryAlgo) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	if a.loads.Load() > 1 && !a.destroyFinished.Load() {
		return nil, nil, errPrematureReload
	}
	return nil, nil, nil
}

func (a *teardownRetryAlgo) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	if a.loads.Load() > 1 && !a.destroyFinished.Load() {
		return errPrematureReload
	}
	return nil
}

func (*teardownRetryAlgo) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}

func TestRetryWaitsForOwnedTeardownAndHonorsCancellation(t *testing.T) {
	for _, into := range []bool{false, true} {
		name := "Search"
		if into {
			name = "SearchInto"
		}
		for _, cancelled := range []bool{true, false} {
			caseName := name + "/retry"
			if cancelled {
				caseName = name + "/cancel"
			}
			t.Run(caseName, func(t *testing.T) {
				c := NewVectorIndexCache()
				a := &teardownRetryAlgo{loadStarted: make(chan struct{}), releaseLoad: make(chan struct{}), destroyStarted: make(chan struct{}), releaseDestroy: make(chan struct{})}
				ctx, cancel := context.WithCancel(context.Background())
				var releaseLoad, releaseDestroy sync.Once
				t.Cleanup(func() {
					cancel()
					releaseLoad.Do(func() { close(a.releaseLoad) })
					releaseDestroy.Do(func() { close(a.releaseDestroy) })
				})
				proc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: ctx}}
				result := startCacheSearch(c, proc, a, into)
				select {
				case <-a.loadStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("load did not start")
				}
				removed := make(chan struct{})
				go func() { c.Remove("key"); close(removed) }()
				require.Eventually(t, func() bool { _, ok := c.IndexMap.Load("key"); return !ok }, 5*time.Second, time.Millisecond)
				releaseLoad.Do(func() { close(a.releaseLoad) })
				select {
				case <-a.destroyStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("destroy did not start")
				}
				if cancelled {
					cancel()
				} else {
					releaseDestroy.Do(func() { close(a.releaseDestroy) })
				}
				select {
				case err := <-result:
					if cancelled {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("cancelled retry did not return during teardown")
				}
				if cancelled {
					require.EqualValues(t, 1, a.loads.Load(), "the owned algorithm must not be reloaded before destruction")
				} else {
					require.EqualValues(t, 2, a.loads.Load())
				}
				releaseDestroy.Do(func() { close(a.releaseDestroy) })
				select {
				case <-removed:
				case <-time.After(5 * time.Second):
					t.Fatal("eviction did not finish")
				}
				require.EqualValues(t, 1, a.destroys.Load())
				if !cancelled {
					c.Remove("key")
					require.EqualValues(t, 2, a.destroys.Load())
				}
			})
		}
	}
}

type invalidSearchAlgo struct {
	MockSearch
	calls atomic.Int32
}

func (a *invalidSearchAlgo) searchError() error {
	if a.calls.Add(1) > 1 {
		return errors.New("unexpected retry")
	}
	return moerr.NewInvalidStateNoCtx("algorithm invalid state")
}

func (a *invalidSearchAlgo) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, a.searchError()
}

func (a *invalidSearchAlgo) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return a.searchError()
}

func TestSearchPreservesAlgorithmInvalidState(t *testing.T) {
	for _, into := range []bool{false, true} {
		c := NewVectorIndexCache()
		a := &invalidSearchAlgo{}
		var err error
		if into {
			err = c.SearchInto(nil, "key", a, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
		} else {
			_, _, err = c.Search(nil, "key", a, nil, vectorindex.RuntimeConfig{})
		}
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState), "%v", err)
		require.ErrorContains(t, err, "algorithm invalid state")
		require.EqualValues(t, 1, a.calls.Load())
		c.Destroy()
	}
}

func TestFollowerRetriesWithoutWaitingForForeignTeardown(t *testing.T) {
	for _, into := range []bool{false, true} {
		name := "Search"
		if into {
			name = "SearchInto"
		}
		t.Run(name, func(t *testing.T) {
			c := NewVectorIndexCache()
			old := newVectorIndexSearch(&MockSearch{})
			c.IndexMap.Store("key", old)
			t.Cleanup(func() {
				old.Destroy()
				c.Destroy()
			})
			result := startCacheSearch(c, nil, &MockSearch{}, into)
			require.Eventually(t, func() bool { return old.loadWaiters.Load() > 0 }, 5*time.Second, time.Millisecond)
			old.evicting.Store(true)
			require.True(t, c.IndexMap.CompareAndDelete("key", old))
			old.markEvictionResultReady()
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("follower waited for an unrelated teardown")
			}
		})
	}
}

func TestSuccessfulLoadEvictedBeforeSearch(t *testing.T) {
	for _, into := range []bool{false, true} {
		for _, cancelled := range []bool{true, false} {
			name := "Search/retry"
			if into {
				name = "SearchInto/retry"
			}
			if cancelled {
				name += "/cancel"
			}
			t.Run(name, func(t *testing.T) {
				c := NewVectorIndexCache()
				a := &teardownRetryAlgo{loadStarted: make(chan struct{}), releaseLoad: make(chan struct{}), destroyStarted: make(chan struct{}), releaseDestroy: make(chan struct{}), successFirst: true}
				ctx, cancel := context.WithCancel(context.Background())
				var loadRelease, destroyRelease, startDestroy sync.Once
				var old *VectorIndexSearch
				removed := make(chan struct{})
				destroy := func() { startDestroy.Do(func() { go func() { old.Destroy(); close(removed) }() }) }
				t.Cleanup(func() {
					cancel()
					loadRelease.Do(func() { close(a.releaseLoad) })
					destroyRelease.Do(func() { close(a.releaseDestroy) })
					if old != nil {
						destroy()
						<-removed
					}
				})
				proc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: ctx}}
				result := startCacheSearch(c, proc, a, into)
				select {
				case <-a.loadStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("load did not start")
				}
				value, ok := c.IndexMap.Load("key")
				require.True(t, ok)
				old = value.(*VectorIndexSearch)
				// Stage the real eviction transition through map deletion, then delay
				// its destroy call. The loader must not reuse its object in this gap.
				require.True(t, old.beginEviction(false))
				require.True(t, c.IndexMap.CompareAndDelete("key", old))
				if cancelled {
					cancel()
				}
				loadRelease.Do(func() { close(a.releaseLoad) })
				if !cancelled {
					destroy()
					destroyRelease.Do(func() { close(a.releaseDestroy) })
				}
				select {
				case err := <-result:
					if cancelled {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("retry did not settle")
				}
				if cancelled {
					require.EqualValues(t, 1, a.loads.Load())
				} else {
					require.EqualValues(t, 2, a.loads.Load())
					c.Remove("key")
				}
			})
		}
	}
}
