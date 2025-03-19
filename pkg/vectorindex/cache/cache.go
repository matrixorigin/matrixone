// Copyright 2022 Matrix Origin
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
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
   VectorIndexCache is the generalized cache structure for various algorithm types that share the VectorIndexSearchIf interface.
   Implement the VectorIndexSearchIf such as HnswSearch to able to use VectorIndexCache.

   VectorIndexCache allows to search the vector index concurrently.  Usually vector index model is huge in size and it is not possible
   to load the whole model to memory for each user.  We need a cache that can run concurrently and able to refresh automatically.

   1. When the index is loaded into memory, index can be shared with RWMutex.Rlock() (Read-Only)
   2. With RWMutex.Lock (Write),  index can be loaded from database without race.
   3. HouseKeeping. Index will have time-to-live interval (see VectorIndexCacheTTL).
      3.1 When the index is expired (ExpireAt > 0 && ExpiredAt < Now), index will be deleted from the cache. Ticker go routine will manage the house keeping.
      3.2 ExpiredAt == 0 means index is loading from database so cannot be deleted from housekeeping
      3.3 Every time index is visited by Search/LoadFromDatabase, ExpireAt will be extended to time.Now() + VectorIndexCacheTTL.
*/

const (
	STATUS_NOT_INIT  = 0
	STATUS_LOADED    = 1
	STATUS_OUTDATED  = 2
	STATUS_DESTROYED = 3
	STATUS_ERROR     = 4
)

var (
	VectorIndexCacheTTL time.Duration     = 5 * time.Minute
	Cache               *VectorIndexCache = NewVectorIndexCache()
)

// Various vector index algorithm wants to share with VectorIndexCache need to implement VectorIndexSearchIf interface (see HnswSearch)
type VectorIndexSearchIf interface {
	Search(proc *process.Process, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error)
	Load(*process.Process) error
	UpdateConfig(VectorIndexSearchIf) error
	Destroy()
}

// base VectorIndex Search structure for VectorIndexSearchIf (see HnswSearch)
type VectorIndexSearch struct {
	Mutex      sync.RWMutex
	ExpireAt   atomic.Int64
	LastUpdate atomic.Int64
	Status     atomic.Int32 // 0 - NOT INIT, 1 - LOADED, 2 - marked as outdated,  3 - DESTROYED,  4 or above ERRCODE
	Algo       VectorIndexSearchIf
}

func (s *VectorIndexSearch) Destroy() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.Algo.Destroy()
	// destroyed
	s.Status.Store(STATUS_DESTROYED)
}

func (s *VectorIndexSearch) Load(proc *process.Process) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	err := s.Algo.Load(proc)
	if err != nil {
		// load error
		s.Status.Store(STATUS_ERROR)
		return err
	}
	// Loaded
	s.Status.Store(STATUS_LOADED)
	s.extend(true)
	return nil
}

func (s *VectorIndexSearch) Expired() bool {
	//s.Mutex.RLock()
	//defer s.Mutex.RUnlock()

	ts := s.ExpireAt.Load()
	now := time.Now().UnixMicro()
	return (ts > 0 && ts < now)
}

func (s *VectorIndexSearch) extend(update bool) {
	now := time.Now()
	if update {
		s.LastUpdate.Store(now.UnixMicro())
	}
	ts := time.Now().Add(VectorIndexCacheTTL).UnixMicro()
	s.ExpireAt.Store(ts)
}

func (s *VectorIndexSearch) Search(proc *process.Process, newalgo VectorIndexSearchIf, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	s.Mutex.RLock()

	for s.Status.Load() == 0 {
		s.Mutex.RUnlock()
		time.Sleep(time.Millisecond)
		s.Mutex.RLock()
	}
	defer s.Mutex.RUnlock()

	status := s.Status.Load()
	if status >= STATUS_DESTROYED {
		if status == STATUS_DESTROYED {
			return nil, nil, moerr.NewInternalErrorNoCtx("Index destroyed")
		} else {
			return nil, nil, moerr.NewInternalErrorNoCtx("Load index error")
		}
	}

	// if error mark as outdated
	err = s.Algo.UpdateConfig(newalgo)
	if err != nil {
		s.Status.Store(STATUS_OUTDATED)
	}

	s.extend(false)
	return s.Algo.Search(proc, query, rt)
}

// implementation of VectorIndexCache
type VectorIndexCache struct {
	IndexMap       sync.Map
	TickerInterval time.Duration
	ticker         *time.Ticker
	done           chan bool
	sigc           chan os.Signal
	started        atomic.Bool
	exited         atomic.Bool
	once           sync.Once
}

func NewVectorIndexCache() *VectorIndexCache {
	c := &VectorIndexCache{}
	c.TickerInterval = VectorIndexCacheTTL / 2
	return c
}

func (c *VectorIndexCache) serve() {
	if c.started.Load() {
		return
	}

	// try clean up the temp directory. set tempdir to /tmp/hnsw
	c.ticker = time.NewTicker(c.TickerInterval)
	c.done = make(chan bool)
	c.sigc = make(chan os.Signal, 3)
	signal.Notify(c.sigc, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	// channel initizalized.  set started to true
	c.started.Store(true)

	go func() {
		defer c.ticker.Stop()
		for {
			select {
			case <-c.done:
				c.exited.Store(true)
				return
			case <-c.sigc:
				// sig can be syscall.SIGTERM or syscall.SIGINT
				c.exited.Store(true)
				c.Destroy()
				return
			case <-c.ticker.C:
				// delete expired index
				c.HouseKeeping()
			}
		}
	}()
}

// initialize the Cache and only call once
func (c *VectorIndexCache) Once() {
	c.once.Do(func() { c.serve() })
}

// house keeping to check expired keys and delete from cache
func (c *VectorIndexCache) HouseKeeping() {

	expiredkeys := make([]string, 0, 16)

	c.IndexMap.Range(func(key, value any) bool {
		algo := value.(*VectorIndexSearch)
		if algo.Expired() || algo.Status.Load() == STATUS_OUTDATED {
			expiredkeys = append(expiredkeys, key.(string))
		}
		return true
	})

	for _, k := range expiredkeys {
		value, loaded := c.IndexMap.LoadAndDelete(k)
		if loaded {
			algo := value.(*VectorIndexSearch)
			algo.Destroy()
			algo = nil
		}
	}
}

// destroy the cache
func (c *VectorIndexCache) Destroy() {
	if c.started.Load() {
		//c.ticker.Stop()
		if !c.exited.Load() {
			c.done <- true
		}
	}
	// remove all keys
	c.IndexMap.Range(func(key, value any) bool {
		c.IndexMap.Delete(key)
		algo := value.(*VectorIndexSearch)
		algo.Destroy()
		algo = nil
		return true
	})
}

// Get index from cache and return VectorIndexSearchIf interface
func (c *VectorIndexCache) Search(proc *process.Process, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	value, loaded := c.IndexMap.LoadOrStore(key, &VectorIndexSearch{Algo: newalgo})
	algo := value.(*VectorIndexSearch)
	if !loaded {
		// load model from database and if error during loading, remove the entry from gIndexMap
		err := algo.Load(proc)
		if err != nil {
			c.IndexMap.Delete(key)
			return nil, nil, err
		}
	}
	return algo.Search(proc, newalgo, query, rt)
}

// remove key from cache
func (c *VectorIndexCache) Remove(key string) {
	value, loaded := c.IndexMap.LoadAndDelete(key)
	if loaded {
		algo := value.(*VectorIndexSearch)
		algo.Destroy()
		algo = nil
	}
}
