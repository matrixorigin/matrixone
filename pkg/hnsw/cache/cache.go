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
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hnsw"
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

var (
	VectorIndexCacheTTL time.Duration     = 30 * time.Minute
	Cache               *VectorIndexCache = NewVectorIndexCache()
)

// Various vector index algorithm wants to share with VectorIndexCache need to implement VectorIndexSearchIf interface (see HnswSearch)
type VectorIndexSearchIf interface {
	Search(query []float32, limit uint) (keys []int64, distances []float32, err error)
	Destroy()
	Expired() bool
	LoadFromDatabase(*process.Process) error
}

// base VectorIndex Search structure for VectorIndexSearchIf (see HnswSearch)
type VectorIndexSearch struct {
	Mutex      sync.RWMutex
	ExpireAt   atomic.Int64
	LastUpdate atomic.Int64
	Idxcfg     hnsw.IndexConfig
	Tblcfg     hnsw.IndexTableConfig
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

	os.Stderr.WriteString("Serve start\n")
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
				os.Stderr.WriteString("done handled...\n")
				c.exited.Store(true)
				return
			case <-c.sigc:
				// sig can be syscall.SIGTERM or syscall.SIGINT
				os.Stderr.WriteString("signal handled...\n")
				c.exited.Store(true)
				return
			case <-c.ticker.C:
				os.Stderr.WriteString("ticker...\n")
				// delete expired index
				c.HouseKeeping()
			}
		}
		os.Stderr.WriteString("go func exited\n")
	}()
	os.Stderr.WriteString("Serve end\n")
}

// initialize the Cache and only call once
func (c *VectorIndexCache) Once() {
	c.once.Do(func() { c.serve() })
}

// house keeping to check expired keys and delete from cache
func (c *VectorIndexCache) HouseKeeping() {

	os.Stderr.WriteString("house keeping\n")
	expiredkeys := make([]string, 0, 16)

	c.IndexMap.Range(func(key, value any) bool {
		search := value.(VectorIndexSearchIf)
		if search.Expired() {
			expiredkeys = append(expiredkeys, key.(string))
		}
		return true
	})

	for _, k := range expiredkeys {
		value, loaded := c.IndexMap.LoadAndDelete(k)
		if loaded {
			search := value.(VectorIndexSearchIf)
			os.Stderr.WriteString(fmt.Sprintf("HouseKeep: key %s deleted\n", k))
			search.Destroy()
			search = nil
		}
	}
	os.Stderr.WriteString("house keeping end\n")
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
		search := value.(VectorIndexSearchIf)
		search.Destroy()
		search = nil
		return true
	})
}

// Get index from cache and return VectorIndexSearchIf interface
func (c *VectorIndexCache) GetIndex(proc *process.Process, key string, def VectorIndexSearchIf) (VectorIndexSearchIf, error) {
	value, loaded := c.IndexMap.LoadOrStore(key, def)
	search := value.(VectorIndexSearchIf)
	if !loaded {
		// load model from database and if error during loading, remove the entry from gIndexMap
		err := search.LoadFromDatabase(proc)
		if err != nil {
			c.IndexMap.Delete(key)
			return nil, err
		}
		return search, nil
	}

	return search, nil
}

// remove key from cache
func (c *VectorIndexCache) Remove(key string) {
	value, loaded := c.IndexMap.LoadAndDelete(key)
	if loaded {
		search := value.(VectorIndexSearchIf)
		search.Destroy()
		search = nil
	}
}
