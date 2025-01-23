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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

var (
	VectorIndexCacheTTL time.Duration     = 30 * time.Minute
	Cache               *VectorIndexCache = NewVectorIndexCache()
)

type VectorIndexSearchIf interface {
	GetParent() VectorIndexSearch
	Search(query []float32, limit uint) (keys []int64, distances []float32, err error)
	Destroy()
	Expired() bool
}

type VectorIndexSearch struct {
	Mutex      sync.RWMutex
	ExpireAt   atomic.Int64
	LastUpdate atomic.Int64
	Idxcfg     usearch.IndexConfig
	Tblcfg     hnsw.IndexTableConfig
}

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

func (c *VectorIndexCache) Once() {
	c.once.Do(func() { c.serve() })
}

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

func (c *VectorIndexCache) GetIndex(proc *process.Process, key string, def VectorIndexSearchIf) (VectorIndexSearchIf, error) {
	value, loaded := c.IndexMap.LoadOrStore(key, def)
	if !loaded {
		// load model from database and if error during loading, remove the entry from gIndexMap
		switch search := value.(type) {
		case *HnswSearch:
			err := search.LoadFromDatabase(proc)
			if err != nil {
				c.IndexMap.Delete(key)
				return nil, err
			}
			return value.(VectorIndexSearchIf), nil
		default:
			return nil, moerr.NewInternalError(proc.Ctx, "unsupported VectorIndexSearch type")
		}

	}

	return value.(VectorIndexSearchIf), nil
}

func (c *VectorIndexCache) Remove(key string) {
	value, loaded := c.IndexMap.LoadAndDelete(key)
	if loaded {
		search := value.(VectorIndexSearchIf)
		search.Destroy()
		search = nil
	}
}
