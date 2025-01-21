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
	"github.com/matrixorigin/matrixone/pkg/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

var Cache *HnswCache = NewHnswCache()

type HnswSearchIndex struct {
	Id        int64
	Path      string
	Index     *usearch.Index
	Timestamp int64
	Checksum  string
}

type HnswSearch struct {
	Mutex    sync.RWMutex
	Indexes  []*HnswSearchIndex
	ExpireAt atomic.Int64
	Idxcfg   usearch.IndexConfig
	Tblcfg   hnsw.IndexTableConfig
}

func (h *HnswSearch) Search(v []float32) error {
	h.Mutex.RLock()
	defer h.Mutex.RUnlock()
	if h.Indexes == nil {
		return moerr.NewInternalErrorNoCtx("HNSW cannot find index from database")
	}

	ts := time.Now().Add(time.Hour).Unix()
	h.ExpireAt.Store(ts)

	// search
	return nil
}

func (h *HnswSearch) Destroy() {
	h.Mutex.Lock()
	defer h.Mutex.Unlock()
	// destroy index
	for _, idx := range h.Indexes {
		idx.Index.Destroy()
	}
	h.Indexes = nil
}

type HnswCache struct {
	IndexMap        sync.Map
	ticker          *time.Ticker
	done            chan bool
	sigc            chan os.Signal
	ticker_interval time.Duration
	started         atomic.Bool
}

func NewHnswCache() *HnswCache {
	c := &HnswCache{}
	c.ticker_interval = time.Hour
	return c
}

func (c *HnswCache) Serve() {
	if c.started.Load() {
		return
	}

	// try clean up the temp directory. set tempdir to /tmp/hnsw

	os.Stderr.WriteString("Serve start\n")
	c.ticker = time.NewTicker(c.ticker_interval)
	c.done = make(chan bool)
	c.sigc = make(chan os.Signal, 3)
	signal.Notify(c.sigc, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	// channel initizalized.  set started to true
	c.started.Store(true)

	go func() {
		for {
			select {
			case <-c.done:
				os.Stderr.WriteString("done handled...\n")
				return
			case <-c.sigc:
				// sig can be syscall.SIGTERM or syscall.SIGINT
				os.Stderr.WriteString("signal handled...\n")
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

func (c *HnswCache) HouseKeeping() {

	os.Stderr.WriteString("house keeping\n")
	expiredkeys := make([]string, 0, 16)

	c.IndexMap.Range(func(key, value any) bool {

		search := value.(*HnswSearch)
		search.Mutex.RLock()
		defer search.Mutex.RUnlock()

		ts := search.ExpireAt.Load()
		now := time.Now().Unix()
		if ts < now {
			expiredkeys = append(expiredkeys, key.(string))
		}
		return true
	})

	for _, k := range expiredkeys {
		value, loaded := c.IndexMap.LoadAndDelete(k)
		if loaded {
			search := value.(*HnswSearch)
			// destroy the usearch indexes
			search.Destroy()
			search = nil
		}
	}
	os.Stderr.WriteString("house keeping end\n")
}

func (c *HnswCache) Destroy() {
	if c.started.Load() {
		c.ticker.Stop()
		c.done <- true
	}
}

func (c *HnswCache) GetIndex(proc *process.Process, cfg usearch.IndexConfig, tblcfg hnsw.IndexTableConfig, key string) (*HnswSearch, error) {
	value, loaded := c.IndexMap.LoadOrStore(key, &HnswSearch{})
	if !loaded {
		idx := value.(*HnswSearch)
		idx.Mutex.Lock()
		defer idx.Mutex.Unlock()

		// load model from database and if error during loading, remove the entry from gIndexMap
		idx.Idxcfg = cfg
		idx.Tblcfg = tblcfg

		return idx, nil
	}
	return value.(*HnswSearch), nil
}
