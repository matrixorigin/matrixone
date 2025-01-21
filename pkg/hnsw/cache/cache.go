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

var Cache HsnwCache

// start cache here
func init() {
	Cache.Serve()
}

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

	time.Now().Add(time.Hour)
	h.ExpireAt.Store(time.Unix)

	// search
	return nil
}

func (h *HnswSearch) Destroy() {
	h.Mutex.Lock()
	defer h.Mutex.Unlock()
	// TODO: destroy index
	for _, idx := range h.Indexes {
		idx.Destroy()
	}
	h.Indexes = nil
}

type HnswCache struct {
	IndexMap        sync.Map
	ticker          *time.Ticker
	done            chan bool
	sigc            chan os.Signal
	ticker_interval int64
}

func (c *HnswCache) Serve() {
	// try clean up the temp directory. set tempdir to /tmp/hnsw

	// start the ticker
	c.ticker_interval = time.Hour
	c.ticker = time.NewTicker(c.ticker_interval)
	c.sigc := make(chan os.Signal, 2)
	signal.Notify(c.sigc, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			select {
			case <-c.done:
				return
			case sig := <-c.sigc:
				// sig can be syscall.SIGTERM or syscall.SIGINT
				return
			case t := <-c.ticker.C:
				// delete expired index
				c.HouseKeeping()
			}
		}
	}()
}

func (c *HnswCache) HouseKeeping() {

	expiredkeys := make([]string, 0, 16)

	c.IndexMap.Range(func(key, value any) bool {

		search := value.(*HnswSearch)
		search.Mutex.RLock()
		defer search.Mutex.RUnlock()

		ts := search.ExpiredAt.Load()
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
}

func (c *HnswCache) Destroy() {
	c.ticker.Stop()
	c.done <- true

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
