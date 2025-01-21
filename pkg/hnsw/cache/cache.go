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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/hnsw"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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

func (h *HnswSearch) Search(v []float32) (keys []int64, distances []float32, err error) {
	h.Mutex.RLock()
	defer h.Mutex.RUnlock()
	if h.Indexes == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("HNSW cannot find index from database.")
	}

	ts := time.Now().Add(time.Hour).Unix()
	h.ExpireAt.Store(ts)

	// search
	return nil, nil, nil
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

func (h *HnswSearch) Expired() bool {
	h.Mutex.RLock()
	defer h.Mutex.RUnlock()

	ts := h.ExpireAt.Load()
	now := time.Now().Unix()
	return (ts < now)
}

func (s *HnswSearch) LoadMetadata(proc *process.Process) ([]*HnswSearchIndex, error) {

	sql := fmt.Sprintf("SELECT * FROM `%s`.`%s`", s.Tblcfg.DbName, s.Tblcfg.MetadataTable)
	res, err := runSql(proc, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	total := 0
	for _, bat := range res.Batches {
		total += bat.RowCount()
	}

	indexes := make([]*HnswSearchIndex, 0, total)
	for _, bat := range res.Batches {
		idVec := bat.Vecs[0]
		chksumVec := bat.Vecs[1]
		tsVec := bat.Vecs[2]
		for i := 0; i < bat.RowCount(); i++ {
			id := vector.GetFixedAtWithTypeCheck[int64](idVec, i)
			chksum := chksumVec.GetStringAt(i)
			ts := vector.GetFixedAtWithTypeCheck[int64](tsVec, i)

			idx := &HnswSearchIndex{Id: id, Checksum: chksum, Timestamp: ts}
			indexes = append(indexes, idx)
			os.Stderr.WriteString(fmt.Sprintf("Meta %d %d %s\n", id, ts, chksum))
		}
	}

	return indexes, nil
}

func (s *HnswSearch) LoadIndex(proc *process.Process, indexes []*HnswSearchIndex) ([]*HnswSearchIndex, error) {

	return nil, nil
}

func (s *HnswSearch) LoadFromDatabase(proc *process.Process) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// load metadata
	indexes, err := s.LoadMetadata(proc)
	if err != nil {
		return err
	}

	// load index model
	indexes, err = s.LoadIndex(proc, indexes)
	if err != nil {
		return err
	}

	s.Indexes = indexes

	ts := time.Now().Add(time.Hour).Unix()
	s.ExpireAt.Store(ts)

	return nil
}

type HnswCache struct {
	IndexMap       sync.Map
	TickerInterval time.Duration
	ticker         *time.Ticker
	done           chan bool
	sigc           chan os.Signal
	started        atomic.Bool
	exited         atomic.Bool
	once           sync.Once
}

func NewHnswCache() *HnswCache {
	c := &HnswCache{}
	c.TickerInterval = time.Hour
	return c
}

func (c *HnswCache) serve() {
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

func (c *HnswCache) Once() {
	c.once.Do(func() { c.serve() })
}

func (c *HnswCache) HouseKeeping() {

	os.Stderr.WriteString("house keeping\n")
	expiredkeys := make([]string, 0, 16)

	c.IndexMap.Range(func(key, value any) bool {

		search := value.(*HnswSearch)
		if search.Expired() {
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
		if !c.exited.Load() {
			c.done <- true
		}
	}
}

func (c *HnswCache) GetIndex(proc *process.Process, cfg usearch.IndexConfig, tblcfg hnsw.IndexTableConfig, key string) (*HnswSearch, error) {
	value, loaded := c.IndexMap.LoadOrStore(key, &HnswSearch{Idxcfg: cfg, Tblcfg: tblcfg})
	if !loaded {
		idx := value.(*HnswSearch)
		// load model from database and if error during loading, remove the entry from gIndexMap
		err := idx.LoadFromDatabase(proc)
		if err != nil {
			c.IndexMap.Delete(key)
			return nil, err
		}

		return idx, nil
	}
	return value.(*HnswSearch), nil
}

var runSql = runSql_fn

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func runSql_fn(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}

var runSql_streaming = runSql_streaming_fn

// run SQL in WithStreaming() and pass the channel to SQL executor
func runSql_streaming_fn(proc *process.Process, sql string, stream_chan chan executor.Result, error_chan chan error) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId).
		WithStreaming(stream_chan, error_chan)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}
