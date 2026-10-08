// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cache

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"

	usearch "github.com/unum-cloud/usearch/golang"
)

// raceEmptyGenSearch reports EmptyGeneration from the SAME field its Destroy mutates. The
// cache must read the empty-generation flag under the entry's read lock (captured inside
// Search/SearchInto), so it is mutually exclusive with DestroyWithReason's write lock. If the
// read ever moves outside that lock again, -race sees the read here collide with the write in
// Destroy. emptyGenSearch's inherited no-op Destroy cannot expose this.
//
// Search rendezvouses so the evictor is guaranteed in flight (parked on the write lock) while
// the search holds the read lock, and Destroy writes a burst so an unlocked read has a wide
// window to collide with -- otherwise a single nanosecond write almost never overlaps a single
// nanosecond read and the regression has no teeth.
type raceEmptyGenSearch struct {
	MockSearch
	empty   bool
	entered chan struct{}
	release chan struct{}
}

func (m *raceEmptyGenSearch) EmptyGeneration() bool { return m.empty }

func (m *raceEmptyGenSearch) Destroy() {
	for i := 0; i < 4096; i++ {
		m.empty = !m.empty
	}
}

func (m *raceEmptyGenSearch) Search(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (any, []float64, error) {
	if m.entered != nil {
		m.entered <- struct{}{}
		<-m.release
	}
	return m.MockSearch.Search(sqlproc, query, rt)
}

func (m *raceEmptyGenSearch) SearchInto(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error {
	if m.entered != nil {
		m.entered <- struct{}{}
		<-m.release
	}
	return m.MockSearch.SearchInto(sqlproc, query, rt, out)
}

// TestEmptyGenerationDestroyRace runs a loader Search against a concurrent evicting Destroy on
// the same entry, many times. The evictor parks on the write lock while the search holds the
// read lock, so its Destroy write lands the instant the search releases -- exactly when the old
// unlocked algoEmptyGeneration read fired. With the flag captured under the read lock, the two
// accesses to raceEmptyGenSearch.empty are serialized and -race stays clean.
func TestEmptyGenerationDestroyRace(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	const iterations = 100
	Cache = NewVectorIndexCache()
	t.Cleanup(func() { Cache.Destroy() })

	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("race_idx_%d", i)
		algo := &raceEmptyGenSearch{MockSearch: MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}, entered: make(chan struct{}), release: make(chan struct{})}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, _, err := Cache.Search(sqlproc, key, algo, fp32a, vectorindex.RuntimeConfig{Limit: 4}); err != nil && !errors.Is(err, errIndexDestroyed) {
				t.Errorf("Search: unexpected error: %v", err)
			}
		}()
		<-algo.entered // the search holds the read lock and is parked inside Algo.Search
		go func() {
			defer wg.Done()
			Cache.Remove(key)
		}()
		for {
			if _, ok := Cache.IndexMap.Load(key); !ok { // the evictor has claimed the entry and is parked on the write lock
				break
			}
			runtime.Gosched()
		}
		close(algo.release) // the search returns and drops the read lock; the evictor's Destroy runs
		wg.Wait()
	}
}

// TestEmptyGenerationDestroyRaceSearchInto is the box-free twin: the SearchInto path captures
// and reads the empty-generation flag under the same read lock.
func TestEmptyGenerationDestroyRaceSearchInto(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	const iterations = 100
	Cache = NewVectorIndexCache()
	t.Cleanup(func() { Cache.Destroy() })

	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("race_into_%d", i)
		algo := &raceEmptyGenSearch{MockSearch: MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}, entered: make(chan struct{}), release: make(chan struct{})}
		var out vectorindex.SearchOutput
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := Cache.SearchInto(sqlproc, key, algo, fp32a, vectorindex.RuntimeConfig{Limit: 4}, &out); err != nil && !errors.Is(err, errIndexDestroyed) {
				t.Errorf("SearchInto: unexpected error: %v", err)
			}
		}()
		<-algo.entered
		go func() {
			defer wg.Done()
			Cache.Remove(key)
		}()
		for {
			if _, ok := Cache.IndexMap.Load(key); !ok {
				break
			}
			runtime.Gosched()
		}
		close(algo.release)
		wg.Wait()
	}
}
