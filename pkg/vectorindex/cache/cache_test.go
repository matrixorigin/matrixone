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
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

type MockSearch struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

func (m *MockSearch) Search(query []float32, limit uint) (keys []int64, distances []float32, err error) {
	time.Sleep(2 * time.Millisecond)
	return []int64{1}, []float32{2.0}, nil
}

func (m *MockSearch) Destroy() {
}

func (m *MockSearch) Load(*process.Process) error {
	time.Sleep(2 * time.Second)
	return nil
}

// Load Error
type MockSearchLoadError struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

func (m *MockSearchLoadError) Search(query []float32, limit uint) (keys []int64, distances []float32, err error) {
	return []int64{1}, []float32{2.0}, nil
}

func (m *MockSearchLoadError) Destroy() {

}

func (m *MockSearchLoadError) Load(*process.Process) error {
	return moerr.NewInternalErrorNoCtx("Load from database error")
}

// Search Error
type MockSearchSearchError struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

func (m *MockSearchSearchError) Search(query []float32, limit uint) (keys []int64, distances []float32, err error) {
	return nil, nil, moerr.NewInternalErrorNoCtx("Search error")
}

func (m *MockSearchSearchError) Destroy() {

}

func (m *MockSearchSearchError) Load(*process.Process) error {
	return nil
}

func TestCache(t *testing.T) {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())

	VectorIndexCacheTTL = 5 * time.Second
	VectorIndexCacheTTL = 5 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 5 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	os.Stderr.WriteString("cache getindex\n")
	m := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
	os.Stderr.WriteString("cache search\n")
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	keys, distances, err := Cache.Search(proc, tblcfg.IndexTable, m, fp32a, 4)
	require.Nil(t, err)
	require.Equal(t, len(keys), 1)
	require.Equal(t, keys[0], int64(1))
	require.Equal(t, distances[0], float32(2.0))

	os.Stderr.WriteString("cache sleep\n")
	time.Sleep(8 * time.Second)

	// cache expired

	// new search
	m3 := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
	keys, distances, err = Cache.Search(proc, tblcfg.IndexTable, m3, fp32a, 4)
	require.Nil(t, err)
	require.Equal(t, len(keys), 1)
	require.Equal(t, keys[0], int64(1))
	require.Equal(t, distances[0], float32(2.0))

	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheConcurrent(t *testing.T) {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())

	VectorIndexCacheTTL = 2 * time.Second
	VectorIndexCacheTTL = 2 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 1 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	time.Sleep(1999 * time.Millisecond)
	var wg sync.WaitGroup
	nthread := 8
	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
				idxcfg.Usearch.Metric = usearch.L2sq
				tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
				//os.Stderr.WriteString("cache getindex\n")
				m := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
				//os.Stderr.WriteString("cache search\n")
				fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
				keys, distances, err := Cache.Search(proc, tblcfg.IndexTable, m, fp32a, 4)
				require.Nil(t, err)
				require.Equal(t, len(keys), 1)
				require.Equal(t, keys[0], int64(1))
				require.Equal(t, distances[0], float32(2.0))
			}
		}()
	}

	wg.Wait()
	time.Sleep(4 * time.Second)

	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheLoadError(t *testing.T) {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())

	VectorIndexCacheTTL = 5 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 5 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	os.Stderr.WriteString("cache getindex\n")
	m1 := &MockSearchLoadError{Idxcfg: idxcfg, Tblcfg: tblcfg}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	_, _, err := Cache.Search(proc, tblcfg.IndexTable, m1, fp32a, 4)
	require.NotNil(t, err)

	os.Stderr.WriteString(fmt.Sprintf("error : %v\n", err))
	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheSearchError(t *testing.T) {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())

	VectorIndexCacheTTL = 5 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 5 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	os.Stderr.WriteString("cache getindex\n")
	m1 := &MockSearchSearchError{Idxcfg: idxcfg, Tblcfg: tblcfg}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	_, _, err := Cache.Search(proc, tblcfg.IndexTable, m1, fp32a, 4)
	require.NotNil(t, err)

	os.Stderr.WriteString(fmt.Sprintf("error : %v\n", err))
	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}
