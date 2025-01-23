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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

type MockSearch struct {
	VectorIndexSearch
}

func (m *MockSearch) Search(query []float32, limit uint) (keys []int64, distances []float32, err error) {
	return []int64{1}, []float32{2.0}, nil
}

func (m *MockSearch) Destroy() {

}

func (m *MockSearch) Expired() bool {
	now := time.Now().UnixMicro()
	expireat := m.ExpireAt.Load()

	os.Stderr.WriteString(fmt.Sprintf("now %d, expire %d\n", now, expireat))
	return (expireat > 0 && expireat < now)
}

func (m *MockSearch) LoadFromDatabase(*process.Process) error {
	ts := time.Now().Add(VectorIndexCacheTTL).UnixMicro()
	m.ExpireAt.Store(ts)
	return nil
}

func TestCache(t *testing.T) {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())

	VectorIndexCacheTTL = 5 * time.Second
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
	m1 := &MockSearch{VectorIndexSearch: VectorIndexSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}}
	idx, err := Cache.GetIndex(proc, tblcfg.IndexTable, m1)
	require.Nil(t, err)
	require.Equal(t, m1, idx)

	idx2, err := Cache.GetIndex(proc, tblcfg.IndexTable, &MockSearch{VectorIndexSearch: VectorIndexSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}})
	require.Nil(t, err)
	require.Equal(t, idx, idx2)

	os.Stderr.WriteString("cache search\n")
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	keys, distances, err := idx.Search(fp32a, 4)
	require.Nil(t, err)
	require.Equal(t, len(keys), 1)
	require.Equal(t, keys[0], int64(1))
	require.Equal(t, distances[0], float32(2.0))

	os.Stderr.WriteString("cache sleep\n")
	time.Sleep(8 * time.Second)

	// cache expired
	m3 := &MockSearch{VectorIndexSearch: VectorIndexSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}}
	idx3, err := Cache.GetIndex(proc, tblcfg.IndexTable, m3)
	require.Nil(t, err)
	require.Equal(t, m3, idx3)

	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}
