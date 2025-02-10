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

package hnsw

import (
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"

	usearch "github.com/unum-cloud/usearch/golang"
)

func TestBuild(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool("", m)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	build, err := NewHnswBuild(proc, idxcfg, tblcfg)
	require.Nil(t, err)
	defer build.Destroy()

	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			nitem := 500000
			for i := 0; i < nitem; i++ {
				v := []float32{float32(i), float32(i + 1), float32(i + 2)}
				err := build.Add(int64(j*nitem+i), v)
				require.Nil(t, err)
			}
		}()
	}

	wg.Wait()

	sqls, err := build.ToInsertSql(time.Now().UnixMicro())
	require.Nil(t, err)
	require.Equal(t, len(sqls), 51)
	//fmt.Printf("SQLS %v\n", sqls)
	//fmt.Printf("LENF %d\n", len(sqls))

}

func TestBuildIndex(t *testing.T) {
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = 100
	//tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	idx, err := NewHnswBuildIndex(0, idxcfg, 1)
	require.Nil(t, err)

	empty, err := idx.Empty()
	require.Nil(t, err)
	require.Equal(t, empty, true)
}
