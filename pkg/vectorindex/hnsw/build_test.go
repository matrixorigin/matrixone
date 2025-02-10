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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"

	usearch "github.com/unum-cloud/usearch/golang"
)

func TestBuild(t *testing.T) {
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	build, err := NewHnswBuild(idxcfg, tblcfg)
	require.Nil(t, err)
	defer build.Destroy()

	for i := 0; i < 500000; i++ {
		v := []float32{float32(i), float32(i + 1), float32(i + 2)}
		err := build.Add(int64(i), v)
		require.Nil(t, err)
	}

	sqls, err := build.ToInsertSql(time.Now().UnixMicro())
	require.Nil(t, err)
	require.Equal(t, len(sqls), 6)
	//fmt.Printf("SQLS %v\n", sqls)
	//fmt.Printf("LENF %d\n", len(sqls))

}

func TestBuildIndex(t *testing.T) {
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = 100
	//tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	idx, err := NewHnswBuildIndex(0, idxcfg)
	require.Nil(t, err)

	empty, err := idx.Empty()
	require.Nil(t, err)
	require.Equal(t, empty, true)
}
