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
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hnsw"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func TestCache(t *testing.T) {
	proc := testutil.NewProcess()

	Cache.ticker_interval = 5 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	idxcfg := usearch.DefaultConfig(8)
	idxcfg.Metric = usearch.L2sq
	tblcfg := hnsw.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	os.Stderr.WriteString("cache getindex\n")
	idx, err := Cache.GetIndex(proc, idxcfg, tblcfg, tblcfg.IndexTable)
	require.Nil(t, err)

	os.Stderr.WriteString("cache search\n")
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	keys, distances, err := idx.Search(fp32a)
	require.Nil(t, err)
	_ = keys
	_ = distances

	os.Stderr.WriteString("cache sleep\n")
	time.Sleep(12 * time.Second)

	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}
