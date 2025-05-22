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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"

	usearch "github.com/unum-cloud/usearch/golang"
)

/*
// give metadata [index_id, checksum, timestamp]
func mock_runSql(proc *process.Process, sql string) (executor.Result, error) {

	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeMetaBatch(proc)}}, nil
}

// give blob
func mock_runSql_streaming(proc *process.Process, sql string, ch chan executor.Result, err_chan chan error) (executor.Result, error) {

	defer close(ch)
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc)}}
	ch <- res
	return executor.Result{}, nil
}
*/

func doModelSearchTest(t *testing.T, idx *HnswModel, key uint64, v []float32) {
	keys, distances, err := idx.Search(v, 4)
	require.Nil(t, err)
	require.Equal(t, len(keys), 4)
	require.Equal(t, keys[0], key)
	require.Equal(t, distances[0], float32(0))
	fmt.Printf("%v %v\n", keys, distances)

}

func TestModel(t *testing.T) {
	var err error
	fp32a := []float32{0, 1, 2}
	v1000 := []float32{1000, 2000, 3000}

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool("", m)

	// stub runSql function
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming

	models, err := LoadMetadata(proc, "db", "meta")
	require.Nil(t, err)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	require.Equal(t, len(models), 1)
	idx := models[0]
	defer idx.Destroy()

	err = idx.LoadIndex(proc, idxcfg, tblcfg, 0, false)
	require.Nil(t, err)

	doModelSearchTest(t, idx, 0, fp32a)

	require.Equal(t, idx.Dirty, false)

	err = idx.Unload()
	require.Nil(t, err)

	err = idx.LoadIndex(proc, idxcfg, tblcfg, 0, false)
	require.Nil(t, err)

	doModelSearchTest(t, idx, 0, fp32a)

	var found bool
	found, err = idx.Contains(0)
	require.Nil(t, err)
	require.Equal(t, found, true)

	found, err = idx.Contains(1000)
	require.Nil(t, err)
	require.Equal(t, found, false)

	err = idx.Add(1000, v1000)
	require.Nil(t, err)

	require.Equal(t, idx.Dirty, true)

	err = idx.Unload()
	require.Nil(t, err)

	err = idx.LoadIndex(proc, idxcfg, tblcfg, 0, false)
	require.Nil(t, err)

	doModelSearchTest(t, idx, 1000, v1000)

}
