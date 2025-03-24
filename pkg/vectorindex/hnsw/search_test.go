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
	"os"
	"sync"
	"testing"
	"time"

	fallocate "github.com/detailyang/go-fallocate"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"

	usearch "github.com/unum-cloud/usearch/golang"
)

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

func TestHnsw(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool("", m)

	// stub runSql function
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming

	// init cache
	cache.VectorIndexCacheTTL = 2 * time.Second
	cache.VectorIndexCacheTTL = 2 * time.Second
	cache.Cache = cache.NewVectorIndexCache()

	time.Sleep(1999 * time.Millisecond)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	fp32a := []float32{0, 1, 2}

	var wg sync.WaitGroup
	nthread := 64

	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20000; j++ {
				cache.Cache.Once()

				algo := NewHnswSearch(idxcfg, tblcfg)
				anykeys, distances, err := cache.Cache.Search(proc, tblcfg.IndexTable, algo, fp32a, vectorindex.RuntimeConfig{Limit: 4})
				require.Nil(t, err)
				keys, ok := anykeys.([]int64)
				require.True(t, ok)

				require.Equal(t, len(keys), 4)
				require.Equal(t, keys[0], int64(0))
				require.Equal(t, distances[0], float64(0))
				//os.Stderr.WriteString(fmt.Sprintf("keys %v distance %v\n", keys, distances))
			}
		}()
	}

	wg.Wait()

	time.Sleep(3 * time.Second)
	cache.Cache.Destroy()
}

func makeMetaBatch(proc *process.Process) *batch.Batch {
	indexfile := "resources/hnsw0.bin"

	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))   // index_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 65536, 0)) // checksum
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))       // timestamp
	bat.Vecs[3] = vector.NewVec(types.New(types.T_int64, 8, 0))       // timestamp

	vector.AppendBytes(bat.Vecs[0], []byte("abc-0"), false, proc.Mp())
	chksum, err := vectorindex.CheckSum(indexfile)
	if err != nil {
		panic("file checksum error")
	}

	finfo, err := os.Stat(indexfile)
	if err != nil {
		panic("file not found")
	}

	vector.AppendBytes(bat.Vecs[1], []byte(chksum), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], int64(0), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[3], finfo.Size(), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

func makeIndexBatch(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))    // chunk_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_blob, 65536, 0)) // data

	dat, err := os.ReadFile("resources/hnsw0.bin")
	if err != nil {
		panic("read file error")
	}
	vector.AppendFixed[int64](bat.Vecs[0], int64(0), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], dat, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

func TestFallocate(t *testing.T) {

	f, err := os.Create("apple")
	require.Nil(t, err)
	fallocate.Fallocate(f, 0, 10000)
	f.Close()
}
