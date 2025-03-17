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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	usearch "github.com/unum-cloud/usearch/golang"
)

const MaxIndexCapacity = 100000

func TestBuildMulti(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool("", m)

	ndim := 32
	nthread := 8
	total := 200000
	nitem := total / nthread // vectorindex.MaxIndexCapacity

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(uint(ndim))}
	idxcfg.Usearch.Metric = usearch.L2sq
	//idxcfg.Usearch.Quantization = usearch.F32
	idxcfg.Usearch.Connectivity = 48 // default 16
	//idxcfg.Usearch.ExpansionAdd = 128   // default 128
	//idxcfg.Usearch.ExpansionSearch = 30 // default 64
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src",
		MetadataTable: "__secondary_meta", IndexTable: "__secondary_index",
		ThreadsSearch: int64(nthread),
		ThreadsBuild:  int64(nthread),
		IndexCapacity: MaxIndexCapacity}

	uid := fmt.Sprintf("%s:%d:%d", "localhost", 1, 0)
	build, err := NewHnswBuild(proc, uid, 1, idxcfg, tblcfg)

	require.Nil(t, err)
	defer build.Destroy()

	// fix the seek
	r := rand.New(rand.NewSource(99))

	// create sample date
	sample := make([][]float32, nitem*nthread)
	for i := 0; i < nthread*nitem; i++ {
		sample[i] = make([]float32, ndim)
		for j := 0; j < ndim; j++ {
			sample[i][j] = r.Float32()
		}
	}

	fmt.Printf("sample created\n")

	start := time.Now()

	var wg sync.WaitGroup
	for j := 0; j < nthread; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < nitem; i++ {
				key := j*nitem + i
				err := build.Add(int64(key), sample[key])
				require.Nil(t, err)
			}
		}()
	}
	wg.Wait()

	end := time.Now()

	fmt.Printf("Build Time %f sec\n", end.Sub(start).Seconds())

	fmt.Printf("model built\n")
	_, err = build.ToInsertSql(time.Now().UnixMicro())
	require.Nil(t, err)
	indexes := build.GetIndexes()

	fmt.Printf("model search\n")
	// load index file and search
	search := NewHnswSearch(idxcfg, tblcfg)
	defer search.Destroy()

	// test Contains with no indexes
	found, err := search.Contains(int64(nthread*nitem + 1))
	require.Nil(t, err)
	require.False(t, found)

	fmt.Printf("load model from files\n")
	// load index
	search.Indexes = make([]*HnswSearchIndex, len(indexes))
	for i, idx := range indexes {
		sidx := &HnswSearchIndex{}
		sidx.Index, err = usearch.NewIndex(idxcfg.Usearch)
		require.Nil(t, err)

		err = sidx.Index.ChangeThreadsSearch(uint(search.ThreadsSearch))
		require.Nil(t, err)

		err = sidx.Index.Load(idx.Path)
		require.Nil(t, err)
		search.Indexes[i] = sidx
	}

	fmt.Println("start recall")
	start = time.Now()

	// check recall
	failed := 0
	var wg2 sync.WaitGroup
	for j := 0; j < nthread; j++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for i := 0; i < nitem; i++ {
				key := int64(j*nitem + i)
				anykeys, distances, err := search.Search(nil, sample[key], vectorindex.RuntimeConfig{Limit: 10})
				require.Nil(t, err)
				keys, ok := anykeys.([]int64)
				require.True(t, ok)
				_ = distances
				if keys[0] != key {
					failed++
					found, err := search.Contains(key)
					require.Nil(t, err)
					require.True(t, found)
				}
			}
		}()
	}

	wg2.Wait()

	end = time.Now()
	elapsed := end.Sub(start).Seconds()
	fmt.Printf("Search Time %f sec, size = %d, rate = %f msec/row\n", elapsed, nthread*nitem, 1000*elapsed/float64(nthread*nitem))

	// test Contains false
	found, err = search.Contains(int64(nthread*nitem + 1))
	require.Nil(t, err)
	require.False(t, found)

	recall := float32(nthread*nitem-failed) / float32(nthread*nitem)
	fmt.Printf("Recall %f\n", float32(nthread*nitem-failed)/float32(nthread*nitem))
	require.True(t, (recall > 0.96))

}

func TestBuildIndex(t *testing.T) {
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = 100
	//tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	idx, err := NewHnswBuildIndex("abc-0", idxcfg, 1, MaxIndexCapacity)
	require.Nil(t, err)

	empty, err := idx.Empty()
	require.Nil(t, err)
	require.Equal(t, empty, true)

	full, err := idx.Full()
	require.Nil(t, err)
	require.Equal(t, full, false)

	err = idx.Destroy()
	require.Nil(t, err)
}

func TestBuildSingleThread(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool("", m)

	ndim := 32
	nthread := 2
	nitem := MaxIndexCapacity

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(uint(ndim))}
	idxcfg.Usearch.Metric = usearch.L2sq
	//idxcfg.Usearch.Quantization = usearch.F32
	idxcfg.Usearch.Connectivity = 48    // default 16
	idxcfg.Usearch.ExpansionAdd = 128   // default 128
	idxcfg.Usearch.ExpansionSearch = 30 // default 64
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src",
		MetadataTable: "__secondary_meta", IndexTable: "__secondary_index",
		ThreadsSearch: 0,
		ThreadsBuild:  1,
		IndexCapacity: MaxIndexCapacity}

	uid := fmt.Sprintf("%s:%d:%d", "localhost", 1, 0)
	build, err := NewHnswBuild(proc, uid, 1, idxcfg, tblcfg)
	require.Nil(t, err)
	defer build.Destroy()

	// fix the seek
	r := rand.New(rand.NewSource(99))

	// create sample date
	sample := make([][]float32, nitem*nthread)
	for i := 0; i < nthread*nitem; i++ {
		sample[i] = make([]float32, ndim)
		for j := 0; j < ndim; j++ {
			sample[i][j] = r.Float32()
		}
	}

	fmt.Printf("sample created\n")
	var wg sync.WaitGroup
	for j := 0; j < nthread; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < nitem; i++ {
				key := j*nitem + i
				err := build.Add(int64(key), sample[key])
				require.Nil(t, err)
			}
		}()
	}
	wg.Wait()

	fmt.Printf("model built\n")
	sqls, err := build.ToInsertSql(time.Now().UnixMicro())
	require.Nil(t, err)
	require.Equal(t, 3, len(sqls))
	fmt.Println(sqls[0])
	fmt.Println(sqls[2])

	indexes := build.GetIndexes()
	require.Equal(t, 2, len(indexes))

	/*
		for _, idx := range indexes {
			fi, err := os.Stat(idx.Path)
			require.Nil(t, err)
			filesz := fi.Size()
			fmt.Printf("file %s size = %d\n", idx.Path, filesz)
		}
	*/

	fmt.Printf("model search\n")
	// load index file and search
	search := NewHnswSearch(idxcfg, tblcfg)
	defer search.Destroy()

	fmt.Printf("threads search %d\n", search.ThreadsSearch)
	// test Contains with no indexes
	found, err := search.Contains(int64(nthread*nitem + 1))
	require.Nil(t, err)
	require.False(t, found)

	fmt.Printf("load model from files\n")
	// load index
	search.Indexes = make([]*HnswSearchIndex, len(indexes))
	for i, idx := range indexes {
		sidx := &HnswSearchIndex{}
		sidx.Index, err = usearch.NewIndex(idxcfg.Usearch)
		require.Nil(t, err)

		err = sidx.Index.ChangeThreadsSearch(uint(search.ThreadsSearch))
		require.Nil(t, err)

		err = sidx.Index.Load(idx.Path)
		require.Nil(t, err)
		search.Indexes[i] = sidx

		slen, err := sidx.Index.Len()
		require.Nil(t, err)
		require.Equal(t, nitem, int(slen))
	}

	fmt.Println("start recall")
	// check recall
	failed := 0
	var wg2 sync.WaitGroup
	for j := 0; j < nthread; j++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for i := 0; i < nitem; i++ {
				key := int64(j*nitem + i)
				anykeys, distances, err := search.Search(nil, sample[key], vectorindex.RuntimeConfig{Limit: 10})
				require.Nil(t, err)
				keys, ok := anykeys.([]int64)
				require.True(t, ok)
				_ = distances
				if keys[0] != key {
					failed++
					found, err := search.Contains(key)
					require.Nil(t, err)
					require.True(t, found)
				}
			}
		}()
	}

	wg2.Wait()

	// test Contains false
	found, err = search.Contains(int64(nthread*nitem + 1))
	require.Nil(t, err)
	require.False(t, found)

	recall := float32(nthread*nitem-failed) / float32(nthread*nitem)
	fmt.Printf("Recall %f\n", float32(nthread*nitem-failed)/float32(nthread*nitem))
	require.True(t, (recall > 0.96))

}
