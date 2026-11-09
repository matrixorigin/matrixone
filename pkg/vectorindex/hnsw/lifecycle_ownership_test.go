// Copyright 2026 Matrix Origin
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

package hnsw

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func TestCacheRemoveReleasesHnswModelFile(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mp := mpool.MustNewZero()
	sqlproc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mp))
	oldSQL, oldStreaming := runSql, runSql_streaming
	runSql, runSql_streaming = mock_runSql, mock_runSql_streaming
	t.Cleanup(func() { runSql, runSql_streaming = oldSQL, oldStreaming })

	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	tables := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "meta", IndexTable: "idx"}
	search := NewHnswSearch[float32](cfg, tables)
	c := cache.NewVectorIndexCache()
	t.Cleanup(c.Destroy)
	_, _, err := c.Search(sqlproc, "idx", search, []float32{0, 1, 2}, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Len(t, search.Indexes, 1)
	path := search.Indexes[0].Path
	require.FileExists(t, path)
	c.Remove("idx")
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "cache eviction left model file %s: %v", path, err)
	require.Empty(t, search.Indexes)
}

func TestHnswModelDestroyReportsFileCleanupFailure(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(parent, []byte("x"), 0600))
	path := filepath.Join(parent, "model")
	model := &HnswModel[float32]{Path: path}
	require.Error(t, model.Destroy())
	require.Equal(t, path, model.Path)
}

func TestHnswStreamPanicCleansResources(t *testing.T) {
	for _, fromBuffer := range []bool{false, true} {
		for _, producerPanic := range []bool{false, true} {
			name := "load"
			if fromBuffer {
				name = "view"
			}
			if producerPanic {
				name += "/producer"
			} else {
				name += "/consumer"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("TMPDIR", dir)
				mp := mpool.MustNewZero()
				sqlproc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mp))
				before := mp.CurrNB()
				oldStreaming := runSql_streaming
				t.Cleanup(func() { runSql_streaming = oldStreaming })
				exited := make(chan struct{})
				runSql_streaming = func(ctx context.Context, _ *sqlexec.SqlProcess, _ string, out chan executor.Result, _ chan error) (executor.Result, error) {
					defer close(exited)
					if producerPanic {
						panic("producer failed")
					}
					malformed := batch.NewWithSize(1)
					malformed.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					_ = vector.AppendFixed[int64](malformed.Vecs[0], 0, false, mp)
					malformed.SetRowCount(1)
					out <- executor.Result{Mp: mp, Batches: []*batch.Batch{malformed}}
					<-ctx.Done()
					out <- executor.Result{Mp: mp, Batches: []*batch.Batch{makeIndexBatch(sqlproc.Proc)}}
					return executor.Result{}, nil
				}
				idx := &HnswModel[float32]{Id: "broken", FileSize: 4096, Checksum: "expected"}
				cfg := vectorindex.IndexConfig{Usearch: usearch.DefaultConfig(3)}
				load := func() error {
					if fromBuffer {
						return idx.LoadIndexFromBuffer(sqlproc, cfg, vectorindex.IndexTableConfig{}, 1, true)
					}
					return idx.LoadIndex(sqlproc, cfg, vectorindex.IndexTableConfig{}, 1, false)
				}
				if producerPanic {
					require.ErrorContains(t, load(), "producer failed")
				} else {
					require.Panics(t, func() { _ = load() })
				}
				select {
				case <-exited:
				default:
					t.Fatal("stream producer was not joined")
				}
				require.Zero(t, mp.CurrNB()-before)
				require.Empty(t, idx.Path)
				files, err := os.ReadDir(dir)
				require.NoError(t, err)
				require.Empty(t, files)
			})
		}
	}
}

func TestHnswLoadPanicReleasesEarlierModels(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	mp := mpool.MustNewZero()
	sqlproc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mp))
	oldStreaming := runSql_streaming
	t.Cleanup(func() { runSql_streaming = oldStreaming })
	checksum, err := vectorindex.CheckSum("resources/hnsw0.bin")
	require.NoError(t, err)
	info, err := os.Stat("resources/hnsw0.bin")
	require.NoError(t, err)
	indexes := []*HnswModel[float32]{
		{Id: "first", Checksum: checksum, FileSize: info.Size()},
		{Id: "second", Checksum: checksum, FileSize: info.Size()},
	}
	t.Cleanup(func() { destroyModels(indexes) })
	var firstPath string
	calls := 0
	runSql_streaming = func(_ context.Context, _ *sqlexec.SqlProcess, _ string, out chan executor.Result, _ chan error) (executor.Result, error) {
		calls++
		if calls == 1 {
			out <- executor.Result{Mp: mp, Batches: []*batch.Batch{makeIndexBatch(sqlproc.Proc)}}
		} else {
			firstPath = indexes[0].Path
			malformed := batch.NewWithSize(1)
			malformed.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			_ = vector.AppendFixed[int64](malformed.Vecs[0], 0, false, mp)
			malformed.SetRowCount(1)
			out <- executor.Result{Mp: mp, Batches: []*batch.Batch{malformed}}
		}
		return executor.Result{}, nil
	}
	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	search := NewHnswSearch[float32](cfg, vectorindex.IndexTableConfig{})
	before := mp.CurrNB()
	require.Panics(t, func() { _, _ = search.LoadIndex(sqlproc, indexes) })
	require.NotEmpty(t, firstPath)
	_, err = os.Stat(firstPath)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, indexes[0].Index)
	require.Zero(t, mp.CurrNB()-before)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestHnswGenerationClosesMalformedResult(t *testing.T) {
	mp := mpool.MustNewZero()
	oldSQL, oldAuto := runSql, runSqlAutoCommit
	t.Cleanup(func() { runSql, runSqlAutoCommit = oldSQL, oldAuto })
	result := func() executor.Result {
		bat := batch.NewWithSize(2)
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		_ = vector.AppendFixed[int64](bat.Vecs[1], 1, false, mp)
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	}
	runSql = func(*sqlexec.SqlProcess, string) (executor.Result, error) { return result(), nil }
	runSqlAutoCommit = func(context.Context, string, uint32, string, string) (executor.Result, error) { return result(), nil }
	before := mp.CurrNB()
	_, _, err := loadHnswGeneration(nil, vectorindex.IndexTableConfig{})
	require.ErrorContains(t, err, "loadHnswGeneration recovered")
	require.Equal(t, before, mp.CurrNB())
	_, _, err = queryHnswGeneration(context.Background(), "cn", 0, vectorindex.IndexTableConfig{})
	require.ErrorContains(t, err, "queryHnswGeneration recovered")
	require.Equal(t, before, mp.CurrNB())
}
