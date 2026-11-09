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
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// The index-file cache serves an HNSW index through its life on one CN: CREATE caches the model
// it writes, a CDC sync loads that model from the cache and caches the version it writes, and a
// search loads that version from the cache. No load reads the chunk rows. The search that held
// the version the sync replaced releases it as superseded, so it keeps its last use.
func TestIndexFileCacheLifecycle(t *testing.T) {
	ctx := context.Background()
	diskPath := filepath.Join(t.TempDir(), "disk-cache")
	capacity := toml.ByteSize(64 << 20)
	shared, err := fileservice.NewLocalFS(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.CacheConfig{
		DiskPath: &diskPath, DiskCapacity: &capacity, IndexFileDiskCapacity: &capacity, IndexFileCacheEnabled: true,
	}, nil)
	require.NoError(t, err)
	local, err := fileservice.NewLocalFS(ctx, defines.LocalFileServiceName, t.TempDir(), fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	fs, err := fileservice.NewFileServices(defines.SharedFileServiceName, shared, local)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	cache := fileservice.GetIndexFileCache(fs)
	require.NotNil(t, cache)

	// The storage engine resolves the storage table; reading its chunk rows fails the test.
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "__storage", gomock.Any()).Return(rel, nil).AnyTimes()
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(901)).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).DoAndReturn(func(context.Context) *plan.TableDef {
		t.Error("a model file was rebuilt from its chunk rows")
		return nil
	}).AnyTimes()

	oldStream, oldSql, oldTxn, oldWrite := runSql_streaming, runSql, runTxn, writeIndexFile
	t.Cleanup(func() { runSql_streaming, runSql, runTxn, writeIndexFile = oldStream, oldSql, oldTxn, oldWrite })
	runSql_streaming = func(context.Context, *sqlexec.SqlProcess, string, chan executor.Result, chan error) (executor.Result, error) {
		t.Error("a model file was read through SQL")
		return executor.Result{}, nil
	}
	runTxn = mock_runTxn
	var written []indexfile.Ref
	writeIndexFile = func(sqlproc *sqlexec.SqlProcess, files *indexfile.Files, txn client.TxnOperator, ref indexfile.Ref,
		src io.Reader, tempDir string, run func(sql string) error) (int64, error) {
		written = append(written, ref)
		return oldWrite(sqlproc, files, txn, ref, src, tempDir, run)
	}
	entries := func() int {
		n := 0
		require.NoError(t, filepath.Walk(cache.Path(), func(path string, info os.FileInfo, err error) error {
			if err == nil && info.Name() == "data" {
				n++
			}
			return err
		}))
		return n
	}

	// A query process on the CN: its storage engine and file service with the cache.
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	proc.Base.FileService = fs
	proc.Base.SessionInfo.StorageEngine = eng
	proc.Base.TxnOperator = mockTxnOperator{}
	query := sqlexec.NewSqlProcess(proc)

	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	cfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__meta", IndexTable: "__storage"}

	// CREATE INDEX writes the model and caches it.
	built, err := NewHnswModelForBuild[float32]("abc-0", cfg, 1, 16, 0, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, built.Add(1, []float32{1, 2, 3}))
	require.NoError(t, built.PreparePersistence())
	build := &HnswBuild[float32]{indexes: []*HnswModel[float32]{built}, tblcfg: tblcfg}
	require.NoError(t, build.WriteIndexFiles(query, mockTxnOperator{}, func(string) error { return nil }))
	require.NoError(t, built.Destroy())
	require.Len(t, written, 1)
	require.Equal(t, 1, entries())

	// A CDC sync, on a SqlContext with the CN's engine and cache, loads that model from the
	// cache, applies an upsert and caches the version it writes.
	mp := mpool.MustNewZero()
	entryDirs := func() []string {
		var dirs []string
		require.NoError(t, filepath.Walk(cache.Path(), func(path string, info os.FileInfo, err error) error {
			if err == nil && info.Name() == "data" {
				dirs = append(dirs, filepath.Dir(path))
			}
			return err
		}))
		return dirs
	}
	runSql = func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		bat := batch.NewWithSize(4)
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(written[0].IndexID), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(written[0].Checksum), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], written[0].Size, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	sqlexec.MarkProvenanceColumns("db", "__meta")
	t.Cleanup(func() { sqlexec.ForgetProvenanceShape("db", "__meta") })

	// A search on the CN loads that model from the cache and holds it.
	t.Cleanup(func() { veccache.Cache.Remove(tblcfg.IndexTable) })
	keys, _, err := veccache.Cache.Search(query, tblcfg.IndexTable, NewHnswSearch[float32](cfg, tblcfg), []float32{1, 2, 3},
		vectorindex.RuntimeConfig{Limit: 1})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, keys)
	v1 := entryDirs()
	require.Len(t, v1, 1)
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(v1[0], old, old))
	background := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(ctx, "", mockTxnOperator{}, 0, nil))
	files := &indexfile.Files{Engine: eng, Cache: cache}
	sync, err := NewHnswSync[float32](background, "db", "src", "idx", mockMoIndexes(), int32(types.T_array_float32), 3, t.TempDir(), files)
	require.NoError(t, err)
	cdc := &vectorindex.VectorIndexCdc[float32]{Data: []vectorindex.VectorIndexCdcEntry[float32]{
		{Type: vectorindex.CDC_UPSERT, PKey: 2, Vec: []float32{7, 8, 9}},
	}}
	require.NoError(t, sync.RunOnce(background, cdc))
	require.Len(t, written, 2)
	require.Equal(t, written[0].IndexID, written[1].IndexID, "the sync rewrites the model under its id")
	require.NotEqual(t, written[0].Checksum, written[1].Checksum)
	require.Equal(t, 2, entries(), "the new version is cached beside the one it replaces")
	info, err := os.Stat(v1[0])
	require.NoError(t, err)
	require.True(t, info.ModTime().Before(time.Now().Add(-time.Hour)), "the replaced version keeps its last use")

	// A search loads the new version from the cache.
	idx := &HnswModel[float32]{Id: written[1].IndexID, FileSize: written[1].Size, Checksum: written[1].Checksum}
	require.NoError(t, idx.LoadIndexFromBuffer(query, cfg, tblcfg, 1, true))
	ukeys, _, err := idx.Search([]float32{7, 8, 9}, 1)
	require.NoError(t, err)
	require.Equal(t, []usearch.Key{2}, ukeys)
	require.NoError(t, idx.Destroy())
}
