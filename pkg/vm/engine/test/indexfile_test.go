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

package test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	testutil2 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

type indexFileEngine struct {
	t    *testing.T
	ctx  context.Context
	de   *testutil.TestDisttaeEngine
	tae  *testutil.TestTxnStorage
	exec executor.SQLExecutor
}

func setupIndexFileEngine(t *testing.T, opts ...testutil.TestDisttaeEngineOptions) *indexFileEngine {
	catalog.SetupDefines("")
	base, cancel := context.WithCancel(context.Background())
	base = context.WithValue(base, defines.TenantIDKey{}, catalog.System_Account)
	de, tae, rpcAgent, _ := testutil.CreateEngines(base, testutil.TestOptions{}, t, opts...)
	ctx, cancelTimeout := context.WithTimeout(base, 5*time.Minute)
	tae.GetDB().Runtime.SyncProtectionValidator = nil
	t.Cleanup(func() {
		de.Close(base)
		tae.Close(true)
		rpcAgent.Close()
		cancelTimeout()
		cancel()
	})
	require.NoError(t, exec_sql(de, ctx, frontend.MoCatalogMoIndexesDDL))
	require.NoError(t, exec_sql(de, ctx, catalog.MoDatabaseDefaultsDDL))
	require.NoError(t, exec_sql(de, ctx, frontend.MoCatalogMoForeignKeysDDL))
	exec, ok := lookupInternalSQLExecutor()
	require.True(t, ok)
	e := &indexFileEngine{t: t, ctx: ctx, de: de, tae: tae, exec: exec}
	e.run(nil, "create database ifdb")
	e.run(nil, "create table ifdb.idx (index_id varchar(128), chunk_id bigint, data longblob, tag bigint, primary key (index_id, chunk_id))")
	return e
}

// run executes sql in txn, or in its own committed transaction when txn is nil.
func (e *indexFileEngine) run(txn client.TxnOperator, sql string) error {
	opts := executor.Options{}.WithDisableIncrStatement()
	if txn != nil {
		opts = opts.WithTxn(txn)
	}
	res, err := e.exec.Exec(e.ctx, sql, opts)
	if err == nil {
		res.Close()
	}
	if txn == nil {
		require.NoError(e.t, err, sql)
	}
	return err
}

// flush persists the chunk table's rows into storage objects.
func (e *indexFileEngine) flush() {
	e.flushTable("idx")
}

func (e *indexFileEngine) flushTable(table string) {
	op := e.txn()
	db, err := e.de.Engine.Database(e.ctx, "ifdb", op)
	require.NoError(e.t, err)
	rel, err := db.Relation(e.ctx, table, nil)
	require.NoError(e.t, err)
	require.NoError(e.t, e.tae.GetDB().FlushTable(e.ctx, catalog.System_Account, rel.GetDBID(e.ctx), rel.GetTableID(e.ctx),
		types.TimestampToTS(e.de.Now())))
	require.NoError(e.t, op.Commit(e.ctx))
}

func (e *indexFileEngine) txn() client.TxnOperator {
	op, err := e.de.NewTxnOperator(e.ctx, e.de.Now())
	require.NoError(e.t, err)
	return op
}

func (e *indexFileEngine) env(txn client.TxnOperator, cache *fileservice.IndexFileCache) *indexfile.Env {
	proc := testutil2.NewProcess(e.t)
	proc.Base.TxnOperator = txn
	proc.SetIncrStatementDisabled(true)
	return &indexfile.Env{Engine: e.de.Engine, Proc: proc, Txn: txn, Cache: cache}
}

func (e *indexFileEngine) cache() *fileservice.IndexFileCache {
	return e.cacheWith(fileservice.IndexFileRebuildPolicyDefault)
}

func (e *indexFileEngine) cacheWith(rebuild fileservice.Policy) *fileservice.IndexFileCache {
	c, err := fileservice.NewIndexFileCache(e.ctx, e.t.TempDir(), fscache.ConstCapacity(1<<30), "", rebuild)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

func (e *indexFileEngine) write(txn client.TxnOperator, cache *fileservice.IndexFileCache, ref indexfile.Ref, data []byte) (indexfile.Ref, error) {
	size, sum, err := indexfile.Write(e.ctx, e.env(txn, cache), ref, bytes.NewReader(data), func(sql string) error {
		return e.run(txn, sql)
	})
	if err != nil {
		return ref, err
	}
	ref.Size = size
	if ref.Checksum == "" {
		ref.Checksum = sum
	}
	return ref, nil
}

func (e *indexFileEngine) open(txn client.TxnOperator, cache *fileservice.IndexFileCache, ref indexfile.Ref) []byte {
	f, err := indexfile.Open(e.ctx, e.env(txn, cache), ref)
	require.NoError(e.t, err)
	defer f.Close()
	b, err := io.ReadAll(io.NewSectionReader(f.File(), 0, f.Size()))
	require.NoError(e.t, err)
	return b
}

// sqlRead reads a file's chunks with a SQL SELECT in txn.
func (e *indexFileEngine) sqlRead(txn client.TxnOperator, indexID string, tag int64) []byte {
	res, err := e.exec.Exec(e.ctx,
		fmt.Sprintf("select data from ifdb.idx where index_id = '%s' and tag = %d order by chunk_id", indexID, tag),
		executor.Options{}.WithDisableIncrStatement().WithTxn(txn))
	require.NoError(e.t, err)
	defer res.Close()
	var out []byte
	for _, bat := range res.Batches {
		for i := 0; i < bat.RowCount(); i++ {
			out = append(out, bat.Vecs[0].GetBytesAt(i)...)
		}
	}
	return out
}

func md5HexOf(b []byte) string {
	s := md5.Sum(b)
	return hex.EncodeToString(s[:])
}

func indexFileBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31) ^ byte(i/indexfile.ChunkSize) ^ seed
	}
	return b
}

func TestIndexFileWriteOpenThroughEngine(t *testing.T) {
	e := setupIndexFileEngine(t)
	base := indexFileBytes((indexfile.ChunksPerStatement+3)*indexfile.ChunkSize+99, 1)
	frame := indexFileBytes(2*indexfile.ChunkSize+5, 2)

	txn1 := e.txn()
	writeCache := e.cache()
	baseRef, err := e.write(txn1, writeCache, indexfile.Ref{DB: "ifdb", Table: "idx", IndexID: "idx:1:0"}, base)
	require.NoError(t, err)
	frameRef, err := e.write(txn1, writeCache, indexfile.Ref{DB: "ifdb", Table: "idx", IndexID: "cdc_tail", Tag: 1, FirstChunk: 40}, frame)
	require.NoError(t, err)

	require.Equal(t, base, e.sqlRead(txn1, "idx:1:0", 0), "SQL reads the rows the layer wrote")
	require.Equal(t, base, e.open(txn1, nil, baseRef), "the writing transaction rebuilds from its own rows")
	require.Equal(t, frame, e.open(txn1, nil, frameRef))
	require.Equal(t, base, e.open(txn1, writeCache, baseRef), "the writing CN has the file cached")
	require.NoError(t, txn1.Commit(e.ctx))

	// rows the chunk-range filter must exclude: other ids sharing a prefix, the same id
	// beyond the file and other frames of the tail
	e.run(nil, "insert into ifdb.idx values "+
		"('idx:1', 0, 'prefix id', 0), ('idx:1:0x', 0, 'extended id', 0), "+
		"('idx:1:0', 5000, 'beyond the file', 0), ('cdc_tail', 39, 'previous frame', 1), "+
		"('cdc_tail', 43, 'next frame', 1)")
	e.flush()

	txn2 := e.txn()
	readCache := e.cache()
	require.Equal(t, base, e.open(txn2, readCache, baseRef), "another CN rebuilds from flushed rows")
	require.Equal(t, frame, e.open(txn2, readCache, frameRef))
	require.Equal(t, base, e.open(txn2, readCache, baseRef))
	require.NoError(t, txn2.Commit(e.ctx))

	txn3 := e.txn()
	_, err = e.write(txn3, nil, indexfile.Ref{DB: "ifdb", Table: "idx", IndexID: "idx:1:0"}, base[:indexfile.ChunkSize])
	require.Error(t, err, "rewriting existing chunk rows is a duplicate key")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry), err)
	require.NoError(t, txn3.Rollback(e.ctx))

	// a snapshot read sees rows deleted after the snapshot
	snapshotTS := e.de.Now()
	e.run(nil, "delete from ifdb.idx where index_id = 'idx:1:0'")
	txnNow := e.txn()
	_, err = indexfile.Open(e.ctx, e.env(txnNow, e.cache()), baseRef)
	require.ErrorContains(t, err, "missing chunk", "the current snapshot no longer has the rows")
	require.Equal(t, base, e.open(txnNow, writeCache, baseRef), "a cached file is served without its rows")
	require.NoError(t, txnNow.Commit(e.ctx))
	txnLater := e.txn()
	txnSnap := txnLater.CloneSnapshotOp(snapshotTS)
	require.Equal(t, base, e.open(txnSnap, e.cache(), baseRef), "the snapshot rebuilds from its rows")
	require.NoError(t, txnLater.Commit(e.ctx))

	// rows written by plain SQL, as today's writers do
	sqlFile := indexFileBytes(indexfile.ChunkSize+10, 3)
	e.run(nil, fmt.Sprintf("insert into ifdb.idx values ('sql:1', 0, unhex('%s'), 0), ('sql:1', 1, unhex('%s'), 0)",
		hex.EncodeToString(sqlFile[:indexfile.ChunkSize]), hex.EncodeToString(sqlFile[indexfile.ChunkSize:])))
	txn4 := e.txn()
	sqlRef := indexfile.Ref{DB: "ifdb", Table: "idx", IndexID: "sql:1", Size: int64(len(sqlFile)), Checksum: md5HexOf(sqlFile)}
	require.Equal(t, sqlFile, e.open(txn4, e.cache(), sqlRef))
	require.NoError(t, txn4.Commit(e.ctx))
}

type prefetchRecorder struct {
	fileservice.FileService
	mu    sync.Mutex
	paths map[string]struct{}
}

func (r *prefetchRecorder) PrefetchFile(ctx context.Context, filePath string) error {
	r.mu.Lock()
	r.paths[filePath] = struct{}{}
	r.mu.Unlock()
	return r.FileService.PrefetchFile(ctx, filePath)
}

func (r *prefetchRecorder) seen(paths []string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range paths {
		if _, ok := r.paths[p]; ok {
			return true
		}
	}
	return false
}

func (e *indexFileEngine) objectPaths(table string) []string {
	res, err := e.exec.Exec(e.ctx,
		"select distinct object_name from metadata_scan('ifdb."+table+"', '*') g",
		executor.Options{}.WithDisableIncrStatement())
	require.NoError(e.t, err)
	defer res.Close()
	var out []string
	for _, bat := range res.Batches {
		for i := 0; i < bat.RowCount(); i++ {
			out = append(out, bat.Vecs[0].GetStringAt(i))
		}
	}
	require.NotEmpty(e.t, out)
	return out
}

func TestIndexFileRebuildPrefetchFollowsPolicy(t *testing.T) {
	rec := &prefetchRecorder{paths: make(map[string]struct{})}
	e := setupIndexFileEngine(t, testutil.WithDisttaeEngineFileService(func(fs fileservice.FileService) fileservice.FileService {
		rec.FileService = fs
		return rec
	}))
	e.run(nil, "create table ifdb.idx2 (index_id varchar(128), chunk_id bigint, data longblob, tag bigint, primary key (index_id, chunk_id))")

	// prefetch starts once a scan covers four blocks: one object per chunk
	data := indexFileBytes(4*indexfile.ChunkSize, 4)
	for _, table := range []string{"idx", "idx2"} {
		for i := 0; i < 4; i++ {
			chunk := data[i*indexfile.ChunkSize : (i+1)*indexfile.ChunkSize]
			e.run(nil, fmt.Sprintf("insert into ifdb.%s values ('p:1', %d, unhex('%s'), 0)", table, i, hex.EncodeToString(chunk)))
			e.flushTable(table)
		}
	}
	ref := indexfile.Ref{DB: "ifdb", Table: "idx", IndexID: "p:1", Size: int64(len(data)), Checksum: md5HexOf(data)}
	ref2 := ref
	ref2.Table = "idx2"
	restricted, control := e.objectPaths("idx"), e.objectPaths("idx2")

	for _, mode := range []string{fileservice.IndexFileRebuildCacheRead, fileservice.IndexFileRebuildCacheNone} {
		policy, err := fileservice.ParseIndexFileRebuildCache(mode)
		require.NoError(t, err)
		txn := e.txn()
		require.Equal(t, data, e.open(txn, e.cacheWith(policy), ref), mode)
		require.NoError(t, txn.Commit(e.ctx))
	}

	txn := e.txn()
	require.Equal(t, data, e.open(txn, e.cacheWith(fileservice.IndexFileRebuildPolicyDefault), ref2))
	require.NoError(t, txn.Commit(e.ctx))
	require.Eventually(t, func() bool { return rec.seen(control) }, time.Minute, 10*time.Millisecond,
		"a read-write rebuild prefetches its objects")
	require.False(t, rec.seen(restricted), "read and none rebuilds must not prefetch")
}
