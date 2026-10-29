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

package fulltext2

import (
	"context"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// indexFile is an open index file, as indexfile.File.
type indexFile interface {
	File() *os.File
	Size() int64
	Keep() func()
	Close() error
}

// openIndexFile and writeIndexFile read and write index files through the index-file layer;
// tests replace them.
var (
	openIndexFile = func(sqlproc *sqlexec.SqlProcess, ref indexfile.Ref) (indexFile, error) {
		ctx, txn, err := readTarget(sqlproc)
		if err != nil {
			return nil, err
		}
		env := indexFileEnv(sqlproc, nil, txn)
		if env.Engine == nil || env.Proc == nil {
			return nil, moerr.NewInternalErrorNoCtx("fulltext2: reading an index file needs a process with a storage engine")
		}
		return indexfile.Open(ctx, env, ref)
	}
	writeIndexFile = func(sqlproc *sqlexec.SqlProcess, files *IndexFiles, txn client.TxnOperator, ref indexfile.Ref,
		src io.Reader, runSQL func(sql string) error) (int64, string, error) {
		return indexfile.Write(sqlproc.GetContext(), indexFileEnv(sqlproc, files, txn), ref, src, runSQL)
	}
)

// readTarget is what a read of index files runs under, as sqlproc's internal SQL does: the
// account that SQL executes as, and the process's transaction pinned at its named snapshot
// when that is historical.
func readTarget(sqlproc *sqlexec.SqlProcess) (context.Context, client.TxnOperator, error) {
	if sqlproc.Proc == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("fulltext2: reading an index file needs a process")
	}
	account, err := sqlproc.EffectiveAccountID()
	if err != nil {
		return nil, nil, err
	}
	txn := sqlproc.Proc.GetTxnOperator()
	if ts := sqlproc.EffectiveSnapshotTS(); ts != nil && txn != nil {
		txn = txn.CloneSnapshotOp(*ts)
	}
	return defines.AttachAccountId(sqlproc.GetContext(), account), txn, nil
}

// IndexFiles is where a caller without a process reads and writes index files: the storage
// engine and the CN's index-file cache (nil when the CN has none).
type IndexFiles struct {
	Engine engine.Engine
	Cache  *fileservice.IndexFileCache
}

// indexFileEnv is the index-file layer environment in txn (nil means the process's
// transaction), from files or else from sqlproc's process, spilling files outside the cache to
// the LOCAL file service. Without a storage engine it has no cache: a write then needs only the
// transaction.
func indexFileEnv(sqlproc *sqlexec.SqlProcess, files *IndexFiles, txn client.TxnOperator) *indexfile.Env {
	proc := sqlproc.Proc
	env := &indexfile.Env{Proc: proc, Txn: txn, TempDir: localSpillDir(sqlproc)}
	if files != nil {
		env.Engine, env.Cache = files.Engine, files.Cache
	} else if proc != nil {
		if info := proc.GetSessionInfo(); info != nil {
			env.Engine = info.StorageEngine
		}
		env.Cache = fileservice.GetIndexFileCache(proc.Base.FileService)
	}
	if env.Txn == nil && proc != nil {
		env.Txn = proc.GetTxnOperator()
	}
	if env.Engine == nil {
		env.Cache = nil
	}
	return env
}

// baseRef names the index file of base segment id.
func baseRef(cfg TableConfig, id string, size int64, checksum string) indexfile.Ref {
	return indexfile.Ref{
		DB:       cfg.DbName,
		Table:    cfg.IndexTable,
		IndexID:  id,
		Size:     size,
		Checksum: checksum,
	}
}

// tailRef names the index file of the CDC tail frame that starts at chunk start.
func tailRef(cfg TableConfig, start, size int64, checksum string) indexfile.Ref {
	return indexfile.Ref{
		DB:         cfg.DbName,
		Table:      cfg.IndexTable,
		IndexID:    vectorindex.CdcTailId,
		Tag:        int64(vectorindex.Tag_CdcEvents),
		FirstChunk: start,
		Size:       size,
		Checksum:   checksum,
		Verify:     verifyFrame,
	}
}

// smallTailFrame reports whether a tail frame of size bytes fits in one chunk. Such a frame
// is stored without an index-file cache entry and read from its chunk row.
func smallTailFrame(size int64) bool {
	return size <= indexfile.ChunkSize
}

// verifyFrame checks a tail frame: its envelope and body CRC, and that CRC against checksum,
// the one its metadata row records.
func verifyFrame(f *os.File, size int64, checksum string) error {
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return err
	}
	return verifyFrameBytes(buf, checksum)
}

// verifyFrameBytes is verifyFrame of a frame in memory.
func verifyFrameBytes(buf []byte, checksum string) error {
	if _, _, _, _, _, err := cuvscdc.UnframeCdcChunk(buf); err != nil {
		return err
	}
	crc, _ := cuvscdc.CdcChunkChecksum(buf)
	if got := vectorindex.CdcChunkSetChecksum([]uint32{crc}); got != checksum {
		return moerr.NewInternalErrorNoCtxf("fulltext2 tail frame checksum mismatch: got %s, want %s", got, checksum)
	}
	return nil
}
