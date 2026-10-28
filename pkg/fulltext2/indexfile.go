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
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	Close() error
}

// openIndexFile and writeIndexFile read and write index files through the index-file layer;
// tests replace them.
var (
	openIndexFile = func(sqlproc *sqlexec.SqlProcess, ref indexfile.Ref) (indexFile, error) {
		env := indexFileEnv(sqlproc, nil, nil)
		if env.Engine == nil || env.Proc == nil {
			return nil, moerr.NewInternalErrorNoCtx("fulltext2: reading an index file needs a process with a storage engine")
		}
		return indexfile.Open(sqlproc.GetContext(), env, ref)
	}
	writeIndexFile = func(sqlproc *sqlexec.SqlProcess, files *IndexFiles, txn client.TxnOperator, ref indexfile.Ref,
		src io.Reader, runSQL func(sql string) error) (int64, string, error) {
		return indexfile.Write(sqlproc.GetContext(), indexFileEnv(sqlproc, files, txn), ref, src, runSQL)
	}
)

// IndexFiles is where a caller without a process reads and writes index files: the storage
// engine and the CN's index-file cache (nil when the CN has none).
type IndexFiles struct {
	Engine engine.Engine
	Cache  *fileservice.IndexFileCache
}

// indexFileEnv is the index-file layer environment in txn (nil means the process's
// transaction), from files or else from sqlproc's process. Without a storage engine it has no
// cache: a write then needs only the transaction.
func indexFileEnv(sqlproc *sqlexec.SqlProcess, files *IndexFiles, txn client.TxnOperator) *indexfile.Env {
	proc := sqlproc.Proc
	env := &indexfile.Env{Proc: proc, Txn: txn}
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

// verifyFrame checks a tail frame: its envelope and body CRC, and that CRC against checksum,
// the one its metadata row records.
func verifyFrame(f *os.File, size int64, checksum string) error {
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return err
	}
	if _, _, _, _, _, err := cuvscdc.UnframeCdcChunk(buf); err != nil {
		return err
	}
	crc, _ := cuvscdc.CdcChunkChecksum(buf)
	if got := vectorindex.CdcChunkSetChecksum([]uint32{crc}); got != checksum {
		return moerr.NewInternalErrorNoCtxf("fulltext2 tail frame checksum mismatch: got %s, want %s", got, checksum)
	}
	return nil
}
