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

package indexfile

import (
	"context"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Files is where a caller without a process reads and writes index files: the storage
// engine and the CN's index-file cache (nil when the CN has none).
type Files struct {
	Engine engine.Engine
	Cache  *fileservice.IndexFileCache
}

// ReadTarget is what a read of index files runs under, as sqlproc's internal SQL does: the
// account that SQL executes as, and the process's transaction pinned at its named snapshot
// when that is historical.
func ReadTarget(sqlproc *sqlexec.SqlProcess) (context.Context, client.TxnOperator, error) {
	if sqlproc.Proc == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("index file: reading needs a process")
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

// NewEnv is the environment in txn (nil means the process's transaction), from files or else
// from sqlproc's process, keeping files outside the cache in tempDir. Without a storage engine
// it has no cache: a write then needs only the transaction.
func NewEnv(sqlproc *sqlexec.SqlProcess, files *Files, txn client.TxnOperator, tempDir string) *Env {
	proc := sqlproc.Proc
	env := &Env{Proc: proc, Txn: txn, TempDir: tempDir}
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

// OpenWithProcess opens the file ref names as sqlproc's internal SQL reads (ReadTarget),
// keeping a file rebuilt outside the cache in tempDir.
func OpenWithProcess(sqlproc *sqlexec.SqlProcess, ref Ref, tempDir string) (*File, error) {
	ctx, txn, err := ReadTarget(sqlproc)
	if err != nil {
		return nil, err
	}
	env := NewEnv(sqlproc, nil, txn, tempDir)
	if env.Engine == nil || env.Proc == nil {
		return nil, moerr.NewInternalErrorNoCtx("index file: reading needs a process with a storage engine")
	}
	return Open(ctx, env, ref)
}

// TailFrameRef names the index file of the CDC tail frame of db.table that starts at chunk
// start, with the checksum its metadata row records.
func TailFrameRef(db, table string, start, size int64, checksum string) Ref {
	return Ref{
		DB:         db,
		Table:      table,
		IndexID:    vectorindex.CdcTailId,
		Tag:        int64(vectorindex.Tag_CdcEvents),
		FirstChunk: start,
		Size:       size,
		Checksum:   checksum,
		Verify:     VerifyTailFrame,
	}
}

// VerifyTailFrame checks a tail frame: its envelope and body CRC, and that CRC against
// checksum, the one its metadata row records.
func VerifyTailFrame(f *os.File, size int64, checksum string) error {
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return err
	}
	return VerifyTailFrameBytes(buf, checksum)
}

// VerifyTailFrameBytes is VerifyTailFrame of a frame in memory.
func VerifyTailFrameBytes(buf []byte, checksum string) error {
	if _, _, _, _, _, err := cuvscdc.UnframeCdcChunk(buf); err != nil {
		return err
	}
	crc, _ := cuvscdc.CdcChunkChecksum(buf)
	if got := vectorindex.CdcChunkSetChecksum([]uint32{crc}); got != checksum {
		return moerr.NewInternalErrorNoCtxf("tail frame checksum mismatch: got %s, want %s", got, checksum)
	}
	return nil
}
