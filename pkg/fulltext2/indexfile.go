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

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
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
		return indexfile.OpenWithProcess(sqlproc, ref, localSpillDir(sqlproc))
	}
	writeIndexFile = func(sqlproc *sqlexec.SqlProcess, files *indexfile.Files, txn client.TxnOperator, ref indexfile.Ref,
		src io.Reader, tempDir string, runSQL func(sql string) error) (int64, string, error) {
		env := indexfile.NewEnv(sqlproc, files, txn, tempDir)
		return indexfile.Write(sqlproc.GetContext(), env, ref, src, runSQL)
	}
)

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
	return indexfile.TailFrameRef(cfg.DbName, cfg.IndexTable, start, size, checksum)
}

// smallTailFrame reports whether a tail frame of size bytes fits in one chunk. Such a frame
// is stored without an index-file cache entry and read from its chunk row.
func smallTailFrame(size int64) bool {
	return size <= indexfile.ChunkSize
}
