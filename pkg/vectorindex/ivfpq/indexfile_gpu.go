//go:build gpu

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

package ivfpq

import (
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// indexFile is an open index file, as indexfile.File.
type indexFile interface {
	File() *os.File
	Size() int64
	FDPath() string
	Close() error
}

// openIndexFile and writeIndexFile read and write index files through the index-file layer;
// tests replace them.
var (
	openIndexFile = func(sqlproc *sqlexec.SqlProcess, ref indexfile.Ref, tempDir string) (indexFile, error) {
		return indexfile.OpenWithProcess(sqlproc, nil, ref, tempDir)
	}
	writeIndexFile = func(sqlproc *sqlexec.SqlProcess, txn client.TxnOperator, ref indexfile.Ref,
		src io.Reader, tempDir string, runSQL func(sql string) error) (int64, error) {
		env := indexfile.NewEnv(sqlproc, nil, txn, tempDir)
		size, _, err := indexfile.Write(sqlproc.GetContext(), env, ref, src, runSQL)
		return size, err
	}
)

// modelRef names the index file of model id: its model tar, chunks from 0 with
// Tag_ModelChunk, recorded with size and its MD5 checksum.
func modelRef(tblcfg vectorindex.IndexTableConfig, id string, size int64, checksum string) indexfile.Ref {
	return indexfile.Ref{
		DB:       tblcfg.DbName,
		Table:    tblcfg.IndexTable,
		IndexID:  id,
		Tag:      int64(vectorindex.Tag_ModelChunk),
		Size:     size,
		Checksum: checksum,
	}
}
