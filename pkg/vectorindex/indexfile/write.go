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
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// ChunksPerStatement bounds the chunk rows one INSERT writes.
const ChunksPerStatement = 1000

// FuncName is the table function that turns a registered source into chunk rows.
const FuncName = "index_file_chunks"

// Write stores the bytes of src as the chunk rows of ref in env's transaction and caches
// the file. ref.Size is ignored; ref.Checksum names the cached file when set (a CDC tail
// frame's checksum), otherwise the MD5 does. runSQL runs one statement in env's
// transaction; each writes at most ChunksPerStatement chunks through
// INSERT ... SELECT FROM index_file_chunks(...). It returns the file size and hex MD5.
func Write(ctx context.Context, env *Env, ref Ref, src io.Reader, runSQL func(sql string) error) (size int64, sum string, err error) {
	check := ref
	check.Size = 0
	if check.Checksum == "" {
		check.Checksum = "md5"
	}
	if err := check.validate(); err != nil {
		return 0, "", err
	}

	stage, err := newStagingWriter(env.Cache)
	if err != nil {
		return 0, "", err
	}
	defer stage.close()

	md5h, crch := md5.New(), crc32.New(castagnoli)
	size, err = io.Copy(io.MultiWriter(stage, md5h, crch), src)
	if err != nil {
		return 0, "", err
	}
	tmp := stage.f
	sum = hex.EncodeToString(md5h.Sum(nil))
	ref.Size = size
	if ref.Checksum == "" {
		ref.Checksum = sum
	}

	token, unregister, err := registerSource(&Source{
		File: tmp, IndexID: ref.IndexID, Tag: ref.Tag, FirstChunk: ref.FirstChunk, Size: size,
		txnID: string(env.Txn.Txn().ID),
	})
	if err != nil {
		return 0, "", err
	}
	defer unregister()
	n := ref.numChunks()
	for start := ref.FirstChunk; start < ref.FirstChunk+n; start += ChunksPerStatement {
		count := min(int64(ChunksPerStatement), ref.FirstChunk+n-start)
		if err := runSQL(insertSQL(&ref, token, start, count)); err != nil {
			return 0, "", err
		}
	}

	if stage.cached {
		rel, err := openRelation(ctx, env, &ref)
		if err != nil {
			return 0, "", err
		}
		name := entryName(rel.GetTableID(ctx), &ref)
		crc := crch.Sum32()
		written, err := env.Cache.Put(ctx, name, []fileservice.IndexCacheFile{
			{Name: dataFile, Path: tmp.Name()},
			{Name: manifestFile, Open: func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(encodeManifest(size, crc))), nil
			}},
		})
		if err != nil {
			return 0, "", err
		}
		if written {
			verified.add(env.Cache.Path() + "\x00" + name)
		}
	}
	return size, sum, nil
}

// stagingWriter appends the file being written to a cache temp file. If the cache temp
// file cannot be created or written, it moves what was written to a private temp file
// and continues there; the file is then not cached.
type stagingWriter struct {
	f       *os.File
	remove  func()
	cached  bool
	written int64
}

func newStagingWriter(cache *fileservice.IndexFileCache) (*stagingWriter, error) {
	if cache != nil {
		if f, remove, err := cache.CreateTempFile(); err == nil {
			return &stagingWriter{f: f, remove: remove, cached: true}, nil
		}
	}
	f, remove, err := createPrivateTemp()
	if err != nil {
		return nil, err
	}
	return &stagingWriter{f: f, remove: remove}, nil
}

func createPrivateTemp() (*os.File, func(), error) {
	f, err := os.CreateTemp("", "indexfile-*")
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = os.Remove(f.Name()) }, nil
}

func (w *stagingWriter) Write(p []byte) (int, error) {
	n, err := writeStagingAt(w.f, p, w.written)
	w.written += int64(n)
	if err == nil || !w.cached {
		return n, err
	}
	if err := w.toPrivate(); err != nil {
		return n, err
	}
	m, err := writeStagingAt(w.f, p[n:], w.written)
	w.written += int64(m)
	return n + m, err
}

func (w *stagingWriter) toPrivate() error {
	f, remove, err := createPrivateTemp()
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, io.NewSectionReader(w.f, 0, w.written)); err != nil {
		_ = f.Close()
		remove()
		return &stagingError{err}
	}
	w.close()
	w.f, w.remove, w.cached = f, remove, false
	return nil
}

func (w *stagingWriter) close() {
	_ = w.f.Close()
	w.remove()
}

func insertSQL(ref *Ref, token string, start, count int64) string {
	cols := fmt.Sprintf("%s, %s, %s, %s",
		catalog.IndexStorage_TblCol_Index_Id, catalog.IndexStorage_TblCol_Chunk_Id,
		catalog.IndexStorage_TblCol_Data, catalog.IndexStorage_TblCol_Tag)
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s(%s, %d, %d) AS f",
		sqlquote.QualifiedIdent(ref.DB, ref.Table), cols, cols, FuncName, sqlquote.String(token), start, count)
}

// Source is a local file registered for index_file_chunks: the bytes of one index file
// whose first chunk is FirstChunk.
type Source struct {
	File       io.ReaderAt
	IndexID    string
	Tag        int64
	FirstChunk int64
	Size       int64
	txnID      string
}

var sources sync.Map

func registerSource(s *Source) (token string, unregister func(), err error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", nil, err
	}
	token = hex.EncodeToString(b[:])
	sources.Store(token, s)
	return token, func() { sources.Delete(token) }, nil
}

// LookupSource returns the source registered under token by the transaction txnID.
func LookupSource(token string, txnID []byte) (*Source, error) {
	v, ok := sources.Load(token)
	if !ok || v.(*Source).txnID != string(txnID) {
		return nil, moerr.NewInvalidInputNoCtxf("%s: unknown source", FuncName)
	}
	return v.(*Source), nil
}

// ReadChunk returns chunk chunkID of s into buf, which must hold ChunkSize bytes.
func (s *Source) ReadChunk(chunkID int64, buf []byte) ([]byte, error) {
	i := chunkID - s.FirstChunk
	off := i * ChunkSize
	if i < 0 || off >= s.Size {
		return nil, moerr.NewInvalidInputNoCtxf("%s: chunk %d outside the file", FuncName, chunkID)
	}
	n := min(int64(ChunkSize), s.Size-off)
	if _, err := s.File.ReadAt(buf[:n], off); err != nil {
		return nil, err
	}
	return buf[:n], nil
}
