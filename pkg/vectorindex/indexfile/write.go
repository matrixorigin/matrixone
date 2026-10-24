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

	tmp, remove, err := createWriteTemp(env.Cache)
	if err != nil {
		return 0, "", err
	}
	defer func() {
		_ = tmp.Close()
		remove()
	}()

	md5h, crch := md5.New(), crc32.New(castagnoli)
	size, err = io.Copy(io.MultiWriter(tmp, md5h, crch), src)
	if err != nil {
		return 0, "", err
	}
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

	if env.Cache != nil {
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
			verified.Store(env.Cache.Path()+"\x00"+name, struct{}{})
		}
	}
	return size, sum, nil
}

func createWriteTemp(cache *fileservice.IndexFileCache) (*os.File, func(), error) {
	if cache != nil {
		if f, remove, err := cache.CreateTempFile(); err == nil {
			return f, remove, nil
		}
	}
	f, err := os.CreateTemp("", "indexfile-*")
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = os.Remove(f.Name()) }, nil
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
