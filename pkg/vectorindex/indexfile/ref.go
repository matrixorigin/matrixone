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

// Package indexfile reads and writes index files stored as chunk rows of an index
// storage table (index_id, chunk_id, data, tag), caching whole files in the CN's
// index-file disk cache.
package indexfile

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// ChunkSize is the size of every chunk of a file except its last.
const ChunkSize = vectorindex.MaxChunkSize

// Ref identifies one index file: the chunks [FirstChunk, FirstChunk+ceil(Size/ChunkSize))
// of IndexID with Tag in the storage table DB.Table, as recorded by the file's metadata
// row (Size, Checksum).
type Ref struct {
	DB         string
	Table      string
	IndexID    string
	Tag        int64
	FirstChunk int64
	Size       int64
	Checksum   string
	// Verify checks the assembled file against Checksum. Nil means a hex MD5 of the file.
	Verify func(f *os.File, size int64, checksum string) error
}

func (r *Ref) validate() error {
	if r.DB == "" || r.Table == "" || r.IndexID == "" {
		return moerr.NewInvalidInputNoCtxf("index file: incomplete reference %+v", *r)
	}
	if r.Size < 0 || r.FirstChunk < 0 || r.Checksum == "" {
		return moerr.NewInvalidInputNoCtxf("index file %s: invalid size %d, first chunk %d or checksum %q",
			r.IndexID, r.Size, r.FirstChunk, r.Checksum)
	}
	return nil
}

func (r *Ref) numChunks() int64 {
	return (r.Size + ChunkSize - 1) / ChunkSize
}

// entryName is the index-file cache name of the file's content.
func entryName(tableID uint64, r *Ref) string {
	return fmt.Sprintf("%d/%s/%d/%d-%d/%s",
		tableID, hex.EncodeToString([]byte(r.IndexID)), r.Tag, r.FirstChunk, r.Size, pathSafe(r.Checksum))
}

// pathSafe maps a checksum to a path component one-to-one, also on a case-insensitive
// file system: digits and lowercase letters not starting with 'x' are kept as they are,
// anything else is 'x' followed by its lowercase hex encoding.
func pathSafe(s string) string {
	if len(s) > 0 && s[0] == 'x' {
		return "x" + hex.EncodeToString([]byte(s))
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') {
			return "x" + hex.EncodeToString([]byte(s))
		}
	}
	return s
}

// VerifyMD5 checks that the first size bytes of f hash to the hex MD5 checksum.
func VerifyMD5(f *os.File, size int64, checksum string) error {
	h := md5.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, size)); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != checksum {
		return moerr.NewInternalErrorNoCtxf("index file checksum mismatch: got %s, want %s", got, checksum)
	}
	return nil
}

const (
	manifestMagic   = "MOIF"
	manifestVersion = 1
	manifestLen     = 4 + 2 + 8 + 4
	dataFile        = "data"
	manifestFile    = "manifest"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func encodeManifest(size int64, crc uint32) []byte {
	b := make([]byte, manifestLen)
	copy(b, manifestMagic)
	binary.LittleEndian.PutUint16(b[4:], manifestVersion)
	binary.LittleEndian.PutUint64(b[6:], uint64(size))
	binary.LittleEndian.PutUint32(b[14:], crc)
	return b
}

func decodeManifest(b []byte) (size int64, crc uint32, err error) {
	if len(b) != manifestLen || string(b[:4]) != manifestMagic ||
		binary.LittleEndian.Uint16(b[4:]) != manifestVersion {
		return 0, 0, moerr.NewInternalErrorNoCtx("index file: invalid manifest")
	}
	return int64(binary.LittleEndian.Uint64(b[6:])), binary.LittleEndian.Uint32(b[14:]), nil
}

func fileCRC(f *os.File, size int64) (uint32, error) {
	h := crc32.New(castagnoli)
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, size)); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
