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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// fakeIndexFiles stands in for the index-file layer: written files are kept in memory, and an
// open checks the size and checksum as the layer does.
type fakeIndexFiles struct {
	mu      sync.Mutex
	files   map[string][]byte
	opens   int
	openErr error
}

func fakeFileKey(id string, tag, first int64) string { return fmt.Sprintf("%s/%d/%d", id, tag, first) }

type fakeOpenFile struct {
	f    *os.File
	size int64
}

func (f *fakeOpenFile) File() *os.File { return f.f }
func (f *fakeOpenFile) Size() int64    { return f.size }
func (f *fakeOpenFile) Close() error   { return f.f.Close() }

// swapIndexFiles replaces the index-file layer for the test.
func swapIndexFiles(t *testing.T) *fakeIndexFiles {
	fake := &fakeIndexFiles{files: make(map[string][]byte)}
	prevOpen, prevWrite := openIndexFile, writeIndexFile
	openIndexFile = func(_ *sqlexec.SqlProcess, ref indexfile.Ref) (indexFile, error) {
		fake.mu.Lock()
		fake.opens++
		buf, ok := fake.files[fakeFileKey(ref.IndexID, ref.Tag, ref.FirstChunk)]
		err := fake.openErr
		fake.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if !ok || int64(len(buf)) != ref.Size {
			return nil, moerr.NewInternalErrorNoCtxf("index file %s: missing chunk", ref.IndexID)
		}
		f, err := os.CreateTemp(t.TempDir(), "ft2file")
		if err != nil {
			return nil, err
		}
		if _, err = f.Write(buf); err != nil {
			f.Close()
			return nil, err
		}
		if ref.Verify != nil {
			err = ref.Verify(f, ref.Size, ref.Checksum)
		} else if sum := md5.Sum(buf); hex.EncodeToString(sum[:]) != ref.Checksum {
			err = moerr.NewInternalErrorNoCtxf("index file %s: checksum mismatch", ref.IndexID)
		}
		if err != nil {
			f.Close()
			return nil, err
		}
		return &fakeOpenFile{f: f, size: int64(len(buf))}, nil
	}
	writeIndexFile = func(_ *sqlexec.SqlProcess, _ *IndexFiles, _ client.TxnOperator, ref indexfile.Ref, src io.Reader,
		_ func(sql string) error) (int64, string, error) {
		buf, err := io.ReadAll(src)
		if err != nil {
			return 0, "", err
		}
		fake.put(ref.IndexID, ref.Tag, ref.FirstChunk, buf)
		sum := md5.Sum(buf)
		return int64(len(buf)), hex.EncodeToString(sum[:]), nil
	}
	t.Cleanup(func() { openIndexFile, writeIndexFile = prevOpen, prevWrite })
	return fake
}

func (f *fakeIndexFiles) put(id string, tag, first int64, buf []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[fakeFileKey(id, tag, first)] = append([]byte(nil), buf...)
}

// putFrame stores a tail frame starting at chunk start.
func (f *fakeIndexFiles) putFrame(start int64, frame []byte) {
	f.put(vectorindex.CdcTailId, int64(vectorindex.Tag_CdcEvents), start, frame)
}

func (f *fakeIndexFiles) get(t *testing.T, id string, tag, first int64) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	buf, ok := f.files[fakeFileKey(id, tag, first)]
	require.True(t, ok, "index file %s/%d not written", id, tag)
	return buf
}

// frameRow is a tail frame's metadata row.
type frameRow struct {
	start    int64
	frame    []byte
	checksum string
}

// frameRowsBatch renders tail frames' metadata rows as readTailFrames reads them; a frame with no
// checksum gets the one its footer carries.
func frameRowsBatch(t *testing.T, mp *mpool.MPool, rows []frameRow) *batch.Batch {
	b := batch.NewWithSize(3)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	for _, r := range rows {
		checksum := r.checksum
		if checksum == "" {
			crc, _ := cuvscdc.CdcChunkChecksum(r.frame)
			checksum = vectorindex.CdcChunkSetChecksum([]uint32{crc})
		}
		require.NoError(t, vector.AppendFixed(b.Vecs[0], r.start, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], int64(len(r.frame)), false, mp))
		require.NoError(t, vector.AppendBytes(b.Vecs[2], []byte(checksum), false, mp))
	}
	b.SetRowCount(len(rows))
	return b
}
