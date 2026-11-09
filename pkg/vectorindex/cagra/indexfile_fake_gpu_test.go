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

package cagra

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// fakeIndexFiles stands in for the index-file layer: written files are kept in memory, and an
// open checks the size and MD5 as the layer does.
type fakeIndexFiles struct {
	mu sync.Mutex
	// files are the cached files by name; rows are the stored files by chunk range.
	files   map[string][]byte
	rows    map[string][]byte
	opens   int
	hits    int
	openErr error
	held    map[string]int
	// tempDirs records the temp directory of each open and write.
	tempDirs []string
}

// fakeFileKey names a file as the index-file cache does: by its chunk range, size and checksum.
func fakeFileKey(id string, tag, first, size int64, checksum string) string {
	return fmt.Sprintf("%s/%d/%d-%d/%s", id, tag, first, size, checksum)
}

type fakeOpenFile struct {
	f       *os.File
	size    int64
	release func()
}

func (f *fakeOpenFile) File() *os.File { return f.f }
func (f *fakeOpenFile) Size() int64    { return f.size }
func (f *fakeOpenFile) FDPath() string { return fmt.Sprintf("/dev/fd/%d", f.f.Fd()) }
func (f *fakeOpenFile) Close() error {
	if f.release != nil {
		f.release()
		f.release = nil
	}
	return f.f.Close()
}

// hold counts a hold on file key until the returned release.
func (f *fakeIndexFiles) hold(key string) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held[key]++
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.held[key]--
		})
	}
}

// holds returns the holds on the model tars of id.
func (f *fakeIndexFiles) holds(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for key, held := range f.held {
		if strings.HasPrefix(key, id+"/") {
			n += held
		}
	}
	return n
}

// swapIndexFiles replaces the index-file layer for the test.
func swapIndexFiles(t *testing.T) *fakeIndexFiles {
	fake := &fakeIndexFiles{files: make(map[string][]byte), rows: make(map[string][]byte), held: make(map[string]int)}
	prevOpen, prevWrite := openIndexFile, writeIndexFile
	openIndexFile = func(_ *sqlexec.SqlProcess, ref indexfile.Ref, tempDir string) (indexFile, error) {
		fake.mu.Lock()
		fake.opens++
		fake.tempDirs = append(fake.tempDirs, tempDir)
		key := fakeFileKey(ref.IndexID, ref.Tag, ref.FirstChunk, ref.Size, ref.Checksum)
		buf, ok := fake.files[key]
		if !ok {
			// A miss rebuilds the file from its chunk rows.
			buf, ok = fake.rows[fakeFileKey(ref.IndexID, ref.Tag, ref.FirstChunk, 0, "")]
		} else {
			fake.hits++
		}
		err := fake.openErr
		fake.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if !ok || int64(len(buf)) != ref.Size {
			return nil, moerr.NewInternalErrorNoCtxf("index file %s: missing chunk", ref.IndexID)
		}
		if sum := md5.Sum(buf); hex.EncodeToString(sum[:]) != ref.Checksum {
			return nil, moerr.NewInternalErrorNoCtxf("index file %s: checksum mismatch", ref.IndexID)
		}
		f, err := os.CreateTemp(t.TempDir(), "cagrafile")
		if err != nil {
			return nil, err
		}
		if _, err = f.Write(buf); err != nil {
			f.Close()
			return nil, err
		}
		return &fakeOpenFile{f: f, size: int64(len(buf)), release: fake.hold(key)}, nil
	}
	writeIndexFile = func(_ *sqlexec.SqlProcess, _ client.TxnOperator, ref indexfile.Ref, src io.Reader,
		tempDir string, _ func(sql string) error) (int64, error) {
		buf, err := io.ReadAll(src)
		if err != nil {
			return 0, err
		}
		fake.mu.Lock()
		// Write stores the file's chunk rows and caches it under ref's name.
		fake.rows[fakeFileKey(ref.IndexID, ref.Tag, ref.FirstChunk, 0, "")] = buf
		fake.files[fakeFileKey(ref.IndexID, ref.Tag, ref.FirstChunk, int64(len(buf)), ref.Checksum)] = buf
		fake.tempDirs = append(fake.tempDirs, tempDir)
		fake.mu.Unlock()
		return int64(len(buf)), nil
	}
	t.Cleanup(func() { openIndexFile, writeIndexFile = prevOpen, prevWrite })
	return fake
}

// put stores the model tar of id as chunk rows, not cached.
func (f *fakeIndexFiles) put(id string, buf []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[fakeFileKey(id, 0, 0, 0, "")] = append([]byte(nil), buf...)
}

// putFile stores the file at path as the model tar of id.
func (f *fakeIndexFiles) putFile(t *testing.T, id, path string) {
	buf, err := os.ReadFile(path)
	require.NoError(t, err)
	f.put(id, buf)
}

func (f *fakeIndexFiles) get(t *testing.T, id string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	buf, ok := f.rows[fakeFileKey(id, 0, 0, 0, "")]
	require.True(t, ok, "index file %s not written", id)
	return buf
}
