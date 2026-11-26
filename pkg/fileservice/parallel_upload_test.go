// Copyright 2025 Matrix Origin
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

package fileservice

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestNormalizeParallelOption(t *testing.T) {
	cases := []struct {
		name         string
		opt          *ParallelMultipartOption
		expectSize   int64
		expectConc   int
		expectExpire bool
	}{
		{
			name:       "default values",
			opt:        nil,
			expectSize: defaultParallelMultipartPartSize,
			expectConc: runtime.NumCPU(),
		},
		{
			name: "clamp below min",
			opt: &ParallelMultipartOption{
				PartSize: minMultipartPartSize - 1,
			},
			expectSize: minMultipartPartSize,
			expectConc: runtime.NumCPU(),
		},
		{
			name: "clamp above max",
			opt: &ParallelMultipartOption{
				PartSize: maxMultipartPartSize + 1,
			},
			expectSize: maxMultipartPartSize,
			expectConc: runtime.NumCPU(),
		},
		{
			name: "custom values",
			opt: &ParallelMultipartOption{
				PartSize:    16 << 20,
				Concurrency: 3,
			},
			expectSize: 16 << 20,
			expectConc: 3,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := normalizeParallelOption(c.opt)
			if res.PartSize != c.expectSize {
				t.Fatalf("part size mismatch, got %d expect %d", res.PartSize, c.expectSize)
			}
			if res.Concurrency != c.expectConc {
				t.Fatalf("concurrency mismatch, got %d expect %d", res.Concurrency, c.expectConc)
			}
		})
	}
}

// partStorage is object storage that records the writes of an object in parts.
type partStorage struct {
	ObjectStorage
	exists bool

	mu      sync.Mutex
	puts    map[string][]byte
	creates []*time.Time
	parts   map[int32][]byte
	done    []byte
	cur     atomic.Int64
	most    atomic.Int64
}

func newPartStorage() *partStorage {
	return &partStorage{puts: map[string][]byte{}, parts: map[int32][]byte{}}
}

func (p *partStorage) Exists(context.Context, string) (bool, error) { return p.exists, nil }

func (p *partStorage) Write(_ context.Context, key string, r io.Reader, _ *int64, _ *time.Time) error {
	data, err := io.ReadAll(r)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.puts[key] = data
	return err
}

func (p *partStorage) createMultipart(_ context.Context, _ string, expire *time.Time) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates = append(p.creates, expire)
	return "upload", nil
}

func (p *partStorage) uploadPart(_ context.Context, _, _ string, num int32, body io.ReaderAt, size int64) (string, error) {
	data, err := io.ReadAll(io.NewSectionReader(body, 0, size))
	if err != nil {
		return "", err
	}
	n := p.cur.Add(1)
	defer p.cur.Add(-1)
	for most := p.most.Load(); n > most && !p.most.CompareAndSwap(most, n); most = p.most.Load() {
	}
	time.Sleep(10 * time.Millisecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parts[num] = data
	return "etag", nil
}

func (p *partStorage) completeMultipart(_ context.Context, _, _ string, parts []multipartPart) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, part := range parts {
		p.done = append(p.done, p.parts[part.num]...)
	}
	return nil
}

func (p *partStorage) abortMultipart(context.Context, string, string) error { return nil }

// newPartFS returns an S3FS on storage whose server has room free for room requests,
// the rest held until the test ends.
func newPartFS(t *testing.T, storage ObjectStorage, mode ParallelMode, room int) *S3FS {
	prev := writePartSize
	writePartSize = minMultipartPartSize
	t.Cleanup(func() { writePartSize = prev })
	fs := &S3FS{name: "s3", server: newObjectStorageServer(storage, minRequestRoom), rawStorage: storage, ioMerger: NewIOMerger(), asyncUpdate: true, parallelMode: mode}
	limitRoom(t, fs.server, room, max(room/2, 1))
	return fs
}

// A write that ends within its first part is one put; a longer one is a multipart upload
// whose parts are uploaded at once only in parallel mode, and at most as many at once as
// the server has room for.
func TestS3FSWriteInParts(t *testing.T) {
	small := []byte("hello")
	large := bytes.Repeat([]byte("abcdefg"), int(3*minMultipartPartSize/7)+2)
	for _, c := range []struct {
		name  string
		mode  ParallelMode
		room  int
		data  []byte
		size  bool
		parts int
		most  int64
	}{
		{"small", ParallelForce, 8, small, true, 0, 0},
		{"small unknown size", ParallelForce, 8, small, false, 0, 0},
		{"large parallel", ParallelForce, 8, large, true, 4, 4},
		{"large unknown size", ParallelAuto, 8, large, false, 4, 4},
		{"large serial", ParallelOff, 8, large, true, 4, 1},
		{"large parallel in one room", ParallelForce, 1, large, true, 4, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			storage := newPartStorage()
			fs := newPartFS(t, storage, c.mode, c.room)
			entry := IOEntry{Size: int64(len(c.data)), Data: c.data}
			if !c.size {
				entry = IOEntry{Size: -1, ReaderForWrite: bytes.NewReader(c.data)}
			}
			expire := time.Now().Add(time.Hour)
			require.NoError(t, fs.Write(context.Background(), IOVector{FilePath: "obj", ExpireAt: expire, Entries: []IOEntry{entry}}))
			if c.parts == 0 {
				require.Equal(t, c.data, storage.puts["obj"])
				require.Empty(t, storage.creates)
				return
			}
			require.Empty(t, storage.puts)
			require.Equal(t, c.parts, len(storage.parts))
			require.True(t, bytes.Equal(c.data, storage.done))
			require.LessOrEqual(t, storage.most.Load(), c.most)
			if c.most == 1 {
				require.Equal(t, int64(1), storage.most.Load())
			}
			require.True(t, storage.creates[0].Equal(expire), "expire is passed")
		})
	}
}

func TestS3FSWriteFileExists(t *testing.T) {
	storage := newPartStorage()
	storage.exists = true
	fs := newPartFS(t, storage, ParallelOff, 1)
	err := fs.Write(context.Background(), IOVector{FilePath: "dup", Entries: []IOEntry{{Size: 1, Data: []byte("x")}}})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists), "got %v", err)
}
