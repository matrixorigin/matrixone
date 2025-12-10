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
	"iter"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestGetParallelUploadPoolSingleton(t *testing.T) {
	p1 := getParallelUploadPool()
	p2 := getParallelUploadPool()
	if p1 == nil || p2 == nil {
		t.Fatalf("pool not initialized")
	}
	if p1 != p2 {
		t.Fatalf("expected singleton pool")
	}
}

type mockParallelStorage struct {
	supports bool
	exists   bool

	writeCalled int
	mpCalled    int

	lastKey    string
	lastData   []byte
	lastSize   *int64
	lastOpt    ParallelMultipartOption
	lastExpire *time.Time
}

func (m *mockParallelStorage) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {}
}

func (m *mockParallelStorage) Stat(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (m *mockParallelStorage) Exists(ctx context.Context, key string) (bool, error) {
	return m.exists, nil
}

func (m *mockParallelStorage) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) error {
	m.writeCalled++
	m.lastKey = key
	m.lastSize = sizeHint
	m.lastExpire = expire
	b, _ := io.ReadAll(r)
	m.lastData = b
	return nil
}

func (m *mockParallelStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (m *mockParallelStorage) Delete(ctx context.Context, keys ...string) error {
	return nil
}

func (m *mockParallelStorage) SupportsParallelMultipart() bool {
	return m.supports
}

func (m *mockParallelStorage) WriteMultipartParallel(ctx context.Context, key string, r io.Reader, sizeHint *int64, opt *ParallelMultipartOption) error {
	m.mpCalled++
	m.lastKey = key
	if opt != nil {
		m.lastOpt = *opt
	}
	m.lastSize = sizeHint
	b, _ := io.ReadAll(r)
	m.lastData = b
	return nil
}

func TestS3FSWriteUsesParallelWhenForced(t *testing.T) {
	storage := &mockParallelStorage{supports: true}
	fs := &S3FS{
		name:        "s3",
		storage:     storage,
		ioMerger:    NewIOMerger(),
		asyncUpdate: true,
	}

	data := []byte("hello")
	vector := IOVector{
		FilePath:      "obj",
		ForceParallel: true,
		Entries: []IOEntry{
			{Offset: 0, Size: int64(len(data)), Data: data},
		},
	}

	if err := fs.Write(context.Background(), vector); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if storage.mpCalled != 1 {
		t.Fatalf("expected parallel write, got %d", storage.mpCalled)
	}
	if storage.writeCalled != 0 {
		t.Fatalf("unexpected non-parallel write")
	}
	if storage.lastKey != "obj" {
		t.Fatalf("unexpected key %s", storage.lastKey)
	}
	if string(storage.lastData) != "hello" {
		t.Fatalf("unexpected data %s", string(storage.lastData))
	}
}

func TestS3FSWriteDisableParallel(t *testing.T) {
	storage := &mockParallelStorage{supports: true}
	fs := &S3FS{
		name:        "s3",
		storage:     storage,
		ioMerger:    NewIOMerger(),
		asyncUpdate: true,
	}

	data := bytes.Repeat([]byte("a"), int(minMultipartPartSize+1))
	vector := IOVector{
		FilePath:        "large",
		DisableParallel: true,
		Entries: []IOEntry{
			{Offset: 0, Size: int64(len(data)), Data: data},
		},
	}

	if err := fs.Write(context.Background(), vector); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if storage.mpCalled != 0 {
		t.Fatalf("parallel write should be disabled")
	}
	if storage.writeCalled != 1 {
		t.Fatalf("expected single write, got %d", storage.writeCalled)
	}
	if storage.lastKey != "large" {
		t.Fatalf("unexpected key %s", storage.lastKey)
	}
}

func TestS3FSWriteUnknownSizeUsesParallel(t *testing.T) {
	storage := &mockParallelStorage{supports: true}
	fs := &S3FS{
		name:        "s3",
		storage:     storage,
		ioMerger:    NewIOMerger(),
		asyncUpdate: true,
	}

	reader := strings.NewReader("data")
	vector := IOVector{
		FilePath: "unknown",
		Entries: []IOEntry{
			{Offset: 0, Size: -1, ReaderForWrite: reader},
		},
	}

	if err := fs.Write(context.Background(), vector); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if storage.mpCalled != 1 {
		t.Fatalf("expected parallel write for unknown size")
	}
	if storage.lastSize != nil {
		t.Fatalf("size hint should be nil for unknown size")
	}
}

func TestS3FSSmallSizeSkipsParallel(t *testing.T) {
	storage := &mockParallelStorage{supports: true}
	fs := &S3FS{
		name:        "s3",
		storage:     storage,
		ioMerger:    NewIOMerger(),
		asyncUpdate: true,
	}

	data := bytes.Repeat([]byte("b"), int(minMultipartPartSize-1))
	vector := IOVector{
		FilePath: "small",
		Entries: []IOEntry{
			{Offset: 0, Size: int64(len(data)), Data: data},
		},
	}

	if err := fs.Write(context.Background(), vector); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if storage.writeCalled != 1 {
		t.Fatalf("expected non-parallel write for small object")
	}
	if storage.mpCalled != 0 {
		t.Fatalf("unexpected parallel write")
	}
}

func TestS3FSForceParallelFallbackWhenUnsupported(t *testing.T) {
	storage := &mockParallelStorage{supports: false}
	fs := &S3FS{
		name:        "s3",
		storage:     storage,
		ioMerger:    NewIOMerger(),
		asyncUpdate: true,
	}

	data := []byte("force")
	vector := IOVector{
		FilePath:      "force",
		ForceParallel: true,
		Entries: []IOEntry{
			{Offset: 0, Size: int64(len(data)), Data: data},
		},
	}

	if err := fs.Write(context.Background(), vector); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if storage.mpCalled != 0 {
		t.Fatalf("parallel write should not happen when unsupported")
	}
	if storage.writeCalled != 1 {
		t.Fatalf("expected fallback single write")
	}
}

func TestS3FSParallelPassesExpire(t *testing.T) {
	storage := &mockParallelStorage{supports: true}
	fs := &S3FS{
		name:        "s3",
		storage:     storage,
		ioMerger:    NewIOMerger(),
		asyncUpdate: true,
	}

	data := bytes.Repeat([]byte("c"), int(minMultipartPartSize+1))
	expire := time.Now().Add(time.Hour)
	vector := IOVector{
		FilePath: "with-expire",
		ExpireAt: expire,
		Entries: []IOEntry{
			{Offset: 0, Size: int64(len(data)), Data: data},
		},
	}

	if err := fs.Write(context.Background(), vector); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if storage.mpCalled != 1 {
		t.Fatalf("expected parallel write")
	}
	if storage.lastOpt.Expire == nil || !storage.lastOpt.Expire.Equal(expire) {
		t.Fatalf("expire not propagated")
	}
}

func TestS3FSWriteFileExists(t *testing.T) {
	storage := &mockParallelStorage{supports: true, exists: true}
	fs := &S3FS{
		name:        "s3",
		storage:     storage,
		ioMerger:    NewIOMerger(),
		asyncUpdate: true,
	}
	vector := IOVector{
		FilePath: "dup",
		Entries: []IOEntry{
			{Offset: 0, Size: 1, Data: []byte("x")},
		},
	}
	err := fs.Write(context.Background(), vector)
	if err == nil {
		t.Fatalf("expected file exists error")
	}
}

type countingParallelStorage struct {
	supports bool
	current  atomic.Int64
	max      atomic.Int64
}

func (c *countingParallelStorage) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {}
}

func (c *countingParallelStorage) Stat(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (c *countingParallelStorage) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (c *countingParallelStorage) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) error {
	return nil
}

func (c *countingParallelStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (c *countingParallelStorage) Delete(ctx context.Context, keys ...string) error {
	return nil
}

func (c *countingParallelStorage) SupportsParallelMultipart() bool {
	return c.supports
}

func (c *countingParallelStorage) WriteMultipartParallel(ctx context.Context, key string, r io.Reader, sizeHint *int64, opt *ParallelMultipartOption) error {
	cur := c.current.Add(1)
	defer c.current.Add(-1)
	for {
		old := c.max.Load()
		if cur <= old || c.max.CompareAndSwap(old, cur) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	return nil
}

func TestObjectStorageSemaphoreLimitsParallel(t *testing.T) {
	upstream := &countingParallelStorage{supports: true}
	sem := newObjectStorageSemaphore(upstream, 1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = sem.WriteMultipartParallel(context.Background(), "a", strings.NewReader("1"), nil, nil)
	}()
	go func() {
		defer wg.Done()
		_ = sem.WriteMultipartParallel(context.Background(), "b", strings.NewReader("2"), nil, nil)
	}()
	wg.Wait()

	if upstream.max.Load() > 1 {
		t.Fatalf("expected semaphore to cap concurrency at 1, got %d", upstream.max.Load())
	}
}
