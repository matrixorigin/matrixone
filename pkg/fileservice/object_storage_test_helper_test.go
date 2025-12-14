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
	"context"
	"io"
	"iter"
	"strings"
	"time"
)

type dummyObjectStorage struct{}

func (dummyObjectStorage) Delete(ctx context.Context, keys ...string) error {
	return nil
}

func (dummyObjectStorage) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (dummyObjectStorage) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {}
}

func (dummyObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (dummyObjectStorage) Stat(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (dummyObjectStorage) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) error {
	_, _ = io.Copy(io.Discard, r)
	return nil
}

type mockParallelObjectStorage struct {
	dummyObjectStorage
	ctx      context.Context
	key      string
	sizeHint *int64
	opt      *ParallelMultipartOption
	err      error
	supports bool
}

func (m *mockParallelObjectStorage) SupportsParallelMultipart() bool {
	return m.supports
}

func (m *mockParallelObjectStorage) WriteMultipartParallel(ctx context.Context, key string, r io.Reader, sizeHint *int64, opt *ParallelMultipartOption) error {
	m.ctx = ctx
	m.key = key
	m.sizeHint = sizeHint
	m.opt = opt
	return m.err
}

type recordingObjectStorage struct {
	calls []string
	ctxs  []context.Context
}

func (r *recordingObjectStorage) record(ctx context.Context, name string) {
	r.calls = append(r.calls, name)
	r.ctxs = append(r.ctxs, ctx)
}

func (r *recordingObjectStorage) Delete(ctx context.Context, keys ...string) error {
	r.record(ctx, "delete")
	return nil
}

func (r *recordingObjectStorage) Exists(ctx context.Context, key string) (bool, error) {
	r.record(ctx, "exists")
	return true, nil
}

func (r *recordingObjectStorage) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	r.record(ctx, "list")
	return func(yield func(*DirEntry, error) bool) {
		yield(&DirEntry{
			Name: "one",
		}, nil)
	}
}

func (r *recordingObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	r.record(ctx, "read")
	return io.NopCloser(strings.NewReader("data")), nil
}

func (r *recordingObjectStorage) Stat(ctx context.Context, key string) (int64, error) {
	r.record(ctx, "stat")
	return 3, nil
}

func (r *recordingObjectStorage) Write(ctx context.Context, key string, rd io.Reader, sizeHint *int64, expire *time.Time) error {
	r.record(ctx, "write")
	_, _ = io.Copy(io.Discard, rd)
	return nil
}

type blockingObjectStorage struct {
	dummyObjectStorage
	start chan struct{}
	wait  chan struct{}
	err   error
}

func (b *blockingObjectStorage) Write(ctx context.Context, key string, rd io.Reader, sizeHint *int64, expire *time.Time) error {
	b.start <- struct{}{}
	<-b.wait
	return b.err
}
