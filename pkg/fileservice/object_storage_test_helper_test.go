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
	"errors"
	"github.com/stretchr/testify/require"
	"io"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
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

// readTestObjectStorage keeps the ordinary and conditional provider entry points
// observable without an SDK, network server, or shared mutable request state.
type readTestObjectStorage struct {
	dummyObjectStorage
	open func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error)
}

func (s *readTestObjectStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	return s.open(ctx, key, min, max, nil)
}
func (s *readTestObjectStorage) ReadObjectWithIdentity(ctx context.Context, key string, min, max *int64, expected ObjectIdentity) (io.ReadCloser, error) {
	return s.open(ctx, key, min, max, &expected)
}
func (s *readTestObjectStorage) StatObjectIdentity(context.Context, string) (ObjectIdentity, error) {
	return ObjectIdentity{ETag: "version1", Size: 8}, nil
}
func openTestObject(ctx context.Context, storage ObjectStorage, conditional bool) (io.ReadCloser, error) {
	min, max := int64(1), int64(4)
	if conditional {
		return storage.(objectStorageIdentityReader).ReadObjectWithIdentity(ctx, "key", &min, &max, ObjectIdentity{ETag: "version1", Size: 8})
	}
	return storage.Read(ctx, "key", &min, &max)
}

// Both wrappers own independent Close guards; exercise each directly so an
// outer guard cannot mask an unguarded inner wrapper.
func testObjectStorageReadConcurrentClose(t *testing.T, open func(io.ReadCloser) io.ReadCloser, checkReleased func()) {
	t.Helper()
	readStarted, readStop := make(chan struct{}), make(chan struct{})
	closeStarted, closeFinish := make(chan struct{}), make(chan struct{})
	closeMayInterruptRead := make(chan struct{})
	allowInterrupt := sync.OnceFunc(func() { close(closeMayInterruptRead) })
	stopRead, finishClose := sync.OnceFunc(func() { close(readStop) }), sync.OnceFunc(func() { close(closeFinish) })
	startClose := sync.OnceFunc(func() { close(closeStarted) })
	var calls atomic.Int32
	var group sync.WaitGroup
	t.Cleanup(func() { allowInterrupt(); stopRead(); finishClose(); group.Wait() })
	readErr, closeErr := errors.New("read interrupted"), errors.New("close failed")
	body := &readCloser{r: readerFunc(func([]byte) (int, error) {
		close(readStarted)
		<-readStop
		return 0, readErr
	}), closeFunc: func() error {
		calls.Add(1)
		startClose()
		<-closeMayInterruptRead
		stopRead()
		<-closeFinish
		return closeErr
	}}
	reader := open(body)
	t.Cleanup(func() { allowInterrupt(); stopRead(); finishClose(); _ = reader.Close() })
	readResult := make(chan error, 1)
	group.Go(func() { _, err := reader.Read(make([]byte, 1)); readResult <- err })
	select {
	case <-readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not start")
	}
	results := make(chan error, 8)
	for range cap(results) {
		group.Go(func() { results <- reader.Close() })
	}
	select {
	case <-closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for blocked Read")
	}
	checkReleased() // Read is still blocked, so its error cannot release the permit.
	allowInterrupt()
	select {
	case err := <-readResult:
		require.ErrorIs(t, err, readErr)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not interrupt Read")
	}
	select {
	case err := <-results:
		t.Fatalf("Close returned before provider completed: %v", err)
	default:
	}
	finishClose()
	group.Wait()
	for range cap(results) {
		require.ErrorIs(t, <-results, closeErr)
	}
	require.ErrorIs(t, reader.Close(), closeErr)
	require.Equal(t, int32(1), calls.Load())
	checkReleased()
}
