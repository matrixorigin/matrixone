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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObjectStorageSemaphoreCopyObject(t *testing.T) {
	src := &testObjectCopyStorage{}
	upstream := &testObjectCopyStorage{}
	wrapped := newObjectStorageSemaphore(upstream, 1)

	copied, err := wrapped.CopyObject(context.Background(), src, "source", "destination")
	require.NoError(t, err)
	require.True(t, copied)
	require.Same(t, src, upstream.src)
	require.Equal(t, "source", upstream.srcKey)
	require.Equal(t, "destination", upstream.dstKey)

	copied, err = newObjectStorageSemaphore(dummyObjectStorage{}, 1).CopyObject(
		context.Background(), src, "source", "destination",
	)
	require.NoError(t, err)
	require.False(t, copied)

	blocked := newObjectStorageSemaphore(upstream, 1)
	blocked.semaphore <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	copied, err = blocked.CopyObject(ctx, src, "source", "destination")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, copied)
	<-blocked.semaphore
}

func TestObjectStorageSemaphoreDeleteObservesContextWhileWaiting(t *testing.T) {
	wrapped := newObjectStorageSemaphore(dummyObjectStorage{}, 1)
	wrapped.semaphore <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := wrapped.Delete(ctx, "object")
	require.ErrorIs(t, err, context.Canceled)
	<-wrapped.semaphore
}

func TestObjectStorageSemaphoreOperationsObserveContextWhileWaiting(t *testing.T) {
	tests := []struct {
		name string
		call func(*objectStorageSemaphore, context.Context) error
	}{
		{
			name: "exists",
			call: func(storage *objectStorageSemaphore, ctx context.Context) error {
				_, err := storage.Exists(ctx, "object")
				return err
			},
		},
		{
			name: "list",
			call: func(storage *objectStorageSemaphore, ctx context.Context) error {
				for _, err := range storage.List(ctx, "prefix") {
					return err
				}
				return nil
			},
		},
		{
			name: "read",
			call: func(storage *objectStorageSemaphore, ctx context.Context) error {
				reader, err := storage.Read(ctx, "object", nil, nil)
				if err != nil {
					return err
				}
				return reader.Close()
			},
		},
		{
			name: "stat",
			call: func(storage *objectStorageSemaphore, ctx context.Context) error {
				_, err := storage.Stat(ctx, "object")
				return err
			},
		},
		{
			name: "write",
			call: func(storage *objectStorageSemaphore, ctx context.Context) error {
				return storage.Write(ctx, "object", strings.NewReader(""), nil, nil)
			},
		},
		{
			name: "write multipart parallel",
			call: func(storage *objectStorageSemaphore, ctx context.Context) error {
				return storage.WriteMultipartParallel(ctx, "object", nil, nil, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := &mockParallelObjectStorage{supports: true}
			storage := newObjectStorageSemaphore(upstream, 1)
			storage.semaphore <- struct{}{}
			t.Cleanup(func() {
				select {
				case <-storage.semaphore:
				default:
				}
			})

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			result := make(chan error, 1)
			go func() {
				result <- test.call(storage, ctx)
			}()

			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				<-storage.semaphore
				<-result
				t.Fatal("operation did not observe context cancellation while waiting")
			}

			select {
			case <-storage.semaphore:
			default:
				t.Fatal("canceled operation acquired the semaphore")
			}
		})
	}
}

func TestObjectStorageSemaphoreSerializes(t *testing.T) {
	start := make(chan struct{}, 2)
	wait := make(chan struct{})
	upstream := &blockingObjectStorage{
		start: start,
		wait:  wait,
	}
	sem := newObjectStorageSemaphore(upstream, 1)

	done := make(chan struct{})
	go func() {
		require.NoError(t, sem.Write(context.Background(), "a", nil, nil, nil))
		close(done)
	}()

	select {
	case <-start:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}

	startSecond := make(chan struct{})
	go func() {
		defer close(startSecond)
		require.NoError(t, sem.Write(context.Background(), "b", nil, nil, nil))
	}()

	select {
	case <-startSecond:
		t.Fatal("second write started before release")
	case <-time.After(50 * time.Millisecond):
	}

	close(wait) // release first
	select {
	case <-startSecond:
	case <-time.After(time.Second):
		t.Fatal("second write not started after release")
	}
	<-done
}

func TestObjectStorageSemaphoreReleasesOnError(t *testing.T) {
	start := make(chan struct{}, 1)
	wait := make(chan struct{})
	upstream := &blockingObjectStorage{
		start: start,
		wait:  wait,
		err:   context.DeadlineExceeded,
	}
	sem := newObjectStorageSemaphore(upstream, 1)

	// release the blocked write once it has started
	go func() {
		<-start
		close(wait)
	}()

	err := sem.Write(context.Background(), "a", nil, nil, nil)
	require.Error(t, err)

	// another call should proceed after the failed one
	require.NoError(t, sem.Delete(context.Background(), "x"))
}

func TestObjectStorageSemaphoreReadLifecycle(t *testing.T) {
	for _, conditional := range []bool{false, true} {
		name := "ordinary"
		if conditional {
			name = "identity"
		}
		t.Run(name, func(t *testing.T) {
			terminalErr := error(io.EOF)
			if conditional {
				terminalErr = errors.New("read failed")
			}
			closeErr := errors.New("close failed")
			var closes, reads int
			body := &readCloser{r: readerFunc(func(buf []byte) (int, error) {
				reads++
				if reads == 1 {
					buf[0] = 'a'
					return 1, nil
				}
				if reads == 2 {
					buf[0] = 'b'
					return 1, terminalErr
				}
				return 0, io.EOF
			}), closeFunc: func() error { closes++; return closeErr }}
			source := &readTestObjectStorage{open: func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
				return body, nil
			}}
			wrapped := newObjectStorageSemaphore(source, 1)
			reader, err := openTestObject(t.Context(), wrapped, conditional)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Close() })
			buf := make([]byte, 1)
			n, err := reader.Read(buf)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Equal(t, byte('a'), buf[0])
			require.Len(t, wrapped.semaphore, 1)
			n, err = reader.Read(buf)
			require.ErrorIs(t, err, terminalErr)
			require.Equal(t, 1, n)
			require.Equal(t, byte('b'), buf[0])
			require.Empty(t, wrapped.semaphore)
			require.Zero(t, closes)
			// A finished reader must never release a subsequent reader's permit.
			source.open = func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("next")), nil
			}
			next, err := openTestObject(t.Context(), wrapped, conditional)
			require.NoError(t, err)
			t.Cleanup(func() { _ = next.Close() })
			require.ErrorIs(t, reader.Close(), closeErr)
			require.ErrorIs(t, reader.Close(), closeErr)
			require.Len(t, wrapped.semaphore, 1)
			_, err = reader.Read(buf)
			require.ErrorIs(t, err, io.EOF)
			require.Len(t, wrapped.semaphore, 1)
			require.Equal(t, 1, closes)
			require.NoError(t, next.Close())
			require.Empty(t, wrapped.semaphore)
			openErr := errors.New("open failed")
			source.open = func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
				return nil, openErr
			}
			failed, err := openTestObject(t.Context(), wrapped, conditional)
			require.Nil(t, failed)
			require.ErrorIs(t, err, openErr)
			require.Empty(t, wrapped.semaphore)
			// A supported but saturated entry point must cancel before provider I/O.
			wrapped.semaphore <- struct{}{}
			t.Cleanup(func() {
				select {
				case <-wrapped.semaphore:
				default:
				}
			})
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			source.open = func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
				t.Error("canceled read reached provider")
				return nil, openErr
			}
			failed, err = openTestObject(ctx, wrapped, conditional)
			require.Nil(t, failed)
			require.ErrorIs(t, err, context.Canceled)
			require.Len(t, wrapped.semaphore, 1)
		})
	}
}
func TestObjectStorageSemaphoreReadConcurrentClose(t *testing.T) {
	var wrapped *objectStorageSemaphore
	testObjectStorageReadConcurrentClose(t, func(body io.ReadCloser) io.ReadCloser {
		wrapped = newObjectStorageSemaphore(&readTestObjectStorage{open: func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
			return body, nil
		}}, 1)
		reader, err := openTestObject(t.Context(), wrapped, false)
		require.NoError(t, err)
		return reader
	}, func() { require.Empty(t, wrapped.semaphore) })
}
