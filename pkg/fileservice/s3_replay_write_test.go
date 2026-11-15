// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func writeReplayableData(ctx context.Context, fs FileService, vector IOVector) error {
	vector.Policy |= ReplayDataForDiskCache
	return fs.Write(ctx, vector)
}

func newReplayWriteTestFS(t *testing.T) *S3FS {
	t.Helper()
	ctx := context.Background()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "replay", Endpoint: "disk", Bucket: t.TempDir()},
		CacheConfig{DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20)}, nil, false, false)
	require.NoError(t, err)
	require.NotNil(t, fs.diskCache)
	t.Cleanup(func() { fs.Close(ctx) })
	return fs
}

func readReplayCache(t *testing.T, fs *S3FS, path string) []byte {
	t.Helper()
	got, err := os.ReadFile(fs.diskCache.pathForFile(path))
	require.NoError(t, err)
	return got
}

func TestReplayWriteEntriesEligibility(t *testing.T) {
	data := []byte("abcd")
	for _, tc := range []struct {
		name    string
		entries []IOEntry
		want    bool
	}{
		{"contiguous", []IOEntry{{Offset: 0, Size: 2, Data: data[:2]}, {Offset: 2, Size: 2, Data: data[2:]}}, true},
		{"empty element", []IOEntry{{Offset: 0, Size: 0}, {Offset: 0, Size: 4, Data: data}}, true},
		{"gap", []IOEntry{{Offset: 1, Size: 4, Data: data}}, false},
		{"overlap", []IOEntry{{Offset: 0, Size: 4, Data: data}, {Offset: 2, Size: 2, Data: data[:2]}}, false},
		{"wrong size", []IOEntry{{Offset: 0, Size: 3, Data: data}}, false},
		{"negative size", []IOEntry{{Offset: 0, Size: -1, Data: data}}, false},
		{"stream", []IOEntry{{Offset: 0, Size: 4, ReaderForWrite: bytes.NewReader(data)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, canReplayWriteEntries(tc.entries)) })
	}
}

func TestReplayWriteDataLifetimeAndStreams(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"ordinary", "replay"} {
		t.Run(mode, func(t *testing.T) {
			fs := newReplayWriteTestFS(t)
			source := []byte("immutable-during-call")
			want := bytes.Clone(source)
			vector := IOVector{FilePath: "data", Entries: []IOEntry{
				{Offset: 0, Size: 4, Data: source[:4]},
				{Offset: 4, Size: int64(len(source) - 4), Data: source[4:]},
			}}
			var err error
			if mode == "ordinary" {
				err = fs.Write(ctx, vector)
			} else {
				err = writeReplayableData(ctx, fs, vector)
			}
			require.NoError(t, err)
			clear(source)
			require.Equal(t, want, readReplayCache(t, fs, "data"))
		})
	}
	for _, size := range []int64{4, -1} {
		t.Run(fmt.Sprintf("stream-%d", size), func(t *testing.T) {
			fs := newReplayWriteTestFS(t)
			v := IOVector{FilePath: "stream", Entries: []IOEntry{{Offset: 0, Size: size, ReaderForWrite: bytes.NewReader([]byte("once"))}}}
			require.NoError(t, writeReplayableData(ctx, fs, v))
			require.Equal(t, []byte("once"), readReplayCache(t, fs, "stream"))
		})
	}
}

type replayShortStorage struct {
	ObjectStorage
	consume int64
	fail    error
}

func (s *replayShortStorage) Write(_ context.Context, _ string, r io.Reader, _ *int64, _ *time.Time) error {
	_, err := io.CopyN(io.Discard, r, s.consume)
	if err != nil && err != io.EOF {
		return err
	}
	return s.fail
}

func TestReplayWriteShortSuccessAndError(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"ordinary", "replay"} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fail-%t", mode, fail), func(t *testing.T) {
				fs := newReplayWriteTestFS(t)
				mock := &replayShortStorage{ObjectStorage: fs.storage, consume: 3}
				if fail {
					mock.fail = io.ErrUnexpectedEOF
				}
				fs.storage = mock
				vector := IOVector{FilePath: "partial", Entries: []IOEntry{{Offset: 0, Size: 7, Data: []byte("1234567")}}}
				var err error
				if mode == "ordinary" {
					err = fs.Write(ctx, vector)
				} else {
					err = writeReplayableData(ctx, fs, vector)
				}
				if fail {
					require.ErrorIs(t, err, io.ErrUnexpectedEOF)
					_, statErr := os.Stat(fs.diskCache.pathForFile("partial"))
					require.True(t, os.IsNotExist(statErr))
				} else {
					require.NoError(t, err)
					require.Equal(t, []byte("123"), readReplayCache(t, fs, "partial"))
				}
			})
		}
	}
}

type replayObservedWrapper struct {
	*S3FS
	writes    int
	sawReplay bool
}

func (w *replayObservedWrapper) Write(ctx context.Context, vector IOVector) error {
	w.writes++
	w.sawReplay = vector.Policy.Any(ReplayDataForDiskCache)
	return w.S3FS.Write(ctx, vector)
}

func TestReplayWritePreservesWrapperWrite(t *testing.T) {
	fs := newReplayWriteTestFS(t)
	wrapped := &replayObservedWrapper{S3FS: fs}
	vector := IOVector{FilePath: "wrapped", Entries: []IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}}}
	require.NoError(t, writeReplayableData(context.Background(), wrapped, vector))
	require.Equal(t, 1, wrapped.writes)
	require.True(t, wrapped.sawReplay)
	require.Equal(t, []byte("data"), readReplayCache(t, fs, "wrapped"))
}

func TestReplayWriteKnownWrappers(t *testing.T) {
	ctx := context.Background()
	fs := newReplayWriteTestFS(t)
	services, err := NewFileServices(fs.Name(), fs)
	require.NoError(t, err)
	vector := IOVector{FilePath: "services", Entries: []IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}}}
	require.NoError(t, writeReplayableData(ctx, services, vector))
	require.Equal(t, []byte("data"), readReplayCache(t, fs, "services"))
	sub := SubPath(fs, "scope")
	vector.FilePath = "nested"
	require.NoError(t, writeReplayableData(ctx, sub, vector))
	require.Equal(t, []byte("data"), readReplayCache(t, fs, "scope/nested"))
}

func TestReplayWriteConcurrentIndependentInputs(t *testing.T) {
	ctx := context.Background()
	fs := newReplayWriteTestFS(t)
	const count = 8
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path := fmt.Sprintf("concurrent-%d", i)
			source := bytes.Repeat([]byte{byte(i + 1)}, 1024)
			want := bytes.Clone(source)
			vector := IOVector{FilePath: path, Entries: []IOEntry{{Offset: 0, Size: int64(len(source)), Data: source}}}
			if err := writeReplayableData(ctx, fs, vector); err != nil {
				errs <- err
				return
			}
			clear(source)
			got, err := os.ReadFile(fs.diskCache.pathForFile(path))
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, want) {
				errs <- fmt.Errorf("cache mismatch for %s", path)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestReplayWriteSkipCacheAndCanceledContext(t *testing.T) {
	ctx := context.Background()
	fs := newReplayWriteTestFS(t)
	vector := IOVector{FilePath: "skip", Policy: SkipDiskCacheWrites, Entries: []IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}}}
	require.NoError(t, writeReplayableData(ctx, fs, vector))
	_, err := os.Stat(fs.diskCache.pathForFile("skip"))
	require.True(t, os.IsNotExist(err))
	r, err := fs.storage.Read(ctx, fs.pathToKey("skip"), nil, nil)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, []byte("data"), got)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	vector.FilePath = "canceled"
	vector.Policy = 0
	require.ErrorIs(t, writeReplayableData(canceled, fs, vector), context.Canceled)
	_, err = os.Stat(fs.diskCache.pathForFile("canceled"))
	require.True(t, os.IsNotExist(err))
}

func TestReplayWritePreservesExistingCacheBehavior(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"ordinary", "replay"} {
		t.Run(mode, func(t *testing.T) {
			fs := newReplayWriteTestFS(t)
			require.NoError(t, fs.diskCache.SetFile(ctx, "existing", func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("old"))), nil
			}))
			vector := IOVector{FilePath: "existing", Entries: []IOEntry{{Offset: 0, Size: 3, Data: []byte("new")}}}
			var err error
			if mode == "ordinary" {
				err = fs.Write(ctx, vector)
			} else {
				err = writeReplayableData(ctx, fs, vector)
			}
			require.NoError(t, err)
			// Preserve the existing cache admission behavior; this test does not
			// assert that an independently stale entry is desirable.
			require.Equal(t, []byte("old"), readReplayCache(t, fs, "existing"))
		})
	}
}

func TestReplayDataReader(t *testing.T) {
	entries := []IOEntry{
		{Offset: 0, Size: 2, Data: []byte("ab")},
		{Offset: 2, Size: 0},
		{Offset: 2, Size: 3, Data: []byte("cde")},
	}
	for _, tc := range []struct {
		limit int64
		want  string
	}{
		{0, ""}, {1, "a"}, {3, "abc"}, {5, "abcde"},
	} {
		t.Run(fmt.Sprintf("limit-%d", tc.limit), func(t *testing.T) {
			original := append([]IOEntry(nil), entries...)
			reader := &replayDataReader{ctx: context.Background(), entries: entries, remaining: tc.limit}
			require.NoError(t, iotest.TestReader(reader, []byte(tc.want)))
			require.Equal(t, original, entries)
		})
	}
	t.Run("incomplete input", func(t *testing.T) {
		reader := &replayDataReader{ctx: context.Background(), entries: entries, remaining: 6}
		got, err := io.ReadAll(reader)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Equal(t, []byte("abcde"), got)
	})
}

func TestReplayDataReaderCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &replayDataReader{ctx: ctx, entries: []IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}}, remaining: 4}
	buf := []byte{0xa5, 0xa5, 0xa5, 0xa5}
	n, err := r.Read(buf)
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []byte{0xa5, 0xa5, 0xa5, 0xa5}, buf)
}

type replayCancelStorage struct {
	ObjectStorage
	cancel context.CancelFunc
}

func (s *replayCancelStorage) Write(_ context.Context, _ string, r io.Reader, _ *int64, _ *time.Time) error {
	buf := make([]byte, 2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	s.cancel()
	_, err := r.Read(buf)
	return err
}

func TestReplayWriteCanceledDuringUpload(t *testing.T) {
	for _, mode := range []string{"ordinary", "replay"} {
		t.Run(mode, func(t *testing.T) {
			fs := newReplayWriteTestFS(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fs.storage = &replayCancelStorage{ObjectStorage: fs.storage, cancel: cancel}
			vector := IOVector{FilePath: "cancel-upload", Entries: []IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}}}
			var err error
			if mode == "ordinary" {
				err = fs.Write(ctx, vector)
			} else {
				err = writeReplayableData(ctx, fs, vector)
			}
			require.ErrorIs(t, err, context.Canceled)
			_, statErr := os.Stat(fs.diskCache.pathForFile("cancel-upload"))
			require.True(t, os.IsNotExist(statErr))
		})
	}
}

type replayMultipartStorage struct {
	ObjectStorage
	used bool
}

func (*replayMultipartStorage) SupportsParallelMultipart() bool { return true }
func (s *replayMultipartStorage) WriteMultipartParallel(ctx context.Context, key string, r io.Reader, size *int64, opt *ParallelMultipartOption) error {
	s.used = true
	return s.ObjectStorage.Write(ctx, key, r, size, opt.Expire)
}

func TestReplayWriteParallelMultipart(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"ordinary", "replay"} {
		t.Run(mode, func(t *testing.T) {
			fs := newReplayWriteTestFS(t)
			storage := &replayMultipartStorage{ObjectStorage: fs.storage}
			fs.storage = storage
			fs.parallelMode = ParallelForce
			vector := IOVector{FilePath: "multipart", Entries: []IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}}}
			var err error
			if mode == "ordinary" {
				err = fs.Write(ctx, vector)
			} else {
				err = writeReplayableData(ctx, fs, vector)
			}
			require.NoError(t, err)
			require.True(t, storage.used)
			require.Equal(t, []byte("data"), readReplayCache(t, fs, "multipart"))
		})
	}
}

func TestReplayWriteDiskCacheFailureParity(t *testing.T) {
	ctx := context.Background()
	failures := make([]bool, 0, 2)
	for _, mode := range []string{"ordinary", "replay"} {
		fs := newReplayWriteTestFS(t)
		cacheFile := fs.diskCache.pathForFile("readonly.mofscache/object")
		// The cache loader retains this suffix; a regular file cannot be a parent directory.
		blocker := filepath.Dir(cacheFile)
		require.Eventually(t, func() bool {
			if err := os.MkdirAll(filepath.Dir(blocker), 0700); err != nil {
				return false
			}
			return os.WriteFile(blocker, []byte("block"), 0600) == nil
		}, 5*time.Second, 10*time.Millisecond)
		vector := IOVector{FilePath: "readonly.mofscache/object", Entries: []IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}}}
		var writeErr error
		if mode == "ordinary" {
			writeErr = fs.Write(ctx, vector)
		} else {
			writeErr = writeReplayableData(ctx, fs, vector)
		}
		_, cacheErr := os.Stat(cacheFile)
		require.Error(t, cacheErr)
		failures = append(failures, writeErr != nil)
		r, err := fs.storage.Read(ctx, fs.pathToKey("readonly.mofscache/object"), nil, nil)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, []byte("data"), got)
	}
	require.Equal(t, failures[0], failures[1])
}
