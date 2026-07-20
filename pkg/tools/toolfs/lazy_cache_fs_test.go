// Copyright 2021 Matrix Origin
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

package toolfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const objectIOMagic = 0xFFFFFFFF

type readHookFileService struct {
	fileservice.FileService
	read func(context.Context, *fileservice.IOVector) error
}

func (f *readHookFileService) Read(ctx context.Context, vector *fileservice.IOVector) error {
	return f.read(ctx, vector)
}

func TestLazyCacheFSConcurrentFailureIsSharedAndRetryable(t *testing.T) {
	ctx := context.Background()
	memory, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, memory.Write(ctx, fileservice.IOVector{
		FilePath: "dir/file",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 4, Data: []byte("data")}},
	}))

	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var reads atomic.Int32
	remote := &readHookFileService{FileService: memory}
	remote.read = func(ctx context.Context, vector *fileservice.IOVector) error {
		if reads.Add(1) == 1 {
			close(readStarted)
			<-releaseRead
			return assert.AnError
		}
		return memory.Read(ctx, vector)
	}

	fs, _, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)
	lazy := fs.(*lazyCacheFS)
	waiterReached := make(chan struct{})
	lazy.waitForCacheFillForTest = func() { close(waiterReached) }

	results := make(chan error, 2)
	go func() { results <- fs.PrefetchFile(ctx, "dir/file") }()
	<-readStarted
	go func() { results <- fs.PrefetchFile(ctx, "dir/file") }()
	<-waiterReached
	close(releaseRead)

	require.ErrorIs(t, <-results, assert.AnError)
	require.ErrorIs(t, <-results, assert.AnError)
	require.Equal(t, int32(1), reads.Load())

	lazy.waitForCacheFillForTest = nil
	require.NoError(t, fs.PrefetchFile(ctx, "dir/file"))
	require.Equal(t, int32(2), reads.Load())
}

func TestLazyCacheFSInvalidatedFillCannotPublishStaleData(t *testing.T) {
	ctx := context.Background()
	memory, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, memory.Write(ctx, fileservice.IOVector{
		FilePath: "dir/file",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 3, Data: []byte("old")}},
	}))

	oldReadReady := make(chan struct{})
	releaseOldRead := make(chan struct{})
	var reads atomic.Int32
	remote := &readHookFileService{FileService: memory}
	remote.read = func(ctx context.Context, vector *fileservice.IOVector) error {
		if reads.Add(1) == 1 {
			_, err := vector.Entries[0].WriterForRead.Write([]byte("old"))
			close(oldReadReady)
			<-releaseOldRead
			return err
		}
		return memory.Read(ctx, vector)
	}

	fs, _, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)
	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- fs.PrefetchFile(ctx, "dir/file") }()
	<-oldReadReady

	require.NoError(t, memory.Delete(ctx, "dir/file"))
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "dir/file",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 3, Data: []byte("new")}},
	}))
	close(releaseOldRead)
	require.NoError(t, <-prefetchDone)
	require.Equal(t, int32(2), reads.Load())

	vec := &fileservice.IOVector{FilePath: "dir/file", Entries: []fileservice.IOEntry{{Offset: 0, Size: -1}}}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, []byte("new"), vec.Entries[0].Data)
}

func TestLazyCacheFSDeleteInvalidatesInFlightFill(t *testing.T) {
	ctx := context.Background()
	memory, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, memory.Write(ctx, fileservice.IOVector{
		FilePath: "dir/file",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 3, Data: []byte("old")}},
	}))

	oldReadReady := make(chan struct{})
	releaseOldRead := make(chan struct{})
	var reads atomic.Int32
	remote := &readHookFileService{FileService: memory}
	remote.read = func(ctx context.Context, vector *fileservice.IOVector) error {
		if reads.Add(1) == 1 {
			_, err := vector.Entries[0].WriterForRead.Write([]byte("old"))
			close(oldReadReady)
			<-releaseOldRead
			return err
		}
		return memory.Read(ctx, vector)
	}

	fs, root, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)
	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- fs.PrefetchFile(ctx, "dir/file") }()
	<-oldReadReady
	require.NoError(t, fs.Delete(ctx, "dir/file"))
	close(releaseOldRead)
	require.Error(t, <-prefetchDone)
	require.Equal(t, int32(2), reads.Load())
	require.NoFileExists(t, filepath.Join(root, "dir/file"))
}

func TestLazyCacheFSReadsMagicPrefixedRemoteObjectsWithoutLocalChecksum(t *testing.T) {
	ctx := context.Background()
	remote, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	payload := make([]byte, 4, 32)
	binary.LittleEndian.PutUint32(payload, objectIOMagic)
	payload = append(payload, []byte("checkpoint-object-bytes")...)

	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   payload,
		}},
	}))

	fs, _, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)

	vec := &fileservice.IOVector{
		FilePath: "SHARED:ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, payload, vec.Entries[0].Data)

	var buf bytes.Buffer
	cacheVec := &fileservice.IOVector{
		FilePath: "ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset:        4,
			Size:          10,
			WriterForRead: &buf,
		}},
	}
	require.NoError(t, fs.ReadCache(ctx, cacheVec))
	require.Equal(t, payload[4:14], buf.Bytes())
}

func TestLazyCacheFSEvictsOldEntriesWhenOverLimit(t *testing.T) {
	t.Setenv("MO_TOOL_REMOTE_CACHE_SIZE", "32")

	ctx := context.Background()
	remote, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	first := bytes.Repeat([]byte("a"), 24)
	second := bytes.Repeat([]byte("b"), 24)
	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "ckp/first",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(first)),
			Data:   first,
		}},
	}))
	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "ckp/second",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(second)),
			Data:   second,
		}},
	}))

	fs, root, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)

	readAll := func(path string) []byte {
		vec := &fileservice.IOVector{
			FilePath: path,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   -1,
			}},
		}
		require.NoError(t, fs.Read(ctx, vec))
		return vec.Entries[0].Data
	}

	require.Equal(t, first, readAll("ckp/first"))
	require.FileExists(t, filepath.Join(root, "ckp/first"))

	require.Equal(t, second, readAll("ckp/second"))
	require.NoFileExists(t, filepath.Join(root, "ckp/first"))
	require.FileExists(t, filepath.Join(root, "ckp/second"))

	require.Equal(t, first, readAll("ckp/first"))
	require.FileExists(t, filepath.Join(root, "ckp/first"))
	require.NoFileExists(t, filepath.Join(root, "ckp/second"))
}

func TestLazyCacheFSDelegatesMetadataAndInvalidatesCachedWrites(t *testing.T) {
	ctx := context.Background()
	remote, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "dir/file",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   3,
			Data:   []byte("old"),
		}},
	}))

	fs, root, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)
	require.Equal(t, "SHARED", fs.Name())
	require.NotNil(t, fs.Cost())

	require.NoError(t, fs.PrefetchFile(ctx, "SHARED:dir/file"))
	require.FileExists(t, filepath.Join(root, "dir/file"))

	stat, err := fs.StatFile(ctx, "dir/file")
	require.NoError(t, err)
	require.Equal(t, int64(3), stat.Size)

	listed := make([]string, 0, 1)
	for entry, err := range fs.List(ctx, "dir") {
		require.NoError(t, err)
		listed = append(listed, entry.Name)
	}
	require.Contains(t, listed, "file")

	require.NoError(t, remote.Delete(ctx, "dir/file"))
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "SHARED:dir/file",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   3,
			Data:   []byte("new"),
		}},
	}))
	require.NoFileExists(t, filepath.Join(root, "dir/file"))

	vec := &fileservice.IOVector{
		FilePath: "dir/file",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, []byte("new"), vec.Entries[0].Data)

	require.NoError(t, fs.Delete(ctx, "SHARED:dir/file"))
	require.NoFileExists(t, filepath.Join(root, "dir/file"))
	_, err = fs.StatFile(ctx, "dir/file")
	require.Error(t, err)
}

func TestLazyCacheFSReadErrors(t *testing.T) {
	ctx := context.Background()
	remote, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "file",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   4,
			Data:   []byte("data"),
		}},
	}))

	fs, _, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)

	require.Error(t, fs.PrefetchFile(ctx, "missing"))

	require.Error(t, fs.Read(ctx, &fileservice.IOVector{
		FilePath: "file",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 0}},
	}))
	require.Error(t, fs.Read(ctx, &fileservice.IOVector{
		FilePath: "file",
		Entries:  []fileservice.IOEntry{{Offset: -1, Size: 1}},
	}))
	require.Error(t, fs.Read(ctx, &fileservice.IOVector{
		FilePath: "file",
		Entries:  []fileservice.IOEntry{{Offset: 10, Size: -1}},
	}))

	var buf bytes.Buffer
	require.Error(t, fs.Read(ctx, &fileservice.IOVector{
		FilePath: "file",
		Entries: []fileservice.IOEntry{{
			Offset:        3,
			Size:          8,
			WriterForRead: &buf,
		}},
	}))
}

func TestParseLazyCacheSize(t *testing.T) {
	tests := []struct {
		value string
		want  int64
		ok    bool
	}{
		{value: "42", want: 42, ok: true},
		{value: "2k", want: 2 << 10, ok: true},
		{value: "3MB", want: 3 << 20, ok: true},
		{value: "4GiB", want: 4 << 30, ok: true},
		{value: "", ok: false},
		{value: "bad", ok: false},
	}

	for _, tt := range tests {
		got, ok := parseLazyCacheSize(tt.value)
		require.Equal(t, tt.ok, ok)
		if tt.ok {
			require.Equal(t, tt.want, got)
		}
	}
}

func TestLazyCacheMaxBytesFromEnvDefault(t *testing.T) {
	old := os.Getenv("MO_TOOL_REMOTE_CACHE_SIZE")
	t.Cleanup(func() {
		if old == "" {
			_ = os.Unsetenv("MO_TOOL_REMOTE_CACHE_SIZE")
		} else {
			_ = os.Setenv("MO_TOOL_REMOTE_CACHE_SIZE", old)
		}
	})
	_ = os.Unsetenv("MO_TOOL_REMOTE_CACHE_SIZE")
	require.Equal(t, defaultLazyCacheMaxBytes, lazyCacheMaxBytesFromEnv())
}
