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

package fileservice

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

// Black-box checks of docs/design/20261106-fileservice-cache-ownership.md, "An object
// being downloaded is fetched once", through S3FS.Read with an object storage whose
// whole-object downloads stall after gateAt bytes until released.

type gatedStorage struct {
	ObjectStorage
	gateAt  int64
	release chan struct{}
	opens   atomic.Int64
	ranges  atomic.Int64
}

func (s *gatedStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	r, err := s.ObjectStorage.Read(ctx, key, min, max)
	if err != nil {
		return nil, err
	}
	s.opens.Add(1)
	if max != nil {
		s.ranges.Add(1)
		return r, nil
	}
	return &gatedReader{r: r, gateAt: s.gateAt, release: s.release}, nil
}

type gatedReader struct {
	r       io.ReadCloser
	gateAt  int64
	read    int64
	release chan struct{}
}

func (g *gatedReader) Read(p []byte) (int, error) {
	if g.read >= g.gateAt {
		<-g.release
	} else if rest := g.gateAt - g.read; int64(len(p)) > rest {
		p = p[:rest]
	}
	n, err := g.r.Read(p)
	g.read += int64(n)
	return n, err
}

func (g *gatedReader) Close() error { return g.r.Close() }

func newDownloadContractFS(t *testing.T, maxExtraWait time.Duration) (*S3FS, *gatedStorage, []byte) {
	ctx := context.Background()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{
		DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20),
		MemoryCapacity:           ptrTo[toml.ByteSize](0),
		DiskDownloadMaxExtraWait: &toml.Duration{Duration: maxExtraWait},
	}, nil, false, false)
	require.NoError(t, err)
	data := make([]byte, 8<<20)
	for i := range data {
		data[i] = byte(i*7 + i/1021)
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipDiskCache | SkipMemoryCache}))
	storage := &gatedStorage{ObjectStorage: fs.storage, gateAt: 1 << 20, release: make(chan struct{})}
	fs.storage = storage
	t.Cleanup(func() {
		select {
		case <-storage.release:
		default:
			close(storage.release)
		}
		fs.Close(ctx)
	})
	return fs, storage, data
}

func readAsync(fs *S3FS, offset, size int64) (*IOVector, chan error) {
	vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: offset, Size: size}}}
	done := make(chan error, 1)
	go func() { done <- fs.Read(context.Background(), vec) }()
	return vec, done
}

func requireReturns(t *testing.T, done chan error) {
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("read did not return")
	}
}

// The read that starts a download returns once its own bytes are written, other reads of
// written bytes take them from the download, a read of bytes not yet written waits for
// them when the margin allows, and the object is fetched from storage once.
func TestDesignDownloadFetchedOnce(t *testing.T) {
	fs, storage, data := newDownloadContractFS(t, time.Hour)

	leader, done := readAsync(fs, 1024, 4)
	requireReturns(t, done)
	require.Equal(t, data[1024:1028], leader.Entries[0].Data, "the first reader returned before the download finished")
	diskPath := fs.diskCache.pathForFile("object")
	require.NotNil(t, fs.diskCache.download(diskPath), "the download continues in the background")

	written, done := readAsync(fs, 4096, 13)
	requireReturns(t, done)
	require.Equal(t, data[4096:4109], written.Entries[0].Data)

	pending, done := readAsync(fs, 6<<20, 17)
	select {
	case err := <-done:
		t.Fatalf("a read of bytes not yet written returned before they were: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(storage.release)
	requireReturns(t, done)
	require.Equal(t, data[6<<20:6<<20+17], pending.Entries[0].Data)

	fs.FlushCache(context.Background())
	require.Equal(t, int64(1), storage.opens.Load(), "the object was fetched from storage once")
	require.FileExists(t, diskPath)
}

// "Otherwise it reads its range at once": with no margin, a read far behind the download
// fetches its range instead of waiting, and gets the right bytes.
func TestDesignDownloadFarBehindReadsRange(t *testing.T) {
	fs, storage, data := newDownloadContractFS(t, 0)

	_, done := readAsync(fs, 1024, 4)
	requireReturns(t, done)
	ranges := storage.ranges.Load()

	far, done := readAsync(fs, 6<<20, 17)
	requireReturns(t, done)
	require.Equal(t, data[6<<20:6<<20+17], far.Entries[0].Data)
	require.Greater(t, storage.ranges.Load(), ranges, "the read fetched its range")
	require.NotNil(t, fs.diskCache.download(fs.diskCache.pathForFile("object")), "without waiting for the stalled download")
}
