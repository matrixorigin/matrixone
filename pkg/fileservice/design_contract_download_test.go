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
	"bytes"
	"context"
	"io"
	"sync"
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
	readers sync.WaitGroup
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
	return &gatedReader{r: r, gateAt: s.gateAt, release: s.release, done: ctx.Done(), ctx: ctx}, nil
}

type gatedReader struct {
	r       io.ReadCloser
	gateAt  int64
	read    int64
	release chan struct{}
	done    <-chan struct{}
	ctx     context.Context
}

// Read stops at gateAt until release, or until the request's context ends, as an HTTP
// response body does.
func (g *gatedReader) Read(p []byte) (int, error) {
	if g.read >= g.gateAt {
		select {
		case <-g.release:
		case <-g.done:
			return 0, g.ctx.Err()
		}
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
		storage.readers.Wait()
		fs.Close(ctx)
	})
	return fs, storage, data
}

type downloadContractRead struct {
	data []byte
	err  error
}

func (s *gatedStorage) readAsync(fs *S3FS, offset, size int64) <-chan downloadContractRead {
	done := make(chan downloadContractRead, 1)
	s.readers.Add(1)
	go func() {
		defer s.readers.Done()
		vec := IOVector{FilePath: "object", Entries: []IOEntry{{Offset: offset, Size: size}}}
		err := fs.Read(context.Background(), &vec)
		result := downloadContractRead{err: err}
		if err == nil {
			// Give the test a copy and release the read buffer even if the
			// test has already timed out and will never receive the result.
			result.data = bytes.Clone(vec.Entries[0].Data)
			vec.Release()
		} else {
			vec.ReleaseReadResultOnError()
		}
		done <- result
	}()
	return done
}

func requireReturns(t *testing.T, done <-chan downloadContractRead) []byte {
	t.Helper()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		return result.data
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("read did not return")
		return nil
	}
}

// The read that starts a download returns once its own bytes are written, other reads of
// written bytes take them from the download, a read of bytes not yet written waits for
// them when the margin allows, and the object is fetched from storage once.
func TestDesignDownloadFetchedOnce(t *testing.T) {
	fs, storage, data := newDownloadContractFS(t, time.Hour)

	leader := requireReturns(t, storage.readAsync(fs, 1024, 4))
	require.Equal(t, data[1024:1028], leader, "the first reader returned before the download finished")
	diskPath := fs.diskCache.pathForFile("object")
	require.NotNil(t, fs.diskCache.download(diskPath), "the download continues in the background")

	written := requireReturns(t, storage.readAsync(fs, 4096, 13))
	require.Equal(t, data[4096:4109], written)

	done := storage.readAsync(fs, 6<<20, 17)
	select {
	case result := <-done:
		t.Fatalf("a read of bytes not yet written returned before they were: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(storage.release)
	pending := requireReturns(t, done)
	require.Equal(t, data[6<<20:6<<20+17], pending)

	fs.FlushCache(context.Background())
	require.Equal(t, int64(1), storage.opens.Load(), "the object was fetched from storage once")
	require.FileExists(t, diskPath)
}

// "Otherwise it reads its range at once": with no margin, a read far behind the download
// fetches its range instead of waiting, and gets the right bytes.
func TestDesignDownloadFarBehindReadsRange(t *testing.T) {
	fs, storage, data := newDownloadContractFS(t, 0)
	// The download stops before it measures its own rate, and the recent rate puts the
	// read's bytes seconds after a range read.
	storage.gateAt = downloadRateMinBytes / 2
	downloadsRate(t, 1<<20)
	rangeCosts(t, 0.001, 1<<30)

	requireReturns(t, storage.readAsync(fs, 1024, 4))
	ranges := storage.ranges.Load()

	far := requireReturns(t, storage.readAsync(fs, 6<<20, 17))
	require.Equal(t, data[6<<20:6<<20+17], far)
	require.Greater(t, storage.ranges.Load(), ranges, "the read fetched its range")
	require.NotNil(t, fs.diskCache.download(fs.diskCache.pathForFile("object")), "without waiting for the stalled download")
}
