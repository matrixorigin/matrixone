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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// openProbeStreams opens n streams of size bytes each of the object, from offset 0 on, and
// returns them open.
func openProbeStreams(t *testing.T, fs *S3FS, n int, size int64) []io.ReadCloser {
	streams := make([]io.ReadCloser, n)
	for i := range streams {
		var rc io.ReadCloser
		vec := &IOVector{FilePath: "object", Policy: SkipAllCache,
			Entries: []IOEntry{{Offset: int64(i) * size, Size: size, ReadCloserForRead: &rc}}}
		require.NoError(t, fs.Read(context.Background(), vec))
		require.NotNil(t, rc)
		streams[i] = rc
	}
	return streams
}

// A caller that keeps open streams whose buffers fill the flight budget and then reads a
// column range whose flight needs a buffer gets its bytes promptly. Control: the same read
// with no stream open returns, and the streams read their bytes.
func TestStreamProbeOpenStreamsDoNotBlockFlights(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	const size = 1 << 10
	fs.diskCache.prefetchFlightBytes = 2 * flightBufSize(size)

	control := PrefetchRange{Offset: 200 << 10, Size: size}
	got, err := probeRead(fs, control)
	require.NoError(t, err)
	require.Equal(t, data[control.Offset:control.Offset+control.Size], got)

	streams := openProbeStreams(t, fs, 2, size)
	defer func() {
		for _, s := range streams {
			_ = s.Close()
		}
	}()
	r := PrefetchRange{Offset: 250 << 10, Size: size}
	start := time.Now()
	got, err = probeRead(fs, r)
	require.NoError(t, err, "a read waited for the buffers of streams its caller holds open")
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got)

	for i, s := range streams {
		b, err := io.ReadAll(s)
		require.NoError(t, err)
		require.Equal(t, data[i*size:(i+1)*size], b)
	}
}

func probeStreamBytes(fs *S3FS) int64 {
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.streamBytes
}

// Streams take buffers only while their bytes fit the stream bound; the others read into
// their temporary files, every stream reads its bytes, and no stream charges the flight
// budget. Control: once they are closed, a stream takes a buffer again.
func TestStreamProbeStreamBytesWithinBound(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	const size = 1 << 10
	buf := flightBufSize(size)
	bound := 2 * buf
	fs.diskCache.memoryPrefetchMax = bound

	streams := openProbeStreams(t, fs, 6, size)
	require.Equal(t, bound, probeStreamBytes(fs), "the streams that fit take buffers")
	memory := 0
	for _, s := range streams {
		if s.(*streamReader).f.buf != nil {
			memory++
		}
	}
	require.Equal(t, 2, memory)
	flight, _ := probeFlightBytes(fs)
	require.Zero(t, flight, "streams charge no flight memory")
	for i, s := range streams {
		b, err := io.ReadAll(s)
		require.NoError(t, err)
		require.Equal(t, data[i*size:(i+1)*size], b)
		require.LessOrEqual(t, probeStreamBytes(fs), bound)
		require.NoError(t, s.Close())
	}
	require.Zero(t, probeStreamBytes(fs))

	again := openProbeStreams(t, fs, 1, size)
	require.Equal(t, buf, probeStreamBytes(fs))
	require.NoError(t, again[0].Close())
	require.Zero(t, probeStreamBytes(fs))
}
