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
	"sync/atomic"
	"testing"
	"time"
)

// The fixed RTT and transfer rate model the transport, while S3FS.Read still
// performs the real range planning, io.ReadAll, and result ownership work.
type sparseReadBenchStorage struct {
	dummyObjectStorage
	object []byte
	rtt    time.Duration
	rate   int64
	opens  atomic.Int64
	bytes  atomic.Int64
}

func (s *sparseReadBenchStorage) Read(ctx context.Context, _ string, min, max *int64) (io.ReadCloser, error) {
	if min == nil || max == nil || *min < 0 || *max <= *min || *max > int64(len(s.object)) {
		return nil, fmt.Errorf("invalid benchmark range")
	}
	span := *max - *min
	s.opens.Add(1)
	delay := s.rtt
	if s.rate > 0 {
		delay += time.Duration(span) * time.Second / time.Duration(s.rate)
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return io.NopCloser(&countingReader{R: bytes.NewReader(s.object[*min:*max]), C: &s.bytes}), nil
}

func BenchmarkS3FSRangeCoalescing(b *testing.B) {
	for _, workload := range []struct {
		name    string
		offsets []int64
		size    int64
	}{
		{"five-sparse", []int64{0, 16 << 20, 32 << 20, 48 << 20, 64 << 20}, 4 << 20},
		{"five-adjacent", []int64{0, 4 << 20, 8 << 20, 12 << 20, 16 << 20}, 4 << 20},
		{"exact-eight-MiB-gap", []int64{0, 9 << 20, 10 << 20, 11 << 20, 12 << 20}, 1 << 20},
		{"below-eight-MiB-gap", []int64{0, 9<<20 - 1, 10<<20 - 1, 11<<20 - 1, 12<<20 - 1}, 1 << 20},
		{"exact-twelve-MiB-gap", []int64{0, 13 << 20, 14 << 20, 15 << 20, 16 << 20}, 1 << 20},
		{"below-twelve-MiB-gap", []int64{0, 13<<20 - 1, 14<<20 - 1, 15<<20 - 1, 16<<20 - 1}, 1 << 20},
		{"one-large-gap-three-small", []int64{0, 16 << 20, 16<<20 + 8192, 16<<20 + 16384, 16<<20 + 24576}, 4096},
	} {
		for _, transport := range []struct {
			name     string
			rtt      time.Duration
			rate     int64
			parallel bool
		}{
			{"local", 0, 0, false},
			{"20ms-200MiBs", 20 * time.Millisecond, 200 << 20, false},
			{"20ms-1GiBs", 20 * time.Millisecond, 1 << 30, false},
			{"20ms-200MiBs-parallel", 20 * time.Millisecond, 200 << 20, true},
		} {
			if transport.parallel && workload.name != "five-sparse" {
				continue
			}
			b.Run(workload.name+"/"+transport.name, func(b *testing.B) {
				b.StopTimer()
				ctx := context.Background()
				fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "bench", Endpoint: "disk", Bucket: b.TempDir()}, CacheConfig{}, nil, true, false)
				if err != nil {
					b.Fatal(err)
				}
				defer fs.Close(ctx)
				objectLen := workload.offsets[len(workload.offsets)-1] + workload.size
				object := make([]byte, objectLen)
				for pos := range object {
					object[pos] = byte((pos / 4096) % 251)
				}
				storage := &sparseReadBenchStorage{object: object, rtt: transport.rtt, rate: transport.rate}
				fs.storage = storage
				readOnce := func() error {
					vector := &IOVector{FilePath: "foo", Policy: SkipAllCache}
					defer vector.Release()
					for _, offset := range workload.offsets {
						vector.Entries = append(vector.Entries, IOEntry{Offset: offset, Size: workload.size})
					}
					if err := fs.Read(ctx, vector); err != nil {
						return err
					}
					for _, entry := range vector.Entries {
						if !bytes.Equal(entry.Data, object[entry.Offset:entry.Offset+entry.Size]) {
							return fmt.Errorf("wrong read result at offset %d", entry.Offset)
						}
					}
					return nil
				}
				b.ReportAllocs()
				b.StartTimer()
				if transport.parallel {
					var firstErr atomic.Pointer[error]
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if err := readOnce(); err != nil {
								errCopy := err
								firstErr.CompareAndSwap(nil, &errCopy)
							}
						}
					})
					if p := firstErr.Load(); p != nil {
						b.Fatal(*p)
					}
				} else {
					for range b.N {
						if err := readOnce(); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(storage.opens.Load())/float64(b.N), "gets/op")
				b.ReportMetric(float64(storage.bytes.Load())/float64(b.N), "body-B/op")
			})
		}
	}
}
