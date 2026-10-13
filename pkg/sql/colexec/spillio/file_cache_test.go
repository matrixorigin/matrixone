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

package spillio

import (
	"hash/crc32"
	"io"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSequentialWriteCacheBoundsRetainedTail(t *testing.T) {
	for _, tc := range []struct {
		name      string
		writeSize int
		writes    int
	}{
		{"small-appends", 64 << 10, 9},
		{"unaligned-records", (64 << 10) + 1, 9},
		{"oversized-records", (512 << 10) + 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "spill-cache-*")
			require.NoError(t, err)
			name := file.Name()
			t.Cleanup(func() {
				if file != nil {
					require.NoError(t, file.Close())
				}
			})

			cache := SequentialWriteCache{}
			payload := make([]byte, tc.writeSize)
			for i := range payload {
				payload[i] = byte(i*17 + 43)
			}
			expected := crc32.NewIEEE()
			pageSize := int64(os.Getpagesize())
			for range tc.writes {
				previousDrop := cache.dropped
				previousSubmit := cache.submitted
				written, writeErr := file.Write(payload)
				require.NoError(t, writeErr)
				require.Equal(t, len(payload), written)
				require.NoError(t, cache.RecordWrite(file, written))
				_, err = expected.Write(payload)
				require.NoError(t, err)
				if cache.written-previousSubmit < writebackBatchBytes {
					require.Equal(t, previousSubmit, cache.submitted)
				} else {
					require.Equal(t, cache.written-cache.written%pageSize, cache.submitted)
				}
				if cache.written-previousDrop <= retainedWriteCacheBytes {
					require.Equal(t, previousDrop, cache.dropped)
				} else {
					require.GreaterOrEqual(t, cache.dropped-previousDrop, writebackBatchBytes)
				}
				require.Zero(t, cache.dropped%pageSize)
				require.LessOrEqual(t, cache.dropped, cache.submitted)
				require.LessOrEqual(t, cache.submitted, cache.written)
				require.LessOrEqual(t, cache.written-cache.dropped, retainedWriteCacheBytes)

				// Progress belongs to the spill file, not to one descriptor.
				require.NoError(t, file.Close())
				file = nil
				file, err = os.OpenFile(name, os.O_RDWR, 0)
				require.NoError(t, err)
				_, err = file.Seek(0, io.SeekEnd)
				require.NoError(t, err)
			}
			require.Equal(t, int64(tc.writeSize*tc.writes), cache.written)
			cache.Finish(file)
			require.Equal(t, cache.written, cache.dropped)
			require.Equal(t, cache.written, cache.submitted)
			cache.Finish(file)
			_, err = file.Seek(0, io.SeekStart)
			require.NoError(t, err)
			actual := crc32.NewIEEE()
			n, err := io.Copy(actual, file)
			require.NoError(t, err)
			require.Equal(t, cache.written, n)
			require.Equal(t, expected.Sum32(), actual.Sum32())
		})
	}
}

func TestSequentialWriteCacheRejectsInvalidProgress(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "spill-cache-invalid-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	cache := SequentialWriteCache{}
	var absent *SequentialWriteCache
	require.ErrorIs(t, absent.RecordWrite(file, 1), errInvalidSequentialWrite)
	require.ErrorIs(t, cache.RecordWrite(nil, 1), errInvalidSequentialWrite)
	require.ErrorIs(t, cache.RecordWrite(file, 0), errInvalidSequentialWrite)
	require.ErrorIs(t, cache.RecordWrite(file, -1), errInvalidSequentialWrite)
	cache.submitted = 1
	require.ErrorIs(t, cache.RecordWrite(file, 1), errInvalidSequentialWrite)
	cache = SequentialWriteCache{written: 2, dropped: 1}
	require.ErrorIs(t, cache.RecordWrite(file, 1), errInvalidSequentialWrite)
	cache = SequentialWriteCache{}
	cache.written = math.MaxInt64
	require.ErrorIs(t, cache.RecordWrite(file, 1), errInvalidSequentialWrite)
	require.Equal(t, int64(math.MaxInt64), cache.written)
}
