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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestFullFilePreloadReadModeAdmission(t *testing.T) {
	defer resetFullFilePreloadForTest(64<<20, 4, 128<<20)()

	vec := &IOVector{
		FilePath:         "foo",
		FullFileSizeHint: 8 << 20,
		Entries: []IOEntry{
			{Offset: 1024, Size: 4096},
		},
	}

	min, max, readFull := vec.readRange()
	require.True(t, readFull)
	require.NotNil(t, min)
	require.Equal(t, int64(0), *min)
	require.Nil(t, max)
	require.Equal(t, int64(0), fullFilePreloadInflight.Load())
	require.Nil(t, vec.preloadToken)
}

func TestFullFilePreloadReadRangeDoesNotAcquireToken(t *testing.T) {
	defer resetFullFilePreloadForTest(64<<20, 4, 128<<20)()

	vec := &IOVector{
		FilePath:         "foo",
		FullFileSizeHint: 8 << 20,
		Entries: []IOEntry{
			{Offset: 1024, Size: 4096},
		},
	}

	_, _, readFull := vec.readRange()
	require.True(t, readFull)
	require.Nil(t, vec.preloadToken)
	require.Equal(t, int64(0), fullFilePreloadInflight.Load())
}

func TestFullFilePreloadReadModeDowngradesOnPressure(t *testing.T) {
	defer resetFullFilePreloadForTest(64<<20, 4, 128<<20)()

	SetMemoryCachePressureTargetPercentByOwner("test-rss", 80, time.Now().Add(time.Minute))
	vec := &IOVector{
		FilePath:         "foo",
		FullFileSizeHint: 8 << 20,
		Entries: []IOEntry{
			{Offset: 1024, Size: 4096},
		},
	}

	vec.resolveS3ReadMode()
	min, max, readFull := vec.readRange()
	require.False(t, readFull)
	require.NotNil(t, min)
	require.NotNil(t, max)
	require.Equal(t, int64(1024), *min)
	require.Equal(t, int64(5120), *max)
}

func TestIOMergeKeyUsesResolvedReadMode(t *testing.T) {
	defer resetFullFilePreloadForTest(64<<20, 4, 4<<20)()

	vec := &IOVector{
		FilePath:         "foo",
		FullFileSizeHint: 128 << 20,
		Entries: []IOEntry{
			{Offset: 10, Size: 20},
		},
	}
	vec.resolveS3ReadMode()

	key := vec.ioMergeKey()
	require.Equal(t, int64(10), key.Offset)
	require.Equal(t, int64(30), key.End)

	min, max, readFull := vec.readRange()
	require.False(t, readFull)
	require.Equal(t, int64(10), *min)
	require.Equal(t, int64(30), *max)
}

func TestS3FSReadUsesDowngradedRange(t *testing.T) {
	defer resetFullFilePreloadForTest(64<<20, 4, 4<<20)()

	storage := &rangeRecordingObjectStorage{data: "0123456789abcdef"}
	fs := &S3FS{
		storage:  storage,
		ioMerger: NewIOMerger(),
	}

	vec := &IOVector{
		FilePath:         "foo",
		FullFileSizeHint: 8 << 20,
		Entries: []IOEntry{
			{Offset: 4, Size: 3},
		},
	}
	require.NoError(t, fs.Read(context.Background(), vec))
	require.Equal(t, []byte("456"), vec.Entries[0].Data)
	require.Len(t, storage.ranges, 1)
	require.Equal(t, readRangeRecord{min: ptrTo[int64](4), max: ptrTo[int64](7)}, storage.ranges[0])
}

func TestS3FSReadWithoutSizeHintUsesRange(t *testing.T) {
	defer resetFullFilePreloadForTest(64<<20, 4, 128<<20)()

	storage := &rangeRecordingObjectStorage{data: "0123456789abcdef"}
	fs := &S3FS{
		storage:  storage,
		ioMerger: NewIOMerger(),
	}

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{Offset: 4, Size: 3},
		},
	}
	require.NoError(t, fs.Read(context.Background(), vec))
	require.Equal(t, []byte("456"), vec.Entries[0].Data)
	require.Len(t, storage.ranges, 1)
	require.Equal(t, readRangeRecord{min: ptrTo[int64](4), max: ptrTo[int64](7)}, storage.ranges[0])
	require.Equal(t, int64(0), fullFilePreloadInflight.Load())
}

func TestS3FSFullFilePreloadTokenCoversResultProcessing(t *testing.T) {
	defer resetFullFilePreloadForTest(64<<20, 4, 128<<20)()

	storage := &rangeRecordingObjectStorage{data: "0123456789abcdef"}
	fs := &S3FS{
		storage:  storage,
		ioMerger: NewIOMerger(),
	}

	vec := &IOVector{
		FilePath:         "foo",
		FullFileSizeHint: int64(len(storage.data)),
		Entries: []IOEntry{
			{
				Size: int64(len(storage.data)),
				ToCacheData: func(ctx context.Context, r io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
					require.Equal(t, int64(1), fullFilePreloadInflight.Load())
					return allocator.CopyToCacheData(ctx, data), nil
				},
			},
		},
	}
	require.NoError(t, fs.Read(context.Background(), vec))
	require.Equal(t, int64(0), fullFilePreloadInflight.Load())
	require.NotNil(t, vec.Entries[0].CachedData)
	vec.Release()
}

type readRangeRecord struct {
	min *int64
	max *int64
}

type rangeRecordingObjectStorage struct {
	dummyObjectStorage
	data   string
	ranges []readRangeRecord
}

func (r *rangeRecordingObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	r.ranges = append(r.ranges, readRangeRecord{
		min: cloneInt64Ptr(min),
		max: cloneInt64Ptr(max),
	})
	start := int64(0)
	end := int64(len(r.data))
	if min != nil {
		start = *min
	}
	if max != nil {
		end = *max
	}
	return io.NopCloser(strings.NewReader(r.data[int(start):int(end)])), nil
}

func cloneInt64Ptr(v *int64) *int64 {
	if v == nil {
		return nil
	}
	return ptrTo(*v)
}
