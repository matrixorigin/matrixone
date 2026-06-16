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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type releaseCountingData struct {
	releases *atomic.Int32
}

func (r *releaseCountingData) Size() int64 {
	return 0
}

func (r *releaseCountingData) Bytes() []byte {
	return nil
}

func (r *releaseCountingData) Slice(int) fscache.Data {
	return r
}

func (r *releaseCountingData) Retain() {
}

func (r *releaseCountingData) Release() {
	r.releases.Add(1)
}

func TestIOVectorReleaseReadResultOnErrorSkipsUndoneReleaseData(t *testing.T) {
	var cacheReleases atomic.Int32
	var doneReleaseData atomic.Int32
	var undoneReleaseData atomic.Int32

	vector := IOVector{
		Entries: []IOEntry{
			{
				CachedData: &releaseCountingData{releases: &cacheReleases},
				releaseData: func() {
					doneReleaseData.Add(1)
				},
				done:      true,
				fromCache: &MemCache{},
			},
			{
				CachedData: &releaseCountingData{releases: &cacheReleases},
				releaseData: func() {
					undoneReleaseData.Add(1)
				},
				fromCache: &MemCache{},
			},
		},
	}

	vector.ReleaseReadResultOnError()

	require.Equal(t, int32(2), cacheReleases.Load())
	require.Equal(t, int32(1), doneReleaseData.Load())
	require.Equal(t, int32(0), undoneReleaseData.Load())
	require.Nil(t, vector.Entries[0].CachedData)
	require.Nil(t, vector.Entries[0].releaseData)
	require.False(t, vector.Entries[0].done)
	require.Nil(t, vector.Entries[0].fromCache)
	require.Nil(t, vector.Entries[1].CachedData)
	require.NotNil(t, vector.Entries[1].releaseData)
	require.False(t, vector.Entries[1].done)
	require.Nil(t, vector.Entries[1].fromCache)
}

func TestIOVectorReadRangeDoesNotMutateReadToEndEntry(t *testing.T) {
	vector := IOVector{
		FilePath: "foo",
		Policy:   SkipFullFilePreloads,
		Entries: []IOEntry{
			{
				Offset: 4,
				Size:   -1,
			},
		},
	}

	min, max, readFull := vector.readRange()

	require.False(t, readFull)
	require.NotNil(t, min)
	require.Equal(t, int64(4), *min)
	require.Nil(t, max)
	require.Equal(t, int64(-1), vector.Entries[0].Size)
}

func TestIOVectorIOMergeKeyDoesNotMutateReadToEndEntry(t *testing.T) {
	vector := IOVector{
		FilePath: "foo",
		Policy:   SkipFullFilePreloads,
		Entries: []IOEntry{
			{
				Offset: 4,
				Size:   -1,
			},
		},
	}

	key := vector.ioMergeKey()

	require.False(t, key.FullObject)
	require.Equal(t, int64(4), key.Offset)
	require.Equal(t, int64(0), key.End)
	require.Equal(t, int64(-1), vector.Entries[0].Size)
}
