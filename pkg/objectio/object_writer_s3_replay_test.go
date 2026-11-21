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

package objectio

import (
	"bytes"
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

type replayPolicyObservingFS struct {
	fileservice.FileService
	policies []fileservice.Policy
}

func (f *replayPolicyObservingFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	f.policies = append(f.policies, vector.Policy)
	return f.FileService.Write(ctx, vector)
}

func TestObjectWriterS3CacheAfterArenaRelease(t *testing.T) {
	ctx := context.Background()
	var counters perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counters)
	cachePath := t.TempDir()
	diskCapacity := toml.ByteSize(64 << 20)
	service, err := fileservice.NewS3FS(ctx, fileservice.ObjectStorageArguments{
		Name: defines.SharedFileServiceName, Endpoint: "disk", Bucket: t.TempDir(),
	}, fileservice.CacheConfig{DiskPath: &cachePath, DiskCapacity: &diskCapacity}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { service.Close(ctx) })

	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	intType, textType := types.T_int64.ToType(), types.T_text.ToType()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(intType)
	bat.Vecs[1] = vector.NewVec(textType)
	t.Cleanup(func() { bat.Clean(mp) })
	values := [][]byte{bytes.Repeat([]byte("alpha"), 800), nil, bytes.Repeat([]byte("beta"), 700)}
	for i, value := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], value, value == nil, mp))
	}
	bat.SetRowCount(len(values))
	arena := NewArena(1 << 20)
	released := false
	t.Cleanup(func() {
		if !released {
			arena.FreeBuffers()
		}
	})
	objectID := NewObjectid()
	name := BuildObjectNameWithObjectID(&objectID)
	wrapped := &replayPolicyObservingFS{FileService: service}
	writer, err := NewObjectWriter(name, wrapped, 0, []uint16{0, 1}, arena)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	require.Len(t, wrapped.policies, 1)
	require.True(t, wrapped.policies[0].Any(fileservice.ReplayDataForDiskCache))
	require.Len(t, blocks, 1)
	extent := blocks[0].GetExtent()
	arena.FreeBuffers()
	released = true

	require.Eventually(t, func() bool {
		found := false
		_ = filepath.WalkDir(cachePath, func(_ string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".mofscache") {
				found = true
			}
			return nil
		})
		return found
	}, 10*time.Second, 10*time.Millisecond)
	before := counters.FileService.Cache.Disk.Hit.Load()
	reader, err := NewObjectReaderWithStr(name.String(), service)
	require.NoError(t, err)
	reader.CacheMetaExtent(&extent)
	read, err := reader.ReadOneBlock(ctx, []uint16{0, 1}, []types.Type{intType, textType}, 0, mp)
	require.NoError(t, err)
	defer read.Release()
	for col := range 2 {
		decoded, err := DecodeCached(read.Entries[col].CachedData)
		require.NoError(t, err)
		got := decoded.(*vector.Vector)
		require.Equal(t, len(values), got.Length())
		for i, value := range values {
			if col == 0 {
				require.Equal(t, int64(i), vector.MustFixedColNoTypeCheck[int64](got)[i])
			} else if value == nil {
				require.True(t, got.IsNull(uint64(i)))
			} else {
				require.Equal(t, value, got.GetBytesAt(i))
			}
		}
	}
	require.Greater(t, counters.FileService.Cache.Disk.Hit.Load(), before)
}
