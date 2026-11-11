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

package ivfflat

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// blockList is a RelData of blocks.
type blockList struct {
	engine.RelData
	blocks []objectio.BlockInfo
}

func (b *blockList) DataCnt() int                          { return len(b.blocks) }
func (b *blockList) GetBlockInfo(i int) objectio.BlockInfo { return b.blocks[i] }

// The objects of a scan's blocks are opened from the index-file cache once each, the
// in-memory block skipped, and stay cached after the scan.
func TestOpenIndexObjectsOfScan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	diskPath := filepath.Join(dir, "disk-cache")
	capacity := toml.ByteSize(64 << 20)
	fs, err := fileservice.NewS3FS(ctx, fileservice.ObjectStorageArguments{
		Name: defines.SharedFileServiceName, Endpoint: "disk", Bucket: filepath.Join(dir, "bucket"),
	}, fileservice.CacheConfig{
		DiskPath: &diskPath, DiskCapacity: &capacity, IndexFileDiskCapacity: &capacity,
		IndexFileCacheEnabled: true, IndexFileCacheObjects: true,
	}, nil, false, false)
	require.NoError(t, err)
	defer fs.Close(ctx)

	data := &blockList{blocks: []objectio.BlockInfo{objectio.EmptyBlockInfo}}
	names := make([]string, 0, 2)
	for range 2 {
		id := objectio.NewObjectid()
		name := objectio.BuildObjectNameWithObjectID(&id)
		names = append(names, name.String())
		require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: name.String(), Entries: []fileservice.IOEntry{{Size: 4096, Data: make([]byte, 4096)}}}))
		for blk := range 2 {
			loc := objectio.BuildLocation(name, objectio.NewExtent(0, 0, 0, 0), 1, uint16(blk))
			info := objectio.BlockInfo{BlockID: *objectio.BuildObjectBlockid(name, uint16(blk)), MetaLoc: objectio.ObjectLocation(loc)}
			data.blocks = append(data.blocks, info)
		}
	}

	// A CN's file services default to another service than SHARED, which holds the
	// objects.
	local, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	services, err := fileservice.NewFileServices(defines.LocalFileServiceName, local, fs)
	require.NoError(t, err)

	require.Equal(t, names, indexObjectNames(data), "each object of a persisted block, once")
	files := openIndexObjects(ctx, services, indexObjectNames(data))
	require.Len(t, files, 2)
	for _, f := range files {
		require.NotNil(t, f)
		require.Equal(t, int64(4096), f.Size())
	}
	closeIndexObjects(files)

	cache, _ := fileservice.IndexObjectCache(services)
	require.NotNil(t, cache)
	for _, name := range names {
		got, err := cache.Open(ctx, fileservice.IndexObjectEntry(name), fileservice.IndexObjectFile)
		require.NoError(t, err, "object %s stays cached", name)
		b, err := io.ReadAll(got[0])
		require.NoError(t, err)
		require.Len(t, b, 4096)
		require.NoError(t, got[0].Close())
	}

	require.Nil(t, openIndexObjects(ctx, nil, names), "no file service, no index objects")
}
