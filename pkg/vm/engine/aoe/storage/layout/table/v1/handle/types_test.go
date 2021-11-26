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

package handle

import (
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	bmgr "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

var (
	moduleName = "handle"
)

func TestSnapshot(t *testing.T) {
	dir := testutils.InitTestEnv(moduleName, t)
	schema := metadata.MockSchema(2)
	row_count := uint64(64)
	seg_cnt := 4
	blk_cnt := 2
	cfg := storage.MetaCfg{
		BlockMaxRows:     row_count,
		SegmentMaxBlocks: uint64(blk_cnt),
	}
	opts := new(storage.Options)
	opts.Meta.Conf = &cfg
	opts.FillDefaults(dir)
	opts.Meta.Catalog, _ = opts.CreateCatalog(dir)
	opts.Meta.Catalog.Start()

	typeSize := uint64(schema.ColDefs[0].Type.Size)
	capacity := typeSize * row_count * uint64(seg_cnt) * uint64(blk_cnt) * 2
	indexBufMgr := bmgr.MockBufMgr(capacity)
	mtBufMgr := bmgr.MockBufMgr(capacity)
	sstBufMgr := bmgr.MockBufMgr(capacity)
	tables := table.NewTables(opts, new(sync.RWMutex), ldio.NewManager(dir, false), mtBufMgr, sstBufMgr, indexBufMgr, nil)

	catalog := opts.Meta.Catalog
	defer catalog.Close()
	gen := shard.NewMockIndexAllocator()
	shardId := uint64(100)
	tableMeta := metadata.MockDBTable(catalog, "db1", schema, nil, uint64(blk_cnt*seg_cnt), gen.Shard(shardId))

	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	t.Logf("TableData RefCount=%d", tableData.RefCount())
	segIDs := table.MockSegments(tableMeta, tableData)
	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	root := tableData.WeakRefRoot()
	assert.Equal(t, int64(1), root.RefCount())

	now := time.Now()

	cols := []int{0, 1}
	tableData.Ref()
	ss := NewSnapshot(segIDs, cols, tableData)
	segIt := ss.NewIt()
	actualSegCnt := 0
	actualBlkCnt := 0
	for segIt.Valid() {
		actualSegCnt++
		segment := segIt.GetHandle()
		blkIt := segment.NewIt()
		for blkIt.Valid() {
			actualBlkCnt++
			blk := blkIt.GetHandle()
			h := blk.Prefetch()
			h.Close()
			// blk.Close()
			blkIt.Next()
		}
		blkIt.Close()
		// segment.Close()
		segIt.Next()
	}
	segIt.Close()
	assert.Equal(t, seg_cnt, actualSegCnt)
	assert.Equal(t, seg_cnt*blk_cnt, actualBlkCnt)
	du := time.Since(now)
	t.Log(du)
	t.Log(sstBufMgr.String())
	t.Log(tableData.String())
	ss.Close()
	assert.Equal(t, int64(1), root.RefCount())

	tableData.Ref()
	ss2 := NewLinkAllSnapshot(cols, tableData)
	linkSegIt := ss2.NewIt()
	actualSegCnt = 0
	actualBlkCnt = 0
	for linkSegIt.Valid() {
		actualSegCnt++
		segment := linkSegIt.GetHandle()
		blkIt := segment.NewIt()
		for blkIt.Valid() {
			actualBlkCnt++
			blk := blkIt.GetHandle()
			h := blk.Prefetch()
			h.Close()
			// blk.Close()
			blkIt.Next()
		}
		blkIt.Close()
		// segment.Close()
		linkSegIt.Next()
	}
	linkSegIt.Close()
	assert.Equal(t, seg_cnt, actualSegCnt)
	assert.Equal(t, seg_cnt*blk_cnt, actualBlkCnt)

	linkSegIt = ss2.NewIt()
	for linkSegIt.Valid() {
		segment := linkSegIt.GetHandle()
		ids := segment.BlockIds()
		for _, id := range ids {
			blk := segment.GetBlock(id)
			assert.NotNil(t, blk)
			blkH := blk.Prefetch()
			blkH.Close()
		}
		linkSegIt.Next()
	}
	linkSegIt.Close()

	ss2.Close()
	assert.Equal(t, int64(1), root.RefCount())
	t.Log(tableData.String())
	t.Log(mtBufMgr.String())
	t.Log(sstBufMgr.String())
}
