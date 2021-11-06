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

package table

import (
	"os"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	bmgr "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

var WORK_DIR = "/tmp/layout/data_test"

func TestBase1(t *testing.T) {
	segCnt := uint64(4)
	blkCnt := uint64(4)
	rowCount := uint64(10)
	capacity := uint64(200)
	os.RemoveAll(WORK_DIR)
	opts := new(storage.Options)
	cfg := &storage.MetaCfg{
		BlockMaxRows:     rowCount,
		SegmentMaxBlocks: blkCnt,
	}
	opts.Meta.Conf = cfg
	opts.FillDefaults(WORK_DIR)
	opts.Meta.Catalog, _ = opts.CreateCatalog(WORK_DIR)
	opts.Meta.Catalog.Start()
	defer opts.Meta.Catalog.Close()
	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	shardId := uint64(100)
	tableMeta := metadata.MockDBTable(opts.Meta.Catalog, "db1", schema, segCnt*blkCnt, gen.Shard(shardId))

	fsMgr := ldio.NewManager(WORK_DIR, true)
	indexBufMgr := bmgr.MockBufMgr(capacity)
	mtBufMgr := bmgr.MockBufMgr(2000)
	sstBufMgr := bmgr.MockBufMgr(capacity)
	tables := NewTables(new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	tblData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)

	idx := 0
	ids := make([]uint64, 0)
	for segId, _ := range tableMeta.IdIndex {
		delta := 0
		if idx > 0 {
			delta = 1
		}
		idx++
		ids = append(ids, segId)
		segMeta := tableMeta.SimpleGetSegment(segId)
		assert.NotNil(t, segMeta)
		seg, err := tblData.RegisterSegment(segMeta)
		assert.Nil(t, err)
		seg.Unref()
		assert.Equal(t, int64(1+delta), seg.RefCount())

		refSeg := tblData.StrongRefSegment(segId)
		refSeg.Unref()
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
		refSeg = tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())

		id := 0
		for blkId, _ := range segMeta.IdIndex {
			idelta := 0
			if id > 0 {
				idelta = 1
			}
			id++
			blkMeta := segMeta.SimpleGetBlock(blkId)
			assert.NotNil(t, blkMeta)
			blk, err := tblData.RegisterBlock(blkMeta)
			assert.Nil(t, err)
			blk.Unref()
			assert.Equal(t, int64(1+idelta), blk.RefCount())
		}
		refSeg = tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
	}

	assert.Equal(t, segCnt, uint64(tblData.GetSegmentCount()))

	t.Log(tblData.String())
	t.Log(fsMgr.String())

	for _, segMeta := range tableMeta.SegmentSet {
		for _, blkMeta := range segMeta.BlockSet {
			blkMeta.Count = blkMeta.Segment.Table.Schema.BlockMaxRows
			blkMeta.SimpleUpgrade(nil)
		}
	}

	for id, segId := range ids {
		delta := 0
		if id > 0 {
			delta = 1
		}
		segMeta := tableMeta.SimpleGetSegment(segId)
		assert.NotNil(t, segMeta)
		refSeg := tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
		for blkId, _ := range segMeta.IdIndex {
			blkMeta := segMeta.SimpleGetBlock(blkId)
			assert.NotNil(t, blkMeta)
			upgraded, err := tblData.UpgradeBlock(blkMeta)
			assert.Nil(t, err)
			upgraded.Unref()
			// assert.Equal(t, int64(1), upgraded.RefCount())
		}
	}

	t.Log(tblData.String())
	t.Log(fsMgr.String())
	for id, segId := range ids {
		delta := 0
		if id > 0 {
			delta = 1
		}
		refSeg := tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
		upgraded, err := tblData.UpgradeSegment(segId)
		assert.Nil(t, err)
		upgraded.Unref()
		assert.Equal(t, int64(1+delta), upgraded.RefCount())
	}

	for segId, _ := range tableMeta.IdIndex {
		segMeta := tableMeta.SimpleGetSegment(segId)
		assert.NotNil(t, segMeta)
		for blkId, _ := range segMeta.IdIndex {
			refBlk := tblData.WeakRefBlock(segId, blkId)
			assert.NotNil(t, refBlk)
			srefBlk := tblData.StrongRefBlock(segId, blkId)
			assert.NotNil(t, srefBlk)
			srefBlk.Unref()
			// handle := refBlk.GetBlockHandle()
			// handle.Close()
		}

	}
	t.Log(tblData.String())
	t.Log(mtBufMgr.String())
	t.Log(sstBufMgr.String())

	mutBlk := tblData.StrongRefLastBlock()
	assert.NotNil(t, mutBlk)
	mutBlk.Unref()
	root := tblData.WeakRefRoot()
	assert.Equal(t, int64(1), root.RefCount())
	sroot := tblData.StongRefRoot()
	assert.Equal(t, int64(2), sroot.RefCount())
	sroot.Unref()
	attr := schema.ColDefs[0].Name
	t.Log(tblData.Size(attr))
	index, ok := tblData.GetMeta().GetAppliedIndex(nil)
	assert.True(t, ok)
	assert.Equal(t, tableMeta.FirstCommitLocked().LogIndex.Id.Id, index)
}
