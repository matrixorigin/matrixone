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

package sched

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	bmgr "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

func TestUpgradeBlk(t *testing.T) {
	row_count := uint64(64)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)
	dir := testutils.InitTestEnv(moduleName, t)
	schema := metadata.MockSchema(2)
	opts := new(storage.Options)
	cfg := &storage.MetaCfg{
		BlockMaxRows:     row_count,
		SegmentMaxBlocks: blk_cnt,
	}
	opts.Meta.Conf = cfg
	opts.FillDefaults(dir)
	opts.Meta.Catalog, _ = opts.CreateCatalog(dir)
	opts.Meta.Catalog.Start()

	typeSize := uint64(schema.ColDefs[0].Type.Size)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager(dir, true)

	tables := table.NewTables(opts, new(sync.RWMutex), fsMgr, bufMgr, bufMgr, bufMgr, nil)

	dbName := "db1"
	gen := shard.NewMockIndexAllocator()
	shardId := uint64(99)
	idxGen := gen.Shard(shardId)
	tableMeta := metadata.MockDBTable(opts.Meta.Catalog, dbName, schema, nil, seg_cnt*blk_cnt, idxGen)
	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	segIds := table.MockSegments(tableMeta, tableData)
	opts.Scheduler = NewScheduler(opts, tables)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.SegmentSet {
		for _, blkMeta := range segMeta.BlockSet {
			blkMeta.Count = blkMeta.Segment.Table.Schema.BlockMaxRows
			blkMeta.SimpleUpgrade(nil)
		}
	}

	for _, segID := range segIds[:1] {
		segMeta := tableMeta.SimpleGetSegment(segID)
		assert.NotNil(t, segMeta)
		blkMeta := segMeta.BlockSet[0]
		assert.Nil(t, err)
		ctx := &Context{
			Opts:     opts,
			Waitable: true,
		}
		ctx.RemoveDataScope()
		tableData.Ref()
		e := NewUpgradeBlkEvent(ctx, blkMeta, tableData)
		opts.Scheduler.Schedule(e)
		err = e.WaitDone()
		assert.Nil(t, err)
		blk := e.Data
		assert.Equal(t, base.PERSISTENT_BLK, blk.GetType())
		blk.Unref()

		seg := tableData.StrongRefSegment(segID)
		assert.NotNil(t, seg)
		tableData.Ref()
		upseg := NewUpgradeSegEvent(ctx, seg, tableData)
		opts.Scheduler.Schedule(upseg)
		err = upseg.WaitDone()
		assert.Nil(t, err)
		assert.Equal(t, base.SORTED_SEG, upseg.Segment.GetType())
	}
	t.Log(fsMgr.String())
	t.Log(tableData.String())
	t.Log(bufMgr.String())
	opts.Scheduler.Stop()
	opts.Meta.Catalog.Close()
}

func TestUpgradeSeg(t *testing.T) {
	row_count := uint64(64)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)
	dir := testutils.InitTestEnv(moduleName, t)
	schema := metadata.MockSchema(2)
	opts := new(storage.Options)
	cfg := &storage.MetaCfg{
		BlockMaxRows:     row_count,
		SegmentMaxBlocks: blk_cnt,
	}
	opts.Meta.Conf = cfg
	opts.FillDefaults(dir)
	opts.Meta.Catalog, _ = opts.CreateCatalog(dir)
	opts.Meta.Catalog.Start()

	typeSize := uint64(schema.ColDefs[0].Type.Size)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager(dir, true)

	tables := table.NewTables(opts, new(sync.RWMutex), fsMgr, bufMgr, bufMgr, bufMgr, nil)

	dbName := "db1"
	gen := shard.NewMockIndexAllocator()
	shardId := uint64(0)

	tableMeta := metadata.MockDBTable(opts.Meta.Catalog, dbName, schema, nil, seg_cnt*blk_cnt, gen.Shard(shardId))
	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	segIds := table.MockSegments(tableMeta, tableData)

	opts.Scheduler = NewScheduler(opts, tables)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.SegmentSet {
		for _, blkMeta := range segMeta.BlockSet {
			blkMeta.Count = blkMeta.Segment.Table.Schema.BlockMaxRows
			blkMeta.SimpleUpgrade(nil)
		}
	}

	for _, segID := range segIds {
		ctx := &Context{
			Waitable: true,
			Opts:     opts,
		}
		old := tableData.StrongRefSegment(segID)
		tableData.Ref()
		e := NewUpgradeSegEvent(ctx, old, tableData)
		opts.Scheduler.Schedule(e)
		err = e.WaitDone()
		assert.Nil(t, err)
		seg := e.Segment
		assert.Equal(t, base.SORTED_SEG, seg.GetType())
	}
	t.Log(fsMgr.String())
	t.Log(tableData.String())
	// t.Log(bufMgr.String())
	opts.Scheduler.Stop()
	opts.Meta.Catalog.Close()
}
