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
	"matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpgradeBlk(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(storage.Options)
	opts.FillDefaults("/tmp")
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)

	tables := table.NewTables(new(sync.RWMutex), fsMgr, bufMgr, bufMgr, bufMgr)
	info := md.MockInfo(&opts.Mu, row_count, blk_cnt)
	tableMeta := md.MockTable(info, schema, seg_cnt*blk_cnt)
	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	segIds := table.MockSegments(tableMeta, tableData)
	opts.Scheduler = NewScheduler(opts, tables)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.Segments {
		for _, blkMeta := range segMeta.Blocks {
			blkMeta.DataState = md.FULL
			blkMeta.Count = blkMeta.MaxRowCount
		}
	}

	for _, segID := range segIds[:1] {
		segMeta, err := tableMeta.ReferenceSegment(segID)
		assert.Nil(t, err)
		cpCtx := md.CopyCtx{}
		blkMeta, err := segMeta.CloneBlock(segMeta.Blocks[0].ID, cpCtx)
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

		segMeta.TryClose()
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
}

func TestUpgradeSeg(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(storage.Options)
	opts.FillDefaults("/tmp")
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)

	tables := table.NewTables(new(sync.RWMutex), fsMgr, bufMgr, bufMgr, bufMgr)
	info := md.MockInfo(&opts.Mu, row_count, blk_cnt)
	tableMeta := md.MockTable(info, schema, seg_cnt*blk_cnt)
	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	segIds := table.MockSegments(tableMeta, tableData)

	opts.Scheduler = NewScheduler(opts, tables)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.Segments {
		for _, blkMeta := range segMeta.Blocks {
			blkMeta.DataState = md.FULL
			blkMeta.Count = blkMeta.MaxRowCount
		}
		segMeta.TryClose()
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
}
