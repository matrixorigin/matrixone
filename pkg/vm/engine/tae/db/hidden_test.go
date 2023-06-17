// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"context"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

// Testing Steps
// 1. Mock schema with PK.
// 2. Append data (append rows less than a block) and commit. Scan hidden column and check.
// 3. Append data and the total rows is more than a block. Commit and then compact the full block.
// 4. Scan hidden column and check.
// 5. Append data and the total rows is more than a segment. Commit and then merge sort the full segment.
// 6. Scan hidden column and check.
func TestHiddenWithPK1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := initDB(ctx, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*4))
	defer bat.Close()
	bats := bat.Split(10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(context.Background(), bats[0])
	{
		offsets := make([]uint32, 0)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, err := blk.GetColumnDataById(context.Background(), schema.PhyAddrKey.Idx)
			assert.NoError(t, err)
			defer view.Close()
			fp := blk.Fingerprint()
			_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
				rid := v.(types.Rowid)
				bid, offset := rid.Decode()
				t.Logf("bid=%s,offset=%d", bid.String(), offset)
				assert.Equal(t, fp.BlockID, bid)
				offsets = append(offsets, offset)
				return
			}, nil)
			it.Next()
		}
		// sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		// assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
	}
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		blk := getOneBlock(rel)
		view, err := blk.GetColumnDataByName(context.Background(), catalog.PhyAddrColumnName)
		assert.NoError(t, err)
		defer view.Close()
		offsets := make([]uint32, 0)
		fp := blk.Fingerprint()
		t.Log(fp.String())
		_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
			rid := v.(types.Rowid)
			bid, offset := rid.Decode()
			t.Logf(",bid=%s,offset=%d", bid, offset)
			assert.Equal(t, fp.BlockID, bid)
			offsets = append(offsets, offset)
			return
		}, nil)
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
	}

	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[3])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[4])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[5])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	compactBlocks(t, 0, tae, "db", schema, false)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	var segMeta *catalog.SegmentEntry
	{
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, err := blk.GetColumnDataByName(context.Background(), catalog.PhyAddrColumnName)
			assert.NoError(t, err)
			defer view.Close()
			offsets := make([]uint32, 0)
			meta := blk.GetMeta().(*catalog.BlockEntry)
			t.Log(meta.String())
			_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
				rid := v.(types.Rowid)
				bid, offset := rid.Decode()
				// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, meta.ID, bid)
				offsets = append(offsets, offset)
				return
			}, nil)
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
			if meta.IsAppendable() {
				assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
			} else {
				segMeta = meta.GetSegment()
				assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, offsets)
			}
			it.Next()
		}
	}

	assert.NoError(t, txn.Commit(context.Background()))
	{
		seg := segMeta.GetSegmentData()
		factory, taskType, scopes, err := seg.BuildCompactionTaskFactory()
		assert.NoError(t, err)
		task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.NoError(t, err)
		err = task.WaitDone()
		assert.NoError(t, err)
	}

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, err := blk.GetColumnDataByName(context.Background(), catalog.PhyAddrColumnName)
			assert.NoError(t, err)
			defer view.Close()
			offsets := make([]uint32, 0)
			meta := blk.GetMeta().(*catalog.BlockEntry)
			t.Log(meta.String())
			t.Log(meta.GetSegment().String())
			_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
				rid := v.(types.Rowid)
				bid, offset := rid.Decode()
				// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, meta.ID, bid)
				offsets = append(offsets, offset)
				return
			}, nil)
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
			if meta.IsAppendable() {
				assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
			} else {
				assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, offsets)
			}
			it.Next()
		}
	}

	assert.NoError(t, txn.Commit(context.Background()))
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

// Testing Steps
// 1. Mock schema w/o primary key
// 2. Append data (append rows less than a block)
func TestHidden2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := initDB(ctx, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, -1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*4))
	defer bat.Close()
	bats := bat.Split(10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(context.Background(), bats[0])
	{
		blk := getOneBlock(rel)
		var hidden *containers.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(context.Background(), def.Idx)
			assert.NoError(t, err)
			defer view.Close()
			assert.Equal(t, bats[0].Length(), view.Length())
			if def.IsPhyAddr() {
				hidden = view
			}
		}
		_ = hidden.GetData().Foreach(func(key any, _ bool, _ int) (err error) {
			rid := key.(types.Rowid)
			bid, offset := rid.Decode()
			t.Logf(",bid=%s,offset=%d", bid, offset)
			v, _, err := rel.GetValueByPhyAddrKey(key, schema.PhyAddrKey.Idx)
			assert.NoError(t, err)
			assert.Equal(t, key, v)
			if offset == 1 {
				err = rel.DeleteByPhyAddrKey(key)
				assert.NoError(t, err)
			}
			return
		}, nil)
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(context.Background(), def.Idx)
			assert.NoError(t, err)
			defer view.Close()
			view.ApplyDeletes()
			assert.Equal(t, bats[0].Length()-1, view.Length())
		}
	}
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		blk := getOneBlock(rel)
		var hidden *containers.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(context.Background(), def.Idx)
			assert.NoError(t, err)
			defer view.Close()
			assert.Equal(t, bats[0].Length()-1, view.Length())
			if def.IsPhyAddr() {
				hidden = view
			}
		}
		_ = hidden.GetData().Foreach(func(key any, _ bool, _ int) (err error) {
			rid := key.(types.Rowid)
			bid, offset := rid.Decode()
			t.Logf(",bid=%s,offset=%d", bid, offset)
			v, _, err := rel.GetValueByPhyAddrKey(key, schema.PhyAddrKey.Idx)
			assert.NoError(t, err)
			assert.Equal(t, key, v)
			if offset == 1 {
				err = rel.DeleteByPhyAddrKey(key)
				assert.NoError(t, err)
			}
			return
		}, nil)
	}
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		it := rel.MakeBlockIt()
		blks := make([]data.Block, 0)
		for it.Valid() {
			blk := it.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData()
			blks = append(blks, blk)
			it.Next()
		}
		for _, blk := range blks {
			factory, taskType, scopes, err := blk.BuildCompactionTaskFactory()
			assert.NoError(t, err)
			task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
			assert.NoError(t, err)
			err = task.WaitDone()
			assert.NoError(t, err)
		}
	}
	assert.NoError(t, txn.Commit(context.Background()))

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		it := rel.MakeSegmentIt()
		segs := make([]data.Segment, 0)
		for it.Valid() {
			seg := it.GetSegment().GetMeta().(*catalog.SegmentEntry).GetSegmentData()
			segs = append(segs, seg)
			it.Next()
		}
		for _, seg := range segs {
			factory, taskType, scopes, err := seg.BuildCompactionTaskFactory()
			assert.NoError(t, err)
			if factory == nil {
				continue
			}
			task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
			assert.NoError(t, err)
			err = task.WaitDone()
			assert.NoError(t, err)
		}

	}
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	t.Log(rel.Rows())
	assert.Equal(t, int64(26), rel.Rows())
	{
		it := rel.MakeBlockIt()
		rows := 0
		for it.Valid() {
			blk := it.GetBlock()
			hidden, err := blk.GetColumnDataById(context.Background(), schema.PhyAddrKey.Idx)
			assert.NoError(t, err)
			defer hidden.Close()
			hidden.ApplyDeletes()
			rows += hidden.Length()
			it.Next()
		}
		assert.Equal(t, 26, rows)
	}
	assert.NoError(t, txn.Commit(context.Background()))
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}
