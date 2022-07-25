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
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
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
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*4))
	defer bat.Close()
	bats := bat.Split(10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	{
		offsets := make([]uint32, 0)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, err := blk.GetColumnDataById(schema.HiddenKey.Idx, nil)
			assert.NoError(t, err)
			defer view.Close()
			fp := blk.Fingerprint()
			_ = view.GetData().Foreach(func(v any, _ int) (err error) {
				sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
				t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, fp.SegmentID, sid)
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
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		blk := getOneBlock(rel)
		view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil)
		assert.NoError(t, err)
		defer view.Close()
		offsets := make([]uint32, 0)
		fp := blk.Fingerprint()
		t.Log(fp.String())
		_ = view.GetData().Foreach(func(v any, _ int) (err error) {
			sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
			t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
			assert.Equal(t, fp.SegmentID, sid)
			assert.Equal(t, fp.BlockID, bid)
			offsets = append(offsets, offset)
			return
		}, nil)
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
	}

	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	err = rel.Append(bats[3])
	assert.NoError(t, err)
	err = rel.Append(bats[4])
	assert.NoError(t, err)
	err = rel.Append(bats[5])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae, "db", schema, false)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	var segMeta *catalog.SegmentEntry
	{
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil)
			assert.NoError(t, err)
			defer view.Close()
			offsets := make([]uint32, 0)
			meta := blk.GetMeta().(*catalog.BlockEntry)
			t.Log(meta.String())
			_ = view.GetData().Foreach(func(v any, _ int) (err error) {
				sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
				// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, meta.GetSegment().ID, sid)
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

	assert.NoError(t, txn.Commit())
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
			view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil)
			assert.NoError(t, err)
			defer view.Close()
			offsets := make([]uint32, 0)
			meta := blk.GetMeta().(*catalog.BlockEntry)
			t.Log(meta.String())
			t.Log(meta.GetSegment().String())
			_ = view.GetData().Foreach(func(v any, _ int) (err error) {
				sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
				// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, meta.GetSegment().ID, sid)
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

	assert.NoError(t, txn.Commit())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestGetDeleteUpdateByHiddenKey(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*6))
	defer bat.Close()
	bats := bat.Split(10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	assert.NoError(t, err)
	blk := getOneBlock(rel)
	view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil)
	assert.NoError(t, err)
	defer view.Close()
	_ = view.GetData().Foreach(func(v any, _ int) (err error) {
		sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
		t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
		expectV := bats[0].Vecs[3].Get(int(offset))
		cv, err := rel.GetValueByHiddenKey(v, 3)
		assert.NoError(t, err)
		assert.Equal(t, expectV, cv)
		if offset == 3 {
			err = rel.DeleteByHiddenKey(v)
			assert.NoError(t, err)
		} else {
			err = rel.UpdateByHiddenKey(v, 3, int64(9999))
			assert.NoError(t, err)
			err2 := rel.UpdateByHiddenKey(v, schema.HiddenKey.Idx, v)
			assert.ErrorIs(t, err2, data.ErrUpdateHiddenKey)
		}
		return
	}, nil)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	blk = getOneBlock(rel)

	assert.Equal(t, bats[0].Length()-1, int(rel.Rows()))
	view, err = blk.GetColumnDataById(3, nil)
	assert.NoError(t, err)
	defer view.Close()
	assert.Equal(t, bats[0].Length()-1, view.Length())
	_ = view.GetData().Foreach(func(v any, _ int) (err error) {
		assert.Equal(t, int64(9999), v)
		return
	}, nil)
	assert.NoError(t, txn.Commit())
}

// Testing Steps
// 1. Mock schema w/o primary key
// 2. Append data (append rows less than a block)
func TestHidden2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, -1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*4))
	defer bat.Close()
	bats := bat.Split(10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	{
		blk := getOneBlock(rel)
		var hidden *model.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(def.Idx, nil)
			assert.NoError(t, err)
			defer view.Close()
			assert.Equal(t, bats[0].Length(), view.Length())
			if def.IsHidden() {
				hidden = view
			}
		}
		_ = hidden.GetData().Foreach(func(key any, _ int) (err error) {
			sid, bid, offset := model.DecodeHiddenKeyFromValue(key)
			t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
			v, err := rel.GetValueByHiddenKey(key, schema.HiddenKey.Idx)
			assert.NoError(t, err)
			assert.Equal(t, key, v)
			if offset == 1 {
				err = rel.DeleteByHiddenKey(key)
				assert.NoError(t, err)
			}
			return
		}, nil)
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(def.Idx, nil)
			assert.NoError(t, err)
			defer view.Close()
			view.ApplyDeletes()
			assert.Equal(t, bats[0].Length()-1, view.Length())
		}
	}
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		blk := getOneBlock(rel)
		var hidden *model.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(def.Idx, nil)
			assert.NoError(t, err)
			defer view.Close()
			assert.Equal(t, bats[0].Length()-1, view.Length())
			if def.IsHidden() {
				hidden = view
			}
		}
		_ = hidden.GetData().Foreach(func(key any, _ int) (err error) {
			sid, bid, offset := model.DecodeHiddenKeyFromValue(key)
			t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
			v, err := rel.GetValueByHiddenKey(key, schema.HiddenKey.Idx)
			assert.NoError(t, err)
			assert.Equal(t, key, v)
			if offset == 1 {
				err = rel.DeleteByHiddenKey(key)
				assert.NoError(t, err)
			} else {
				err = rel.UpdateByHiddenKey(key, 2, int32(8888))
				assert.NoError(t, err)
			}
			return
		}, nil)
		fp := blk.Fingerprint()
		key := model.EncodeHiddenKey(fp.SegmentID, fp.BlockID, 2)
		v, err := rel.GetValueByHiddenKey(key, 2)
		assert.NoError(t, err)
		assert.Equal(t, int32(8888), v.(int32))
		t.Log(v)
	}
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

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
	assert.NoError(t, txn.Commit())

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
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	t.Log(rel.Rows())
	assert.Equal(t, int64(26), rel.Rows())
	{
		it := rel.MakeBlockIt()
		rows := 0
		for it.Valid() {
			blk := it.GetBlock()
			hidden, err := blk.GetColumnDataById(schema.HiddenKey.Idx, nil)
			assert.NoError(t, err)
			defer hidden.Close()
			hidden.ApplyDeletes()
			rows += hidden.Length()
			it.Next()
		}
		assert.Equal(t, 26, rows)
	}
	assert.NoError(t, txn.Commit())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}
