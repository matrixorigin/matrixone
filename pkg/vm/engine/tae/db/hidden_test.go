package db

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*4)
	bats := compute.SplitBatch(bat, 10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	{
		offsets := make([]uint32, 0)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, err := blk.GetColumnDataById(schema.HiddenKey.Idx, nil, nil)
			assert.NoError(t, err)
			fp := blk.Fingerprint()
			_ = compute.ForEachValue(view.GetColumnData(), false, func(v any, _ uint32) (err error) {
				sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
				t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, fp.SegmentID, sid)
				assert.Equal(t, fp.BlockID, bid)
				offsets = append(offsets, offset)
				return
			})
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
		view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil, nil)
		assert.NoError(t, err)
		offsets := make([]uint32, 0)
		fp := blk.Fingerprint()
		t.Log(fp.String())
		_ = compute.ForEachValue(view.GetColumnData(), false, func(v any, _ uint32) (err error) {
			sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
			t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
			assert.Equal(t, fp.SegmentID, sid)
			assert.Equal(t, fp.BlockID, bid)
			offsets = append(offsets, offset)
			return
		})
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
			view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil, nil)
			assert.NoError(t, err)
			offsets := make([]uint32, 0)
			meta := blk.GetMeta().(*catalog.BlockEntry)
			t.Log(meta.String())
			_ = compute.ForEachValue(view.GetColumnData(), false, func(v any, _ uint32) (err error) {
				sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
				// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, meta.GetSegment().ID, sid)
				assert.Equal(t, meta.ID, bid)
				offsets = append(offsets, offset)
				return
			})
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
			view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil, nil)
			assert.NoError(t, err)
			offsets := make([]uint32, 0)
			meta := blk.GetMeta().(*catalog.BlockEntry)
			t.Log(meta.String())
			t.Log(meta.GetSegment().String())
			_ = compute.ForEachValue(view.GetColumnData(), false, func(v any, _ uint32) (err error) {
				sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
				// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
				assert.Equal(t, meta.GetSegment().ID, sid)
				assert.Equal(t, meta.ID, bid)
				offsets = append(offsets, offset)
				return
			})
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
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestGetDeleteUpdateByHiddenKey(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*6)
	bats := compute.SplitBatch(bat, 10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	assert.NoError(t, err)
	blk := getOneBlock(rel)
	view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil, nil)
	assert.NoError(t, err)
	_ = compute.ForEachValue(view.GetColumnData(), false, func(v any, _ uint32) (err error) {
		sid, bid, offset := model.DecodeHiddenKeyFromValue(v)
		t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
		expectV := compute.GetValue(bats[0].Vecs[3], offset)
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
	})
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	blk = getOneBlock(rel)

	assert.Equal(t, compute.LengthOfBatch(bats[0])-1, int(rel.Rows()))
	view, err = blk.GetColumnDataById(3, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, compute.LengthOfBatch(bats[0])-1, view.Length())
	_ = compute.ForEachValue(view.GetColumnData(), false, func(v any, _ uint32) (err error) {
		assert.Equal(t, int64(9999), v)
		return
	})
	assert.NoError(t, txn.Commit())
}

// Testing Steps
// 1. Mock schema w/o primary key
// 2. Append data (append rows less than a block)
func TestHidden2(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, -1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*4)
	bats := compute.SplitBatch(bat, 10)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	{
		blk := getOneBlock(rel)
		var hidden *model.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(def.Idx, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, compute.LengthOfBatch(bats[0]), view.Length())
			if def.IsHidden() {
				hidden = view
			}
		}
		_ = compute.ForEachValue(hidden.GetColumnData(), false, func(key any, _ uint32) (err error) {
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
		})
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(def.Idx, nil, nil)
			assert.NoError(t, err)
			view.ApplyDeletes()
			assert.Equal(t, compute.LengthOfBatch(bats[0])-1, view.Length())
		}
	}
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	{
		blk := getOneBlock(rel)
		var hidden *model.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(def.Idx, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, compute.LengthOfBatch(bats[0])-1, view.Length())
			if def.IsHidden() {
				hidden = view
			}
		}
		_ = compute.ForEachValue(hidden.GetColumnData(), false, func(key any, _ uint32) (err error) {
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
		})
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
			// hidden, err := blk.GetColumnDataById(0, nil, nil)
			hidden, err := blk.GetColumnDataById(schema.HiddenKey.Idx, nil, nil)
			assert.NoError(t, err)
			hidden.ApplyDeletes()
			rows += hidden.Length()
			it.Next()
		}
		assert.Equal(t, 26, rows)
	}
	assert.NoError(t, txn.Commit())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}
