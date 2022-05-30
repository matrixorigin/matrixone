package db

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
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

	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	err := rel.Append(bats[0])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	{
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil, nil)
		assert.NoError(t, err)
		offsets := make([]uint32, 0)
		fp := blk.Fingerprint()
		t.Log(fp.String())
		_ = compute.ForEachValue(view.GetColumnData(), false, func(v any) (err error) {
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

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
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

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	{
		var metas []*catalog.BlockEntry
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			if blk.Rows() < int(schema.BlockMaxRows) {
				it.Next()
				continue
			}
			meta := blk.GetMeta().(*catalog.BlockEntry)
			metas = append(metas, meta)
			it.Next()
		}
		for _, meta := range metas {
			task, err := jobs.NewCompactBlockTask(nil, txn, meta, tae.Scheduler)
			assert.NoError(t, err)
			err = task.OnExec()
			assert.NoError(t, err)
		}
	}

	assert.NoError(t, txn.Commit())
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
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
			_ = compute.ForEachValue(view.GetColumnData(), false, func(v any) (err error) {
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

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
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
			_ = compute.ForEachValue(view.GetColumnData(), false, func(v any) (err error) {
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

// Testing Steps
// 1. Mock schema w/o primary key
// 2. Append data (append rows less than a block)
func TestHidden2(t *testing.T) {
	return
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, -1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*4)
	bats := compute.SplitBatch(bat, 10)

	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	err := rel.Append(bats[0])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}
