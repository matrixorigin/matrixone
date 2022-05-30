package db

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/stretchr/testify/assert"
)

// Testing Steps
// 1. Mock schema
// 2. Append data (append rows less than a block)
// 3. Scan committed hidden column data
func TestHidden1(t *testing.T) {
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
	assert.NoError(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	{
		var meta *catalog.BlockEntry
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			if blk.Rows() < int(schema.BlockMaxRows) {
				it.Next()
				continue
			}
			meta = blk.GetMeta().(*catalog.BlockEntry)
			break
			it.Next()
		}
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, tae.Scheduler)
		assert.NoError(t, err)
		err = task.OnExec()
		assert.NoError(t, err)
	}

	assert.NoError(t, txn.Commit())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
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
				assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5}, offsets)
			} else {
				assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, offsets)
			}
			it.Next()
		}
	}
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
