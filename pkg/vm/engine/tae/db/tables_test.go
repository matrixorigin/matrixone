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

package db

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestTables1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()
	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchema(1, 0)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	rel, _ := database.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)

	dataFactory := tables.NewDataFactory(mockio.SegmentFactory, db.MTBufMgr, db.Scheduler, db.Dir)
	tableFactory := dataFactory.MakeTableFactory()
	table := tableFactory(tableMeta)
	handle := table.GetHandle()
	_, err := handle.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)
	seg, _ := rel.CreateSegment()
	blk, _ := seg.CreateBlock()
	id := blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender := handle.SetAppender(id)
	assert.NotNil(t, appender)

	blkCnt := 3
	rows := schema.BlockMaxRows * uint32(blkCnt)
	_, _, toAppend, err := appender.PrepareAppend(rows, nil)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	assert.Nil(t, err)
	t.Log(toAppend)

	_, _, toAppend, err = appender.PrepareAppend(rows-toAppend, nil)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), toAppend)

	_, err = handle.GetAppender()
	assert.Equal(t, data.ErrAppendableBlockNotFound, err)

	blk, _ = seg.CreateBlock()
	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)

	_, _, toAppend, err = appender.PrepareAppend(rows-toAppend, nil)
	assert.Nil(t, err)
	assert.Equal(t, schema.BlockMaxRows, toAppend)

	_, err = handle.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)

	seg, _ = rel.CreateSegment()
	blk, _ = seg.CreateBlock()

	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)
	_, _, toAppend, err = appender.PrepareAppend(rows-2*toAppend, nil)
	assert.Nil(t, err)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	err = txn.Rollback()
	assert.Nil(t, err)
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchema(3, 2)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 4
	batchRows := schema.BlockMaxRows * 2 / 5
	cnt := uint32(20)
	bat := catalog.MockBatch(schema, int(batchRows*cnt))
	defer bat.Close()
	bats := bat.Split(20)
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db")
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
	}
	var wg sync.WaitGroup
	now := time.Now()
	doAppend := func(b *containers.Batch) func() {
		return func() {
			defer wg.Done()
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, err := database.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			err = rel.Append(b)
			assert.Nil(t, err)
			err = txn.Commit()
			assert.Nil(t, err)
		}
	}
	p, err := ants.NewPool(4)
	assert.Nil(t, err)
	for _, toAppend := range bats {
		wg.Add(1)
		err := p.Submit(doAppend(toAppend))
		assert.Nil(t, err)
	}

	wg.Wait()

	t.Logf("Append takes: %s", time.Since(now))
	// expectBlkCnt := (uint32(batchRows)*uint32(batchCnt)*uint32(loopCnt)-1)/schema.BlockMaxRows + 1
	expectBlkCnt := (uint32(batchRows)*uint32(cnt)-1)/schema.BlockMaxRows + 1
	expectSegCnt := (expectBlkCnt-1)/uint32(schema.SegmentMaxBlocks) + 1
	// t.Log(expectBlkCnt)
	// t.Log(expectSegCnt)
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		_, err = seg.CreateBlock()
		assert.Nil(t, err)
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		segIt := rel.MakeSegmentIt()
		segCnt := uint32(0)
		blkCnt := uint32(0)
		for segIt.Valid() {
			segCnt++
			blkIt := segIt.GetSegment().MakeBlockIt()
			for blkIt.Valid() {
				blkCnt++
				blkIt.Next()
			}
			segIt.Next()
		}
		assert.Equal(t, expectSegCnt, segCnt)
		assert.Equal(t, expectBlkCnt, blkCnt)
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		txn, _ := db.StartTxn(nil)
		if _, err := txn.CreateDatabase("db"); err != nil {
			assert.Nil(t, txn.Rollback())
		} else {
			assert.Nil(t, txn.Commit())
		}
		t.Log(txn.String())
	}
	wg.Add(2)
	go run()
	go run()
	wg.Wait()
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn3(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 3)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 8
	rows := uint32(30)
	colIdx := uint16(0)
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		bat := catalog.MockBatch(schema, int(rows))
		defer bat.Close()
		for i := 0; i < 1; i++ {
			err := rel.Append(bat)
			assert.Nil(t, err)
		}
		err := txn.Commit()
		assert.Nil(t, err)
		t.Log(bat.Vecs[colIdx].String())
	}
	{
		// 1. Update ROW=5, VAL=99 -- PASS
		// 2. Update ROW=5, VAL=100 -- PASS
		// 3. Delete ROWS=[0,2] -- PASS
		// 4. Update ROW=1 -- FAIL
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		assert.True(t, it.Valid())
		blk := it.GetBlock()
		err := blk.Update(5, colIdx, int8(99))
		// err := blk.Update(5, colIdx, uint32(99))
		assert.Nil(t, err)

		// Txn can update a resource many times in a txn
		err = blk.Update(5, colIdx, int8(100))
		// err = blk.Update(5, colIdx, int32(100))
		assert.Nil(t, err)

		err = blk.RangeDelete(0, 2, handle.DT_Normal)
		assert.Nil(t, err)

		// err = blk.Update(1, 0, int32(11))
		err = blk.Update(1, colIdx, int8(11))
		assert.NotNil(t, err)

		var buffer bytes.Buffer
		view, err := blk.GetColumnDataById(0, &buffer)
		assert.Nil(t, err)
		defer view.Close()
		assert.Equal(t, int(rows), view.Length())
		assert.Equal(t, 3, int(view.DeleteMask.GetCardinality()))
		assert.Equal(t, int8(100), view.GetData().Get(5))
		// assert.Equal(t, int32(100), compute.GetValue(vec, 2))
		// Check w-w with uncommitted col update
		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			it := rel.MakeBlockIt()
			assert.True(t, it.Valid())
			blk := it.GetBlock()
			err := blk.Update(5, colIdx, int8(99))
			// err := blk.Update(5, colIdx, int32(99))
			assert.NotNil(t, err)
			err = blk.Update(8, colIdx, int8(88))
			// err = blk.Update(8, colIdx, int32(88))
			assert.Nil(t, err)

			err = blk.RangeDelete(2, 2, handle.DT_Normal)
			assert.NotNil(t, err)
			err = blk.Update(0, colIdx, int8(50))
			// err = blk.Update(0, colIdx, int32(200))
			assert.NotNil(t, err)

			err = txn.Rollback()
			assert.Nil(t, err)
		}
		err = txn.Commit()
		assert.Nil(t, err)
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		assert.True(t, it.Valid())
		blk := it.GetBlock()
		err := blk.Update(5, colIdx, int8(99))
		// err := blk.Update(5, colIdx, int32(99))
		assert.Nil(t, err)
		err = blk.Update(8, colIdx, int8(88))
		// err = blk.Update(8, colIdx, int32(88))
		assert.Nil(t, err)

		txn2, _ := db.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		it2 := rel2.MakeBlockIt()
		assert.True(t, it.Valid())
		blk2 := it2.GetBlock()
		err = blk2.Update(20, colIdx, int8(40))
		// err = blk2.Update(20, colIdx, int32(2000))
		assert.Nil(t, err)
		// chain := it2.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetUpdateChain().(*updates.BlockUpdateChain)
		// t.Log(chain.StringLocked())
		var buffer bytes.Buffer
		view, err := it2.GetBlock().GetColumnDataByName(schema.ColDefs[0].Name, &buffer)
		assert.Nil(t, err)
		vec := view.GetData()
		defer view.Close()
		assert.Equal(t, int(rows), vec.Length())
		assert.Equal(t, 3, int(view.DeleteMask.GetCardinality()))
		assert.Equal(t, int8(100), vec.Get(5))
		assert.Equal(t, int8(40), vec.Get(20))

		assert.Nil(t, txn.Commit())
		view, err = it2.GetBlock().GetColumnDataByName(schema.ColDefs[colIdx].Name, &buffer)
		assert.Nil(t, err)
		defer view.Close()
		// chain = it2.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetUpdateChain().(*updates.BlockUpdateChain)
		// t.Log(chain.StringLocked())
	}
}

func TestTxn4(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 8
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		pk := containers.MakeVector(schema.GetSingleSortKey().Type, schema.GetSingleSortKey().Nullable())
		defer pk.Close()
		pk.AppendMany(int32(1), int32(2), int32(1))
		provider := containers.NewMockDataProvider()
		provider.AddColumnProvider(schema.GetSingleSortKeyIdx(), pk)
		bat := containers.MockBatch(schema.Types(), 3, schema.GetSingleSortKeyIdx(), provider)
		defer bat.Close()
		err := rel.Append(bat)
		t.Log(err)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestTxn5(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	cnt := uint32(10)
	rows := schema.BlockMaxRows / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		_, err := database.CreateRelation(schema)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		err = rel.Append(bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}

	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[1])
		assert.Nil(t, err)

		txn2, _ := db.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		err = rel2.Append(bats[1])
		assert.Nil(t, err)
		err = rel2.Append(bats[2])
		assert.Nil(t, err)

		assert.Nil(t, txn2.Commit())
		assert.Error(t, txn.Commit())
		t.Log(txn2.String())
		t.Log(txn.String())
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn6(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	cnt := uint32(10)
	rows := schema.BlockMaxRows / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		filter := new(handle.Filter)
		filter.Op = handle.FilterEq
		filter.Val = int32(5)
		id, row, err := rel.GetByFilter(filter)
		assert.Nil(t, err)

		err = rel.Update(id, row, uint16(3), int64(33))
		assert.Nil(t, err)

		err = rel.Update(id, row, uint16(3), int64(44))
		assert.Nil(t, err)
		v, err := rel.GetValue(id, row, uint16(3))
		assert.Nil(t, err)
		assert.Equal(t, int64(44), v)

		err = rel.Update(id, row+1, uint16(3), int64(77))
		assert.Nil(t, err)

		err = rel.RangeDelete(id, row+1, row+1, handle.DT_Normal)
		assert.Nil(t, err)

		// Double delete in a same txn -- FAIL
		err = rel.RangeDelete(id, row+1, row+1, handle.DT_Normal)
		assert.NotNil(t, err)

		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			v, err := rel.GetValue(id, row, uint16(3))
			assert.Nil(t, err)
			assert.NotEqual(t, int64(44), v)

			err = rel.Update(id, row, uint16(3), int64(55))
			assert.NotNil(t, err)

			err = rel.Update(id, row+2, uint16(3), int64(88))
			assert.Nil(t, err)

			// Update row that has uncommitted delete -- FAIL
			err = rel.Update(id, row+1, uint16(3), int64(55))
			assert.NotNil(t, err)
			_, err = rel.GetValue(id, row+1, uint16(3))
			assert.Nil(t, err)
			err = txn.Rollback()
			assert.Nil(t, err)
		}
		err = rel.Update(id, row+2, uint16(3), int64(99))
		assert.Nil(t, err)

		assert.Nil(t, txn.Commit())

		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			v, err := rel.GetValue(id, row, uint16(3))
			assert.Nil(t, err)
			assert.Equal(t, int64(44), v)

			v, err = rel.GetValue(id, row+2, uint16(3))
			assert.Nil(t, err)
			assert.Equal(t, int64(99), v)

			_, err = rel.GetValue(id, row+1, uint16(3))
			assert.NotNil(t, err)

			var buffer bytes.Buffer
			it := rel.MakeBlockIt()
			for it.Valid() {
				buffer.Reset()
				blk := it.GetBlock()
				view, err := blk.GetColumnDataByName(schema.ColDefs[3].Name, &buffer)
				assert.Nil(t, err)
				defer view.Close()
				assert.Equal(t, bats[0].Length(), view.Length())
				assert.True(t, view.DeleteMask.Contains(row+1))
				t.Log(view.DeleteMask.String())
				it.Next()
			}

			t.Log(rel.SimplePPString(common.PPL1))
		}
	}
}

func TestMergeBlocks1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := new(options.Options)
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 5
	schema.SegmentMaxBlocks = 8
	col3Data := []int64{10, 8, 1, 6, 15, 7, 3, 12, 11, 4, 9, 5, 14, 13, 2}
	// col3Data := []int64{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pkData := []int32{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pk := containers.MakeVector(schema.GetSingleSortKey().Type, schema.GetSingleSortKey().Nullable())
	defer pk.Close()
	col3 := containers.MakeVector(schema.ColDefs[3].Type, schema.ColDefs[3].Nullable())
	defer col3.Close()
	mapping := make(map[int32]int64)
	for i, v := range pkData {
		pk.Append(v)
		col3.Append(col3Data[i])
		mapping[v] = col3Data[i]
	}

	provider := containers.NewMockDataProvider()
	provider.AddColumnProvider(schema.GetSingleSortKeyIdx(), pk)
	provider.AddColumnProvider(3, col3)
	bat := containers.MockBatch(schema.Types(), int(schema.BlockMaxRows*3), schema.GetSingleSortKeyIdx(), provider)
	defer bat.Close()
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}

	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		blks := make([]*catalog.BlockEntry, 0)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			meta := blk.GetMeta().(*catalog.BlockEntry)
			blks = append(blks, meta)
			it.Next()
		}
		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			it := rel.MakeBlockIt()
			blk := it.GetBlock()
			err := blk.Update(2, 3, int64(22))
			assert.Nil(t, err)
			pkv, err := rel.GetValue(blk.Fingerprint(), 2, uint16(schema.GetSingleSortKeyIdx()))
			mapping[pkv.(int32)] = int64(22)
			assert.Nil(t, err)
			err = blk.RangeDelete(4, 4, handle.DT_Normal)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
		start := time.Now()
		factory := jobs.MergeBlocksIntoSegmentTaskFctory(blks, blks[0].GetSegment(), db.Scheduler)
		// err = task.WaitDone()
		// assert.Nil(t, err)
		{
			task, err := factory(nil, txn)
			assert.Nil(t, err)
			err = task.OnExec()
			assert.Nil(t, err)
		}
		assert.Nil(t, txn.Commit())
		t.Logf("MergeSort takes: %s", time.Since(start))
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(3, nil)
			assert.NotNil(t, view)
			defer view.Close()
			if view.DeleteMask != nil {
				t.Log(view.DeleteMask.String())
			}
			pkView, _ := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil)
			defer pkView.Close()
			for i := 0; i < pkView.Length(); i++ {
				pkv := pkView.GetValue(i)
				colv := view.GetValue(i)
				assert.Equal(t, mapping[pkv.(int32)], colv)
			}
			it.Next()
		}
	}
	// testutils.WaitExpect(1000, func() bool {
	// 	return db.Wal.GetPenddingCnt() == 0
	// })
	// assert.Equal(t, uint64(0), db.Wal.GetPenddingCnt())
	t.Logf("Checkpointed: %d", db.Wal.GetCheckpointed())
	t.Logf("PendingCnt: %d", db.Wal.GetPenddingCnt())
}

func TestMergeBlocks2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 5
	schema.SegmentMaxBlocks = 2
	col3Data := []int64{10, 8, 1, 6, 15, 7, 3, 12, 11, 4, 9, 5, 14, 13, 2}
	// col3Data := []int64{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pkData := []int32{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}

	pk := containers.MakeVector(schema.GetSingleSortKey().Type, schema.GetSingleSortKey().Nullable())
	col3 := containers.MakeVector(schema.ColDefs[3].Type, schema.ColDefs[3].Nullable())
	mapping := make(map[int32]int64)
	for i, v := range pkData {
		pk.Append(v)
		col3.Append(col3Data[i])
		mapping[v] = col3Data[i]
	}

	provider := containers.NewMockDataProvider()
	provider.AddColumnProvider(schema.GetSingleSortKeyIdx(), pk)
	provider.AddColumnProvider(3, col3)
	bat := containers.MockBatch(schema.Types(), int(schema.BlockMaxRows*3), schema.GetSingleSortKeyIdx(), provider)
	{
		txn, _ := tae.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	pk.Close()
	col3.Close()
	bat.Close()
	start := time.Now()
	testutils.WaitExpect(2000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})
	t.Logf("Wait %s", time.Since(start))
	// assert.Equal(t, uint64(0), tae.Wal.GetPenddingCnt())
	t.Logf("Checkpointed: %d", tae.Wal.GetCheckpointed())
	t.Logf("PendingCnt: %d", tae.Wal.GetPenddingCnt())
	tae.Close()
}
