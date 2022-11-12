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

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()
	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db", "")
	schema := catalog.MockSchema(1, 0)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	rel, _ := database.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	dataFactory := tables.NewDataFactory(db.Fs, db.MTBufMgr, db.Scheduler, db.Dir)
	tableFactory := dataFactory.MakeTableFactory()
	table := tableFactory(tableMeta)
	handle := table.GetHandle()
	_, err := handle.GetAppender()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableSegmentNotFound))
	seg, _ := rel.CreateSegment(false)
	blk, _ := seg.CreateBlock(false)
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
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableBlockNotFound))

	blk, _ = seg.CreateBlock(false)
	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)

	_, _, toAppend, err = appender.PrepareAppend(rows-toAppend, nil)
	assert.Nil(t, err)
	assert.Equal(t, schema.BlockMaxRows, toAppend)

	_, err = handle.GetAppender()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableSegmentNotFound))

	seg, _ = rel.CreateSegment(false)
	blk, _ = seg.CreateBlock(false)

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
	defer leaktest.AfterTest(t)()
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
		database, err := txn.CreateDatabase("db", "")
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
	defer p.Release()
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
		seg, err := rel.CreateSegment(false)
		assert.Nil(t, err)
		_, err = seg.CreateBlock(false)
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
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		txn, _ := db.StartTxn(nil)
		if _, err := txn.CreateDatabase("db", ""); err != nil {
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

func TestTxn4(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 8
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "")
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
	defer leaktest.AfterTest(t)()
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
		database, _ := txn.CreateDatabase("db", "")
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
	defer leaktest.AfterTest(t)()
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
		database, _ := txn.CreateDatabase("db", "")
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

		err := rel.UpdateByFilter(filter, uint16(3), int64(33))
		assert.NoError(t, err)

		err = rel.UpdateByFilter(filter, uint16(3), int64(44))
		assert.NoError(t, err)
		v, err := rel.GetValueByFilter(filter, 3)
		assert.NoError(t, err)
		assert.Equal(t, int64(44), v)

		filter.Val = int32(6)
		err = rel.UpdateByFilter(filter, uint16(3), int64(77))
		assert.NoError(t, err)

		err = rel.DeleteByFilter(filter)
		assert.NoError(t, err)

		// Double delete in a same txn -- FAIL
		err = rel.DeleteByFilter(filter)
		assert.Error(t, err)

		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			filter.Val = int32(5)
			v, err := rel.GetValueByFilter(filter, 3)
			assert.NoError(t, err)
			assert.NotEqual(t, int64(44), v)

			err = rel.UpdateByFilter(filter, uint16(3), int64(55))
			assert.Error(t, err)

			filter.Val = int32(7)
			err = rel.UpdateByFilter(filter, uint16(3), int64(88))
			assert.NoError(t, err)

			// Update row that has uncommitted delete -- FAIL
			filter.Val = int32(6)
			err = rel.UpdateByFilter(filter, uint16(3), int64(55))
			assert.Error(t, err)
			_, err = rel.GetValueByFilter(filter, 3)
			assert.NoError(t, err)
			err = txn.Rollback()
			assert.NoError(t, err)
		}
		filter.Val = int32(7)
		err = rel.UpdateByFilter(filter, uint16(3), int64(99))
		assert.NoError(t, err)

		assert.NoError(t, txn.Commit())

		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			filter.Val = int32(5)
			v, err := rel.GetValueByFilter(filter, 3)
			assert.NoError(t, err)
			assert.Equal(t, int64(44), v)

			filter.Val = int32(7)
			v, err = rel.GetValueByFilter(filter, 3)
			assert.NoError(t, err)
			assert.Equal(t, int64(99), v)

			filter.Val = int32(6)
			_, err = rel.GetValueByFilter(filter, 3)
			assert.Error(t, err)

			var buffer bytes.Buffer
			it := rel.MakeBlockIt()
			for it.Valid() {
				buffer.Reset()
				blk := it.GetBlock()
				view, err := blk.GetColumnDataByName(schema.ColDefs[3].Name, &buffer)
				assert.Nil(t, err)
				defer view.Close()
				assert.NotEqual(t, bats[0].Length(), view.Length())
				t.Log(view.DeleteMask.String())
				assert.Equal(t, bats[0].Length()-1, view.ApplyDeletes().Length())
				it.Next()
			}
		}
	}
}

func TestMergeBlocks1(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		database, _ := txn.CreateDatabase("db", "")
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
			err := blk.RangeDelete(4, 4, handle.DT_Normal)
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
	defer leaktest.AfterTest(t)()
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
		database, _ := txn.CreateDatabase("db", "")
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

func TestCompaction1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	cnt := uint32(2)
	rows := schema.BlockMaxRows / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
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
			view.Close()
			assert.True(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[1])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
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
			view.Close()
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
	}
}

func TestCompaction2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	db := initDB(t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 21
	schema.SegmentMaxBlocks = 4
	cnt := uint32(3)
	rows := schema.BlockMaxRows
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	time.Sleep(400 * time.Millisecond)
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(3, nil)
			assert.NotNil(t, view)
			view.Close()
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).IsAppendable())
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
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
			view.Close()
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).IsAppendable())
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
	}
}

// TestCompaction3 is a case for testing block refcount,
// which requires modification of the data block to test.
/*func TestCompaction3(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	db := initDB(t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 21
	schema.SegmentMaxBlocks = 4
    schema.Name = tables.ForTestBlockRefName
	cnt := uint32(3)
	rows := schema.BlockMaxRows / 3 * cnt
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
	go func() {
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[1])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}()
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(3, nil)
			assert.NotNil(t, view)
			view.Close()
			assert.True(t, blk.GetMeta().(*catalog.BlockEntry).IsAppendable())
			it.Next()
		}
	}
	time.Sleep(400 * time.Millisecond)
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(3, nil)
			assert.NotNil(t, view)
			view.Close()
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).IsAppendable())
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
	}
}*/
