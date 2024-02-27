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

package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()
	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db", "", "")
	schema := catalog.MockSchema(1, 0)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 2
	rel, _ := database.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	dataFactory := tables.NewDataFactory(db.Runtime, db.Dir)
	tableFactory := dataFactory.MakeTableFactory()
	table := tableFactory(tableMeta)
	handle := table.GetHandle()
	_, err := handle.GetAppender()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound))
	obj, _ := rel.CreateObject(false)
	blk, _ := obj.CreateBlock(false)
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
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound))

	obj, _ = rel.CreateObject(false)
	blk, _ = obj.CreateBlock(false)
	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)

	_, _, toAppend, err = appender.PrepareAppend(rows-toAppend, nil)
	assert.Nil(t, err)
	assert.Equal(t, schema.BlockMaxRows, toAppend)

	_, err = handle.GetAppender()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound))

	obj, _ = rel.CreateObject(false)
	blk, _ = obj.CreateBlock(false)

	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)
	_, _, toAppend, err = appender.PrepareAppend(rows-2*toAppend, nil)
	assert.Nil(t, err)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	t.Log(db.Catalog.SimplePPString(common.PPL1))
	err = txn.Rollback(context.Background())
	assert.Nil(t, err)
	t.Log(db.Catalog.SimplePPString(common.PPL1))
}

func TestTxn1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	schema := catalog.MockSchema(3, 2)
	schema.BlockMaxRows = 10000
	batchRows := schema.BlockMaxRows * 2 / 5
	cnt := uint32(20)
	bat := catalog.MockBatch(schema, int(batchRows*cnt))
	defer bat.Close()
	bats := bat.Split(20)
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db", "", "")
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema)
		assert.Nil(t, err)
		err = txn.Commit(context.Background())
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
			err = rel.Append(context.Background(), b)
			assert.Nil(t, err)
			err = txn.Commit(context.Background())
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
	expectObjCnt := expectBlkCnt
	// t.Log(expectBlkCnt)
	// t.Log(expectObjCnt)
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		obj, err := rel.CreateObject(false)
		assert.Nil(t, err)
		_, err = obj.CreateBlock(false)
		assert.Nil(t, err)
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		objIt := rel.MakeObjectIt()
		objCnt := uint32(0)
		blkCnt := uint32(0)
		for objIt.Valid() {
			objCnt++
			blkIt := objIt.GetObject().MakeBlockIt()
			for blkIt.Valid() {
				blkCnt++
				blkIt.Next()
			}
			objIt.Next()
		}
		assert.Equal(t, expectObjCnt, objCnt)
		assert.Equal(t, expectBlkCnt, blkCnt)
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))
}

func TestTxn2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		txn, _ := db.StartTxn(nil)
		if _, err := txn.CreateDatabase("db", "", ""); err != nil {
			assert.Nil(t, txn.Rollback(context.Background()))
		} else {
			assert.Nil(t, txn.Commit(context.Background()))
		}
		t.Log(txn.String())
	}
	wg.Add(2)
	go run()
	go run()
	wg.Wait()
	t.Log(db.Catalog.SimplePPString(common.PPL1))
}

func TestTxn4(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 40000
	schema.ObjectMaxBlocks = 8
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		rel, _ := database.CreateRelation(schema)
		pk := containers.MakeVector(schema.GetSingleSortKey().Type, common.DefaultAllocator)
		defer pk.Close()
		pk.AppendMany([]any{int32(1), int32(2), int32(1)}, []bool{false, false, false})
		provider := containers.NewMockDataProvider()
		provider.AddColumnProvider(schema.GetSingleSortKeyIdx(), pk)
		bat := containers.MockBatchWithAttrs(schema.Types(), schema.Attrs(), 3, schema.GetSingleSortKeyIdx(), provider)
		defer bat.Close()
		err := rel.Append(context.Background(), bat)
		t.Log(err)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func TestTxn5(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 4
	cnt := uint32(10)
	rows := schema.BlockMaxRows / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		_, err := database.CreateRelation(schema)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		err = rel.Append(context.Background(), bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback(context.Background()))
	}

	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback(context.Background()))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[1])
		assert.Nil(t, err)

		txn2, _ := db.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		err = rel2.Append(context.Background(), bats[1])
		assert.Nil(t, err)
		err = rel2.Append(context.Background(), bats[2])
		assert.Nil(t, err)

		assert.Nil(t, txn2.Commit(context.Background()))
		assert.Error(t, txn.Commit(context.Background()))
		t.Log(txn2.String())
		t.Log(txn.String())
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))
}

func TestTxn6(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 4
	cnt := uint32(10)
	rows := schema.BlockMaxRows / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		filter := new(handle.Filter)
		filter.Op = handle.FilterEq
		filter.Val = int32(5)

		err := rel.UpdateByFilter(context.Background(), filter, uint16(3), int64(33), false)
		assert.NoError(t, err)

		err = rel.UpdateByFilter(context.Background(), filter, uint16(3), int64(44), false)
		assert.NoError(t, err)
		v, _, err := rel.GetValueByFilter(context.Background(), filter, 3)
		assert.NoError(t, err)
		assert.Equal(t, int64(44), v)

		filter.Val = int32(6)
		err = rel.UpdateByFilter(context.Background(), filter, uint16(3), int64(77), false)
		assert.NoError(t, err)

		err = rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)

		// Double delete in a same txn -- FAIL
		err = rel.DeleteByFilter(context.Background(), filter)
		assert.Error(t, err)

		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			filter.Val = int32(5)
			v, _, err := rel.GetValueByFilter(context.Background(), filter, 3)
			assert.NoError(t, err)
			assert.NotEqual(t, int64(44), v)

			err = rel.UpdateByFilter(context.Background(), filter, uint16(3), int64(55), false)
			assert.Error(t, err)

			filter.Val = int32(7)
			err = rel.UpdateByFilter(context.Background(), filter, uint16(3), int64(88), false)
			assert.NoError(t, err)

			// Update row that has uncommitted delete -- FAIL
			filter.Val = int32(6)
			err = rel.UpdateByFilter(context.Background(), filter, uint16(3), int64(55), false)
			assert.Error(t, err)
			_, _, err = rel.GetValueByFilter(context.Background(), filter, 3)
			assert.NoError(t, err)
			err = txn.Rollback(context.Background())
			assert.NoError(t, err)
		}
		filter.Val = int32(7)
		err = rel.UpdateByFilter(context.Background(), filter, uint16(3), int64(99), false)
		assert.NoError(t, err)

		assert.NoError(t, txn.Commit(context.Background()))

		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			filter.Val = int32(5)
			v, _, err := rel.GetValueByFilter(context.Background(), filter, 3)
			assert.NoError(t, err)
			assert.Equal(t, int64(44), v)

			filter.Val = int32(7)
			v, _, err = rel.GetValueByFilter(context.Background(), filter, 3)
			assert.NoError(t, err)
			assert.Equal(t, int64(99), v)

			filter.Val = int32(6)
			_, _, err = rel.GetValueByFilter(context.Background(), filter, 3)
			assert.Error(t, err)

			it := rel.MakeBlockIt()
			for it.Valid() {
				blk := it.GetBlock()
				view, err := blk.GetColumnDataByName(context.Background(), schema.ColDefs[3].Name, common.DefaultAllocator)
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

func TestFlushAblkMerge(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := new(options.Options)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 8
	col3Data := []int64{10, 8, 1, 6, 15, 7, 3, 12, 11, 4, 9, 5, 14, 13, 2}
	// col3Data := []int64{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pkData := []int32{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pk := containers.MakeVector(schema.GetSingleSortKey().Type, common.DefaultAllocator)
	defer pk.Close()
	col3 := containers.MakeVector(schema.ColDefs[3].Type, common.DefaultAllocator)
	defer col3.Close()
	mapping := make(map[int32]int64)
	for i, v := range pkData {
		pk.Append(v, false)
		col3.Append(col3Data[i], false)
		mapping[v] = col3Data[i]
	}

	provider := containers.NewMockDataProvider()
	provider.AddColumnProvider(schema.GetSingleSortKeyIdx(), pk)
	provider.AddColumnProvider(3, col3)
	bat := containers.MockBatchWithAttrs(schema.Types(), schema.Attrs(), int(schema.BlockMaxRows*3), schema.GetSingleSortKeyIdx(), provider)
	defer bat.Close()
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(context.Background(), bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
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
			err := blk.RangeDelete(4, 4, handle.DT_Normal, common.DefaultAllocator)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit(context.Background()))
		}
		start := time.Now()
		{
			task, err := jobs.NewFlushTableTailTask(nil, txn, blks, db.Runtime, types.MaxTs())
			assert.Nil(t, err)
			err = task.OnExec(context.Background())
			assert.Nil(t, err)
		}
		assert.Nil(t, txn.Commit(context.Background()))
		t.Logf("MergeSort takes: %s", time.Since(start))
		t.Log(db.Catalog.SimplePPString(common.PPL1))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(context.Background(), 3, common.DefaultAllocator)
			assert.NotNil(t, view)
			defer view.Close()
			if view.DeleteMask != nil {
				t.Log(view.DeleteMask.String())
			}
			pkView, _ := blk.GetColumnDataById(context.Background(), schema.GetSingleSortKeyIdx(), common.DefaultAllocator)
			defer pkView.Close()
			for i := 0; i < pkView.Length(); i++ {
				pkv, _ := pkView.GetValue(i)
				colv, _ := view.GetValue(i)
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	col3Data := []int64{10, 8, 1, 6, 15, 7, 3, 12, 11, 4, 9, 5, 14, 13, 2}
	// col3Data := []int64{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pkData := []int32{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}

	pk := containers.MakeVector(schema.GetSingleSortKey().Type, common.DefaultAllocator)
	col3 := containers.MakeVector(schema.ColDefs[3].Type, common.DefaultAllocator)
	mapping := make(map[int32]int64)
	for i, v := range pkData {
		pk.Append(v, false)
		col3.Append(col3Data[i], false)
		mapping[v] = col3Data[i]
	}

	provider := containers.NewMockDataProvider()
	provider.AddColumnProvider(schema.GetSingleSortKeyIdx(), pk)
	provider.AddColumnProvider(3, col3)
	bat := containers.MockBatchWithAttrs(schema.Types(), schema.Attrs(), int(schema.BlockMaxRows*3), schema.GetSingleSortKeyIdx(), provider)
	{
		txn, _ := tae.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(context.Background(), bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 4
	cnt := uint32(2)
	rows := schema.BlockMaxRows / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(context.Background(), 3, common.DefaultAllocator)
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
		err := rel.Append(context.Background(), bats[1])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(context.Background(), 3, common.DefaultAllocator)
			assert.NotNil(t, view)
			view.Close()
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
	}
}

func TestCompaction2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 21
	schema.ObjectMaxBlocks = 4
	cnt := uint32(3)
	rows := schema.BlockMaxRows
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}

	testutils.WaitExpect(5000, func() bool {
		dirty := db.BGCheckpointRunner.GetDirtyCollector().ScanInRangePruned(types.TS{}, types.MaxTs())
		return dirty.GetTree().Compact()
	})
	{
		txn, _ := db.TxnMgr.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(context.Background(), 3, common.DefaultAllocator)
			assert.NotNil(t, view)
			view.Close()
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).IsAppendable())
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
	}
	{
		txn, _ := db.TxnMgr.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(context.Background(), 3, common.DefaultAllocator)
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	db := initDB(t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 21
	schema.ObjectMaxBlocks = 4
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
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	go func() {
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[1])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}()
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			view, _ := blk.GetColumnDataById(context.Background(), 3)
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
			view, _ := blk.GetColumnDataById(context.Background(), 3)
			assert.NotNil(t, view)
			view.Close()
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).IsAppendable())
			assert.False(t, blk.GetMeta().(*catalog.BlockEntry).GetBlockData().IsAppendable())
			it.Next()
		}
	}
}*/
