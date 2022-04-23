package db

import (
	"bytes"
	"sync"
	"testing"
	"time"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func initTestContext(t *testing.T, dir string, txnBufSize, mutBufSize uint64) (*catalog.Catalog, *txnbase.TxnManager, txnbase.NodeDriver, base.INodeManager, base.INodeManager) {
	c := catalog.MockCatalog(dir, "mock", nil)
	driver := txnbase.NewNodeDriver(dir, "store", nil)
	txnBufMgr := buffer.NewNodeManager(txnBufSize, nil)
	mutBufMgr := buffer.NewNodeManager(mutBufSize, nil)
	factory := tables.NewDataFactory(mockio.SegmentFileMockFactory, mutBufMgr)
	mgr := txnbase.NewTxnManager(txnimpl.TxnStoreFactory(c, driver, txnBufMgr, factory), txnimpl.TxnFactory(c))
	mgr.Start()
	return c, mgr, driver, txnBufMgr, mutBufMgr
}

func TestTables1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, txnBufMgr, _ := initTestContext(t, dir, 100000, 1000000)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()
	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchema(1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	rel, _ := db.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)

	dataFactory := tables.NewDataFactory(mockio.SegmentFileMockFactory, txnBufMgr)
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
	toAppend, err := appender.PrepareAppend(rows)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	assert.Nil(t, err)
	t.Log(toAppend)

	toAppend, err = appender.PrepareAppend(rows - toAppend)
	assert.Equal(t, uint32(0), toAppend)
	appender.Close()

	appender, err = handle.GetAppender()
	assert.Equal(t, data.ErrAppendableBlockNotFound, err)

	blk, _ = seg.CreateBlock()
	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)

	toAppend, err = appender.PrepareAppend(rows - toAppend)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	appender.Close()

	appender, err = handle.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)

	seg, _ = rel.CreateSegment()
	blk, _ = seg.CreateBlock()

	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)
	toAppend, err = appender.PrepareAppend(rows - 2*toAppend)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	t.Log(c.SimplePPString(common.PPL1))
	txn.Rollback()
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTxn1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, txnBufMgr, mutBufMgr := initTestContext(t, dir, common.M*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchema(3)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 4
	schema.PrimaryKey = 2
	batchRows := uint64(schema.BlockMaxRows) * 2 / 5
	cnt := uint64(20)
	bat := compute.MockBatch(schema.Types(), batchRows*cnt, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 20)
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		db.CreateRelation(schema)
		err := txn.Commit()
		assert.Nil(t, err)
	}
	var wg sync.WaitGroup
	now := time.Now()
	doAppend := func(b *gbat.Batch) func() {
		return func() {
			defer wg.Done()
			txn := mgr.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			rel, err := db.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			err = rel.Append(b)
			assert.Nil(t, err)
			err = txn.Commit()
			assert.Nil(t, err)
		}
	}
	p, _ := ants.NewPool(4)
	for _, toAppend := range bats {
		wg.Add(1)
		p.Submit(doAppend(toAppend))
	}

	wg.Wait()

	t.Logf("Append takes: %s", time.Since(now))
	// expectBlkCnt := (uint32(batchRows)*uint32(batchCnt)*uint32(loopCnt)-1)/schema.BlockMaxRows + 1
	expectBlkCnt := (uint32(batchRows)*uint32(cnt)-1)/schema.BlockMaxRows + 1
	expectSegCnt := (expectBlkCnt-1)/uint32(schema.SegmentMaxBlocks) + 1
	// t.Log(expectBlkCnt)
	// t.Log(expectSegCnt)
	t.Log(txnBufMgr.String())
	t.Log(mutBufMgr.String())
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		_, err = seg.CreateBlock()
		assert.Nil(t, err)
	}
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
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
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTxn2(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, _, _ := initTestContext(t, dir, common.G, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		txn := mgr.StartTxn(nil)
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
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTxn3(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, _, _ := initTestContext(t, dir, common.M*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 8
	rows := uint64(30)
	colIdx := uint16(0)
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		rel, _ := db.CreateRelation(schema)
		bat := compute.MockBatch(schema.Types(), rows, int(schema.PrimaryKey), nil)
		// bat := mock.MockBatch(schema.Types(), uint64(schema.BlockMaxRows/4))
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
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
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

		err = blk.RangeDelete(0, 2)
		assert.Nil(t, err)

		// err = blk.Update(1, 0, int32(11))
		err = blk.Update(1, colIdx, int8(11))
		assert.NotNil(t, err)

		var comp bytes.Buffer
		var decomp bytes.Buffer
		vec, dels, err := blk.GetVectorCopy(schema.ColDefs[0].Name, &comp, &decomp)
		assert.Nil(t, err)
		assert.Equal(t, int(rows), vector.Length(vec))
		assert.Equal(t, 3, int(dels.GetCardinality()))
		assert.Equal(t, int8(100), compute.GetValue(vec, 5))
		// assert.Equal(t, int32(100), compute.GetValue(vec, 2))
		// Check w-w with uncommitted col update
		{
			txn := mgr.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			rel, _ := db.GetRelationByName(schema.Name)
			it := rel.MakeBlockIt()
			assert.True(t, it.Valid())
			blk := it.GetBlock()
			err := blk.Update(5, colIdx, int8(99))
			// err := blk.Update(5, colIdx, int32(99))
			assert.NotNil(t, err)
			err = blk.Update(8, colIdx, int8(88))
			// err = blk.Update(8, colIdx, int32(88))
			assert.Nil(t, err)

			err = blk.RangeDelete(2, 2)
			assert.NotNil(t, err)
			err = blk.Update(0, colIdx, int8(50))
			// err = blk.Update(0, colIdx, int32(200))
			assert.NotNil(t, err)

			txn.Rollback()
		}
		err = txn.Commit()
		assert.Nil(t, err)
	}
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		assert.True(t, it.Valid())
		blk := it.GetBlock()
		err := blk.Update(5, colIdx, int8(99))
		// err := blk.Update(5, colIdx, int32(99))
		assert.Nil(t, err)
		err = blk.Update(8, colIdx, int8(88))
		// err = blk.Update(8, colIdx, int32(88))
		assert.Nil(t, err)

		txn2 := mgr.StartTxn(nil)
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
		var comp bytes.Buffer
		var decomp bytes.Buffer
		vec, dels, err := it2.GetBlock().GetVectorCopy(schema.ColDefs[0].Name, &comp, &decomp)
		assert.Nil(t, err)
		t.Log(vec.String())
		assert.Equal(t, int(rows), vector.Length(vec))
		assert.Equal(t, 3, int(dels.GetCardinality()))
		assert.Equal(t, int8(100), compute.GetValue(vec, 5))
		// assert.Equal(t, int32(100), compute.GetValue(vec, 2))
		assert.Equal(t, int8(40), compute.GetValue(vec, 20))
		// assert.Equal(t, int32(50), compute.GetValue(vec, 17))

		assert.Nil(t, txn.Commit())
		vec, _, err = it2.GetBlock().GetVectorCopy(schema.ColDefs[colIdx].Name, &comp, &decomp)
		assert.Nil(t, err)
		t.Log(vec.Typ.String())
		// chain = it2.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetUpdateChain().(*updates.BlockUpdateChain)
		// t.Log(chain.StringLocked())
	}
}

func TestTxn4(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, _, _ := initTestContext(t, dir, common.M*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 8
	schema.PrimaryKey = 2
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		rel, _ := db.CreateRelation(schema)
		pk := gvec.New(schema.ColDefs[schema.PrimaryKey].Type)
		compute.AppendValue(pk, int32(1))
		compute.AppendValue(pk, int32(2))
		compute.AppendValue(pk, int32(1))
		provider := compute.NewMockDataProvider()
		provider.AddColumnProvider(int(schema.PrimaryKey), pk)
		bat := compute.MockBatch(schema.Types(), 3, int(schema.PrimaryKey), provider)
		err := rel.Append(bat)
		t.Log(err)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestTxn5(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, _, mutBufMgr := initTestContext(t, dir, common.M*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	schema.PrimaryKey = 2
	cnt := uint64(10)
	rows := uint64(schema.BlockMaxRows) / 2 * cnt
	bat := compute.MockBatch(schema.Types(), rows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(cnt))
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		db.CreateRelation(schema)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		err = rel.Append(bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}

	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[1])
		assert.Nil(t, err)

		txn2 := mgr.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		err = rel2.Append(bats[1])
		assert.Nil(t, err)
		err = rel2.Append(bats[2])
		assert.Nil(t, err)

		assert.Nil(t, txn2.Commit())
		assert.NotNil(t, txn.Commit())
		t.Log(txn2.String())
		t.Log(txn.String())
	}
	t.Log(mutBufMgr.String())
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTxn6(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, _, _ := initTestContext(t, dir, common.M*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	schema.PrimaryKey = 2
	cnt := uint64(10)
	rows := uint64(schema.BlockMaxRows) / 2 * cnt
	bat := compute.MockBatch(schema.Types(), rows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(cnt))
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		rel, _ := db.CreateRelation(schema)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
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

		err = rel.RangeDelete(id, row+1, row+1)
		assert.Nil(t, err)

		// Double delete in a same txn -- FAIL
		err = rel.RangeDelete(id, row+1, row+1)
		assert.NotNil(t, err)

		{
			txn := mgr.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			rel, _ := db.GetRelationByName(schema.Name)

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
			v, err = rel.GetValue(id, row+1, uint16(3))
			assert.Nil(t, err)
			txn.Rollback()
		}
		err = rel.Update(id, row+2, uint16(3), int64(99))
		assert.Nil(t, err)

		assert.Nil(t, txn.Commit())

		{
			txn := mgr.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			rel, _ := db.GetRelationByName(schema.Name)

			v, err := rel.GetValue(id, row, uint16(3))
			assert.Nil(t, err)
			assert.Equal(t, int64(44), v)

			v, err = rel.GetValue(id, row+2, uint16(3))
			assert.Nil(t, err)
			assert.Equal(t, int64(99), v)

			_, err = rel.GetValue(id, row+1, uint16(3))
			assert.NotNil(t, err)

			var comp bytes.Buffer
			var decomp bytes.Buffer
			it := rel.MakeBlockIt()
			for it.Valid() {
				comp.Reset()
				decomp.Reset()
				blk := it.GetBlock()
				vec, dels, err := blk.GetVectorCopy(schema.ColDefs[3].Name, &comp, &decomp)
				assert.Nil(t, err)
				assert.Equal(t, gvec.Length(bats[0].Vecs[0]), gvec.Length(vec))
				assert.True(t, dels.Contains(row+1))
				t.Log(dels.String())
				it.Next()
			}

			t.Log(rel.SimplePPString(common.PPL1))
		}
	}
}
