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

package txnimpl

import (
	"context"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAETXN"
)

// 1. 30 concurrency
// 2. 10000 node
// 3. 512K buffer
// 4. 1K(30%), 4K(25%), 8K(%20), 16K(%15), 32K(%10)

//func getNodes() int {
//	v := rand.Intn(100)
//	if v < 30 {
//		return 1 * 2
//	} else if v < 55 {
//		return 2 * 2
//	} else if v < 75 {
//		return 3 * 2
//	} else if v < 90 {
//		return 4 * 2
//	}
//	return 5 * 2
//}

//func makeTable(t *testing.T, dir string, colCnt int, pkIdx int, bufSize uint64) *txnTable {
//	mgr := buffer.NewNodeManager(bufSize, nil)
//	driver := wal.NewDriverWithBatchStore(dir, "store", nil)
//	id := common.NextGlobalSeqNum()
//	schema := catalog.MockSchemaAll(colCnt, pkIdx)
//	rel := mockTestRelation(id, schema)
//	txn := txnbase.NewTxn(nil, nil, common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), nil)
//	store := newStore(nil, driver, nil, mgr, nil)
//	store.BindTxn(txn)
//	return newTxnTable(store, rel.GetMeta().(*catalog.TableEntry))
//}

//func TestInsertNode(t *testing.T) {
//	defer testutils.AfterTest(t)()
//	testutils.EnsureNoLeak(t)
//	dir := testutils.InitTestEnv(ModuleName, t)
//	tbl := makeTable(t, dir, 2, 1, mpool.KB*6)
//	defer tbl.store.driver.Close()
//	bat := catalog.MockBatch(tbl.GetSchema(), int(mpool.KB))
//	defer bat.Close()
//	p, _ := ants.NewPool(5)
//	defer p.Release()
//
//	var wg sync.WaitGroup
//	var all uint64
//
//	worker := func(id uint64) func() {
//		return func() {
//			defer wg.Done()
//			cnt := getNodes()
//			nodes := make([]*anode, cnt)
//			for i := 0; i < cnt; i++ {
//				var cid common.ID
//				cid.BlockID = id
//				cid.Idx = uint16(i)
//				n := NewANodeWithID(tbl, tbl.store.nodesMgr, &cid, tbl.store.driver)
//				nodes[i] = n
//				h := tbl.store.nodesMgr.Pin(n.storage.mnode)
//				var err error
//				if err = n.storage.mnode.Expand(mpool.KB*1, func() error {
//					_, err := n.Append(context.Background(), bat, 0)
//					return err
//				}); err != nil {
//					err = n.storage.mnode.Expand(mpool.KB*1, func() error {
//						_, err := n.Append(context.Background(), bat, 0)
//						return err
//					})
//				}
//				if err != nil {
//					assert.NotNil(t, err)
//				}
//				h.Close()
//			}
//			for _, n := range nodes {
//				// n.ToTransient()
//				n.Close()
//			}
//			atomic.AddUint64(&all, uint64(len(nodes)))
//		}
//	}
//	idAlloc := common.NewIdAllocator(1)
//	for {
//		id := idAlloc.Alloc()
//		if id > 10 {
//			break
//		}
//		wg.Add(1)
//		err := p.Submit(worker(id))
//		assert.Nil(t, err)
//	}
//	wg.Wait()
//	t.Log(all)
//	t.Log(tbl.store.nodesMgr.String())
//}

func TestTable(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 10
	{
		txn, _ := mgr.StartTxn(nil)
		db, err := txn.CreateDatabase("db", "", "")
		assert.Nil(t, err)
		rel, _ := db.CreateRelation(schema)
		bat := catalog.MockBatch(schema, int(mpool.KB)*100)
		defer bat.Close()
		bats := bat.Split(100)
		for _, data := range bats {
			err := rel.Append(context.Background(), data)
			assert.Nil(t, err)
		}
		tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
		tbl, _ := tDB.getOrSetTable(rel.ID())
		err = tbl.RangeDeleteLocalRows(1024+20, 1024+30)
		assert.Nil(t, err)
		err = tbl.RangeDeleteLocalRows(1024*2+38, 1024*2+40)
		assert.Nil(t, err)
		err = tbl.RangeDeleteLocalRows(1024*10+38, 1024*40+40)
		assert.Nil(t, err)
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024+20))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024+30))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024*10+38))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024*40+40))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024*30+40))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024+19))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024+31))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024*10+37))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024*40+41))
		err = txn.Commit(context.Background())
		assert.Nil(t, err)
	}
}

func TestAppend(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 10

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "", "")
	rel, _ := db.CreateRelation(schema)
	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())
	rows := uint64(MaxNodeRows) / 8 * 3
	brows := rows / 3

	bat := catalog.MockBatch(tbl.GetLocalSchema(false), int(rows))
	defer bat.Close()
	bats := bat.Split(3)

	err := tbl.BatchDedupLocal(bats[0])
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[0])
	assert.Nil(t, err)
	assert.Equal(t, int(brows), int(tbl.dataTable.tableSpace.Rows()))
	assert.Equal(t, int(brows), int(tbl.dataTable.tableSpace.index.Count()))

	err = tbl.BatchDedupLocal(bats[0])
	assert.NotNil(t, err)

	err = tbl.BatchDedupLocal(bats[1])
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[1])
	assert.Nil(t, err)
	assert.Equal(t, 2*int(brows), int(tbl.dataTable.tableSpace.Rows()))
	assert.Equal(t, 2*int(brows), int(tbl.dataTable.tableSpace.index.Count()))

	err = tbl.BatchDedupLocal(bats[2])
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[2])
	assert.Nil(t, err)
	assert.Equal(t, 3*int(brows), int(tbl.dataTable.tableSpace.Rows()))
	assert.Equal(t, 3*int(brows), int(tbl.dataTable.tableSpace.index.Count()))
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestIndex(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	index := NewSimpleTableIndex()
	err := index.Insert(1, 10)
	assert.Nil(t, err)
	err = index.Insert("one", 10)
	assert.Nil(t, err)
	row, err := index.Search("one")
	assert.Nil(t, err)
	assert.Equal(t, 10, int(row))
	err = index.Delete("one")
	assert.Nil(t, err)
	_, err = index.Search("one")
	assert.NotNil(t, err)

	schema := catalog.MockSchemaAll(14, 1)
	bat := catalog.MockBatch(schema, 500)
	defer bat.Close()

	idx := NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Attrs[0], bat.Vecs[0])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[0], bat.Vecs[0], 0, bat.Vecs[0].Length(), 0, false)
	assert.NotNil(t, err)

	err = idx.BatchDedup(bat.Attrs[1], bat.Vecs[1])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[1], bat.Vecs[1], 0, bat.Vecs[1].Length(), 0, false)
	assert.Nil(t, err)

	window := bat.Vecs[1].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(bat.Attrs[1], window)
	assert.NotNil(t, err)

	schema = catalog.MockSchemaAll(14, 12)
	bat = catalog.MockBatch(schema, 500)
	defer bat.Close()
	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Attrs[12], bat.Vecs[12])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[12], bat.Vecs[12], 0, bat.Vecs[12].Length(), 0, false)
	assert.Nil(t, err)

	window = bat.Vecs[12].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(bat.Attrs[12], window)
	assert.Error(t, err)

	// T_array
	schema = catalog.MockSchemaAll(20, 12)
	bat = catalog.MockBatch(schema, 500)
	defer bat.Close()
	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Attrs[19], bat.Vecs[19])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[19], bat.Vecs[19], 0, bat.Vecs[19].Length(), 0, false)
	assert.Nil(t, err)

	window = bat.Vecs[19].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(bat.Attrs[19], window)
	assert.Error(t, err)
}

func TestLoad(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14, 13)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 10

	bat := catalog.MockBatch(schema, 60000)
	defer bat.Close()
	bats := bat.Split(5)

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "", "")
	rel, _ := db.CreateRelation(schema)
	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())

	err := tbl.Append(context.Background(), bats[0])
	assert.NoError(t, err)

	v, _, err := tbl.dataTable.tableSpace.GetValue(100, 0)
	assert.NoError(t, err)
	t.Logf("Row %d, Col %d, Val %v", 100, 0, v)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestNodeCommand(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14, 13)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 10

	bat := catalog.MockBatch(schema, 15000)
	defer bat.Close()

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "", "")
	rel, _ := db.CreateRelation(schema)

	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())
	err := tbl.Append(context.Background(), bat)
	assert.Nil(t, err)

	err = tbl.RangeDeleteLocalRows(100, 200)
	assert.NoError(t, err)

	inode := tbl.dataTable.tableSpace.node
	cmd, err := inode.MakeCommand(uint32(0))
	assert.NoError(t, err)
	assert.NotNil(t, cmd.(*AppendCmd).Data)
	//if entry != nil {
	//	_ = entry.WaitDone()
	//	entry.Free()
	//}
	if cmd != nil {
		t.Log(cmd.String())
	}
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestTxnManager1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	mgr := txnbase.NewTxnManager(TxnStoreFactory(context.Background(), nil, nil, nil, nil, 0),
		TxnFactory(nil), types.NewMockHLCClock(1))
	mgr.Start(context.Background())
	txn, _ := mgr.StartTxn(nil)
	txn.MockIncWriteCnt()

	lock := sync.Mutex{}
	seqs := make([]int, 0)

	txn.SetPrepareCommitFn(func(_ txnif.AsyncTxn) error {
		time.Sleep(time.Millisecond * 100)
		lock.Lock()
		seqs = append(seqs, 2)
		lock.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	short := func() {
		defer wg.Done()
		txn2, _ := mgr.StartTxn(nil)
		txn2.MockIncWriteCnt()
		txn2.SetPrepareCommitFn(func(_ txnif.AsyncTxn) error {
			lock.Lock()
			seqs = append(seqs, 4)
			lock.Unlock()
			return nil
		})
		time.Sleep(10 * time.Millisecond)
		lock.Lock()
		seqs = append(seqs, 1)
		lock.Unlock()
		txn.GetTxnState(true)
		lock.Lock()
		seqs = append(seqs, 3)
		lock.Unlock()
		err := txn2.Commit(context.Background())
		assert.Nil(t, err)
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go short()
	}

	err := txn.Commit(context.Background())
	assert.Nil(t, err)
	wg.Wait()
	defer mgr.Stop()
	expected := []int{1, 2, 3, 4}
	assert.Equal(t, expected, seqs)
}

func initTestContext(ctx context.Context, t *testing.T, dir string) (*catalog.Catalog, *txnbase.TxnManager, wal.Driver) {
	c := catalog.MockCatalog()
	driver := wal.NewDriverWithBatchStore(context.Background(), dir, "store", nil)
	serviceDir := path.Join(dir, "data")
	service := objectio.TmpNewFileservice(ctx, path.Join(dir, "data"))
	fs := objectio.NewObjectFS(service, serviceDir)
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
	)
	factory := tables.NewDataFactory(rt, dir)
	mgr := txnbase.NewTxnManager(TxnStoreFactory(context.Background(), c, driver, rt, factory, 0),
		TxnFactory(c), types.NewMockHLCClock(1))
	rt.Now = mgr.Now
	mgr.Start(context.Background())
	return c, mgr, driver
}

// 1. Txn1 create database "db" and table "tb1". Commit
// 2. Txn2 drop database
// 3. Txn3 create table "tb2"
// 4. Txn2 commit
// 5. Txn3 commit
func TestTransaction1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn1, _ := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1, 0)
	db, err := txn1.CreateDatabase(name, "", "")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn1.Commit(context.Background())
	assert.Nil(t, err)
	t.Log(c.SimplePPString(common.PPL1))

	txn2, _ := mgr.StartTxn(nil)
	db2, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(db2.String())

	txn3, _ := mgr.StartTxn(nil)
	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(db3.String())
	schema = catalog.MockSchema(1, 0)
	rel, err := db3.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn2.Commit(context.Background())
	assert.Nil(t, err)
	err = txn3.Commit(context.Background())
	assert.NoError(t, err)
	// assert.Equal(t, txnif.TxnStateRollbacked, txn3.GetTxnState(true))
	t.Log(txn3.String())
	t.Log(db2.String())
	t.Log(rel.String())
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTransaction2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	name := "db"
	txn1, _ := mgr.StartTxn(nil)
	db, err := txn1.CreateDatabase(name, "", "")
	assert.Nil(t, err)
	t.Log(db.String())

	schema := catalog.MockSchema(1, 0)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn1.Commit(context.Background())
	assert.Nil(t, err)
	t.Log(db.String())
	assert.Equal(t, txn1.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).GetCreatedAtLocked())
	assert.True(t, db.GetMeta().(*catalog.DBEntry).IsCommittedLocked())
	assert.Equal(t, txn1.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).GetCreatedAtLocked())
	assert.True(t, rel.GetMeta().(*catalog.TableEntry).IsCommittedLocked())

	txn2, _ := mgr.StartTxn(nil)
	get, err := txn2.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(get.String())

	dropped, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(dropped.String())

	_, err = txn2.GetDatabase(name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadDB))
	t.Log(err)

	txn3, _ := mgr.StartTxn(nil)

	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	rel, err = db3.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(rel.String())
}

func TestTransaction3(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	pool, _ := ants.NewPool(20)
	defer pool.Release()

	var wg sync.WaitGroup

	flow := func(i int) func() {
		return func() {
			defer wg.Done()
			txn, _ := mgr.StartTxn(nil)
			name := fmt.Sprintf("db-%d", i)
			db, err := txn.CreateDatabase(name, "", "")
			assert.Nil(t, err)
			schema := catalog.MockSchemaAll(13, 12)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			err = txn.Commit(context.Background())
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		err := pool.Submit(flow(i))
		assert.Nil(t, err)
	}
	wg.Wait()
}

func TestObject1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1, _ := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1, 0)
	db, err := txn1.CreateDatabase(name, "", "")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateObject(false)
	assert.Nil(t, err)
	err = txn1.Commit(context.Background())
	assert.Nil(t, err)

	txn2, _ := mgr.StartTxn(nil)
	db, err = txn2.GetDatabase(name)
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	objIt := rel.MakeObjectIt(false)
	cnt := 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 1, cnt)

	_, err = rel.CreateObject(false)
	assert.Nil(t, err)

	objIt = rel.MakeObjectIt(false)
	cnt = 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 2, cnt)

	txn3, _ := mgr.StartTxn(nil)
	db, _ = txn3.GetDatabase(name)
	rel, _ = db.GetRelationByName(schema.Name)
	objIt = rel.MakeObjectIt(false)
	cnt = 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 1, cnt)

	err = txn2.Commit(context.Background())
	assert.Nil(t, err)

	objIt = rel.MakeObjectIt(false)
	cnt = 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 1, cnt)
}

func TestObject2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1, _ := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db", "", "")
	schema := catalog.MockSchema(1, 0)
	rel, _ := db.CreateRelation(schema)
	objCnt := 10
	for i := 0; i < objCnt; i++ {
		_, err := rel.CreateObject(false)
		assert.Nil(t, err)
	}

	it := rel.MakeObjectIt(false)
	cnt := 0
	for it.Next() {
		cnt++
		// iobj := it.GetObject()
	}
	assert.Equal(t, objCnt, cnt)
	// err := txn1.Commit()
	// assert.Nil(t, err)
	t.Log(c.SimplePPString(common.PPL1))
}

func TestDedup1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 4
	cnt := uint64(10)
	rows := uint64(schema.BlockMaxRows) / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db", "", "")
		_, err := db.CreateRelation(schema)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.NoError(t, err)
		err = rel.Append(context.Background(), bats[0])
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		assert.Nil(t, txn.Rollback(context.Background()))
	}

	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		assert.Nil(t, txn.Rollback(context.Background()))
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[1])
		assert.Nil(t, err)

		txn2, _ := mgr.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		err = rel2.Append(context.Background(), bats[2])
		assert.Nil(t, err)
		err = rel2.Append(context.Background(), bats[3])
		assert.Nil(t, err)
		assert.Nil(t, txn2.Commit(context.Background()))

		txn3, _ := mgr.StartTxn(nil)
		db3, _ := txn3.GetDatabase("db")
		rel3, _ := db3.GetRelationByName(schema.Name)
		err = rel3.Append(context.Background(), bats[4])
		assert.Nil(t, err)
		err = rel3.Append(context.Background(), bats[5])
		assert.Nil(t, err)
		assert.Nil(t, txn3.Commit(context.Background()))

		err = rel.Append(context.Background(), bats[3])
		assert.NoError(t, err)
		err = txn.Commit(context.Background())
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	}
	t.Log(c.SimplePPString(common.PPL1))
}
