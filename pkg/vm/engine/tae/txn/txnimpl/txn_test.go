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
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
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

func init() {
	rand.Seed(time.Now().UnixNano())
}

func getNodes() int {
	v := rand.Intn(100)
	if v < 30 {
		return 1 * 2
	} else if v < 55 {
		return 2 * 2
	} else if v < 75 {
		return 3 * 2
	} else if v < 90 {
		return 4 * 2
	}
	return 5 * 2
}

func makeTable(t *testing.T, dir string, colCnt int, pkIdx int, bufSize uint64) *txnTable {
	mgr := buffer.NewNodeManager(bufSize, nil)
	driver := wal.NewDriver(dir, "store", nil)
	id := common.NextGlobalSeqNum()
	schema := catalog.MockSchemaAll(colCnt, pkIdx)
	rel := mockTestRelation(id, schema)
	txn := txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
	store := newStore(nil, driver, mgr, nil)
	store.BindTxn(txn)
	return newTxnTable(store, rel.GetMeta().(*catalog.TableEntry))
}

func TestInsertNode(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	tbl := makeTable(t, dir, 2, 1, common.K*6)
	defer tbl.store.driver.Close()
	bat := catalog.MockBatch(tbl.GetSchema(), int(common.K))
	defer bat.Close()
	p, _ := ants.NewPool(5)

	var wg sync.WaitGroup
	var all uint64

	worker := func(id uint64) func() {
		return func() {
			defer wg.Done()
			cnt := getNodes()
			nodes := make([]*insertNode, cnt)
			for i := 0; i < cnt; i++ {
				var cid common.ID
				cid.BlockID = id
				cid.Idx = uint16(i)
				n := NewInsertNode(tbl, tbl.store.nodesMgr, &cid, tbl.store.driver)
				nodes[i] = n
				h := tbl.store.nodesMgr.Pin(n)
				var err error
				if err = n.Expand(common.K*1, func() error {
					_, err := n.Append(bat, 0)
					return err
				}); err != nil {
					err = n.Expand(common.K*1, func() error {
						_, err := n.Append(bat, 0)
						return err
					})
				}
				if err != nil {
					assert.NotNil(t, err)
				}
				h.Close()
			}
			for _, n := range nodes {
				// n.ToTransient()
				n.Close()
			}
			atomic.AddUint64(&all, uint64(len(nodes)))
		}
	}
	idAlloc := common.NewIdAlloctor(1)
	for {
		id := idAlloc.Alloc()
		if id > 10 {
			break
		}
		wg.Add(1)
		err := p.Submit(worker(id))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Log(all)
	t.Log(tbl.store.nodesMgr.String())
}

func TestTable(t *testing.T) {
	testutils.EnsureNoLeak(t)
	t.Log(stl.DefaultAllocator.String())
	t.Log(tables.ImmutMemAllocator.String())
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	{
		txn, _ := mgr.StartTxn(nil)
		db, err := txn.CreateDatabase("db")
		assert.Nil(t, err)
		rel, _ := db.CreateRelation(schema)
		bat := catalog.MockBatch(schema, int(common.K)*100)
		defer bat.Close()
		bats := bat.Split(100)
		for _, data := range bats {
			err := rel.Append(data)
			assert.Nil(t, err)
		}
		tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
		tbl, _ := tDB.getOrSetTable(rel.ID())
		err = tbl.RangeDeleteLocalRows(1024+20, 1024+30)
		assert.Nil(t, err)
		err = tbl.RangeDeleteLocalRows(1024*2+38, 1024*2+40)
		assert.Nil(t, err)
		assert.True(t, tbl.IsLocalDeleted(1024+20))
		assert.True(t, tbl.IsLocalDeleted(1024+30))
		assert.False(t, tbl.IsLocalDeleted(1024+19))
		assert.False(t, tbl.IsLocalDeleted(1024+31))
		err = txn.Commit()
		assert.Nil(t, err)
	}
}

func TestUpdateUncommitted(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10

	bat := catalog.MockBatch(schema, 1000)
	defer bat.Close()
	bats := bat.Split(2)

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	for _, b := range bats {
		err := rel.Append(b)
		assert.Nil(t, err)
	}

	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())
	row := uint32(9)
	assert.False(t, tbl.IsLocalDeleted(row))
	rows := tbl.UncommittedRows()
	err := tbl.UpdateLocalValue(row, 1, int16(1999))
	assert.Nil(t, err)
	assert.True(t, tbl.IsLocalDeleted(row))
	assert.Equal(t, rows+1, tbl.UncommittedRows())
	assert.NoError(t, txn.Commit())
}

func TestAppend(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())
	rows := uint64(txnbase.MaxNodeRows) / 8 * 3
	brows := rows / 3

	bat := catalog.MockBatch(tbl.GetSchema(), int(rows))
	defer bat.Close()
	bats := bat.Split(3)

	err := tbl.BatchDedupLocal(bats[0])
	assert.Nil(t, err)
	err = tbl.Append(bats[0])
	assert.Nil(t, err)
	assert.Equal(t, int(brows), int(tbl.UncommittedRows()))
	assert.Equal(t, int(brows), int(tbl.localSegment.index.Count()))

	err = tbl.BatchDedupLocal(bats[0])
	assert.NotNil(t, err)

	err = tbl.BatchDedupLocal(bats[1])
	assert.Nil(t, err)
	err = tbl.Append(bats[1])
	assert.Nil(t, err)
	assert.Equal(t, 2*int(brows), int(tbl.UncommittedRows()))
	assert.Equal(t, 2*int(brows), int(tbl.localSegment.index.Count()))

	err = tbl.BatchDedupLocal(bats[2])
	assert.Nil(t, err)
	err = tbl.Append(bats[2])
	assert.Nil(t, err)
	assert.Equal(t, 3*int(brows), int(tbl.UncommittedRows()))
	assert.Equal(t, 3*int(brows), int(tbl.localSegment.index.Count()))
	assert.NoError(t, txn.Commit())
}

func TestIndex(t *testing.T) {
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
	err = idx.BatchDedup(bat.Vecs[0])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[0], 0, bat.Vecs[0].Length(), 0, false)
	assert.NotNil(t, err)

	err = idx.BatchDedup(bat.Vecs[1])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[1], 0, bat.Vecs[1].Length(), 0, false)
	assert.Nil(t, err)

	window := bat.Vecs[1].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(window)
	assert.NotNil(t, err)

	schema = catalog.MockSchemaAll(14, 12)
	bat = catalog.MockBatch(schema, 500)
	defer bat.Close()
	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Vecs[12])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[12], 0, bat.Vecs[12].Length(), 0, false)
	assert.Nil(t, err)

	window = bat.Vecs[12].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(window)
	assert.Error(t, err)
}

func TestLoad(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14, 13)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10

	bat := catalog.MockBatch(schema, 60000)
	defer bat.Close()
	bats := bat.Split(5)

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())

	err := tbl.Append(bats[0])
	assert.NoError(t, err)

	t.Log(tbl.store.nodesMgr.String())
	v, err := tbl.GetLocalValue(100, 0)
	assert.NoError(t, err)
	t.Log(tbl.store.nodesMgr.String())
	t.Logf("Row %d, Col %d, Val %v", 100, 0, v)
	assert.NoError(t, txn.Commit())
}

func TestNodeCommand(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14, 13)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10

	bat := catalog.MockBatch(schema, 15000)
	defer bat.Close()

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)

	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())
	err := tbl.Append(bat)
	assert.Nil(t, err)

	err = tbl.RangeDeleteLocalRows(100, 200)
	assert.NoError(t, err)

	for i, inode := range tbl.localSegment.nodes {
		cmd, entry, err := inode.MakeCommand(uint32(i), false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cmd.(*AppendCmd).Cmds))
		if entry != nil {
			_ = entry.WaitDone()
			entry.Free()
		}
		if cmd != nil {
			t.Log(cmd.String())
		}
	}
	assert.NoError(t, txn.Commit())
}

func TestApplyToColumn1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	deletes := roaring.BitmapOf(1)
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnUpdateNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	err := node.UpdateLocked(3, []byte("update"))
	assert.Nil(t, err)
	deletes.AddRange(3, 4)

	vec := containers.MakeVector(types.Type_VARCHAR.ToType(), true)
	defer vec.Close()
	for i := 0; i < 5; i++ {
		data := "val" + strconv.Itoa(i)
		vec.Append([]byte(data))
	}
	vec.Update(2, types.Null{})
	vec.Update(4, types.Null{})

	// Nulls {2, 4}
	// Deletes {1,3}
	// Length 5
	assert.True(t, types.IsNull(vec.Get(2)))
	assert.True(t, types.IsNull(vec.Get(4)))
	node.ApplyToColumn(vec, deletes)
	assert.Equal(t, 3, vec.Length())
	assert.True(t, types.IsNull(vec.Get(1)))
	assert.True(t, types.IsNull(vec.Get(2)))
	assert.Equal(t, uint64(2), vec.NullMask().GetCardinality())
	t.Log(vec.String())
}

func TestApplyToColumn2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	deletes := roaring.BitmapOf(1)
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnUpdateNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	err := node.UpdateLocked(0, int32(8))
	assert.Nil(t, err)
	deletes.AddRange(2, 4)

	vec := containers.MakeVector(types.Type_INT32.ToType(), true)
	defer vec.Close()
	vec.AppendMany(int32(1), int32(2), int32(3), int32(4))

	vec.Update(2, types.Null{})
	vec.Update(1, types.Null{})
	vec.Update(3, types.Null{})
	vec.Update(0, types.Null{})

	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, uint64(4), vec.NullMask().GetCardinality())
	// deletes: {1,2,3}
	node.ApplyToColumn(vec, deletes)
	assert.Equal(t, 1, vec.Length())
	assert.False(t, vec.HasNull())
	assert.Equal(t, any(int32(8)), vec.Get(0))
}

func TestApplyToColumn3(t *testing.T) {
	testutils.EnsureNoLeak(t)
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnUpdateNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	err := node.UpdateLocked(3, []byte("update"))
	assert.Nil(t, err)

	vec := containers.MakeVector(types.Type_VARCHAR.ToType(), true)
	defer vec.Close()
	for i := 0; i < 5; i++ {
		data := "val" + strconv.Itoa(i)
		vec.Append([]byte(data))
	}

	deletes := roaring.BitmapOf(1)

	assert.Equal(t, 5, vec.Length())
	t.Log(vec.String())
	node.ApplyToColumn(vec, deletes)
	t.Log(vec.String())

	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, "update", string(vec.Get(2).([]byte)))
}

func TestApplyToColumn4(t *testing.T) {
	testutils.EnsureNoLeak(t)
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnUpdateNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	err := node.UpdateLocked(3, int32(8))
	assert.Nil(t, err)

	vec := containers.MakeVector(types.Type_INT32.ToType(), true)
	defer vec.Close()
	vec.AppendMany(int32(1), int32(2), int32(3), int32(4))

	t.Log(vec.String())
	node.ApplyToColumn(vec, nil)
	t.Log(vec.String())
	assert.Equal(t, any(int32(8)), vec.Get(3))
}

func TestTxnManager1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	mgr := txnbase.NewTxnManager(TxnStoreFactory(nil, nil, nil, nil), TxnFactory(nil))
	mgr.Start()
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
		err := txn2.Commit()
		assert.Nil(t, err)
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go short()
	}

	err := txn.Commit()
	assert.Nil(t, err)
	wg.Wait()
	defer mgr.Stop()
	expected := []int{1, 2, 3, 4}
	assert.Equal(t, expected, seqs)
}

func initTestContext(t *testing.T, dir string) (*catalog.Catalog, *txnbase.TxnManager, wal.Driver) {
	mockio.ResetFS()
	c := catalog.MockCatalog(dir, "mock", nil, nil)
	driver := wal.NewDriver(dir, "store", nil)
	txnBufMgr := buffer.NewNodeManager(common.G, nil)
	mutBufMgr := buffer.NewNodeManager(common.G, nil)
	factory := tables.NewDataFactory(mockio.SegmentFactory, mutBufMgr, nil, dir)
	mgr := txnbase.NewTxnManager(TxnStoreFactory(c, driver, txnBufMgr, factory), TxnFactory(c))
	mgr.Start()
	return c, mgr, driver
}

// 1. Txn1 create database "db" and table "tb1". Commit
// 2. Txn2 drop database
// 3. Txn3 create table "tb2"
// 4. Txn2 commit
// 5. Txn3 commit
func TestTransaction1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn1, _ := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1, 0)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn1.Commit()
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

	err = txn2.Commit()
	assert.Nil(t, err)
	err = txn3.Commit()
	assert.NotNil(t, err)
	assert.Equal(t, txnif.TxnStateRollbacked, txn3.GetTxnState(true))
	t.Log(txn3.String())
	// assert.NotNil(t, err)
	t.Log(db2.String())
	t.Log(rel.String())
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTransaction2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	name := "db"
	txn1, _ := mgr.StartTxn(nil)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	t.Log(db.String())

	schema := catalog.MockSchema(1, 0)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn1.Commit()
	assert.Nil(t, err)
	t.Log(db.String())
	assert.Equal(t, txn1.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).CreateAt)
	assert.Nil(t, db.GetMeta().(*catalog.DBEntry).Txn)
	assert.Equal(t, txn1.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).CreateAt)
	assert.Nil(t, rel.GetMeta().(*catalog.TableEntry).Txn)

	txn2, _ := mgr.StartTxn(nil)
	get, err := txn2.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(get.String())

	dropped, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(dropped.String())

	_, err = txn2.GetDatabase(name)
	assert.Equal(t, catalog.ErrNotFound, err)
	t.Log(err)

	txn3, _ := mgr.StartTxn(nil)

	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	rel, err = db3.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(rel.String())
}

func TestTransaction3(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	pool, _ := ants.NewPool(20)

	var wg sync.WaitGroup

	flow := func(i int) func() {
		return func() {
			defer wg.Done()
			txn, _ := mgr.StartTxn(nil)
			name := fmt.Sprintf("db-%d", i)
			db, err := txn.CreateDatabase(name)
			assert.Nil(t, err)
			schema := catalog.MockSchemaAll(13, 12)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			err = txn.Commit()
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

func TestSegment1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1, _ := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1, 0)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateSegment()
	assert.Nil(t, err)
	err = txn1.Commit()
	assert.Nil(t, err)

	txn2, _ := mgr.StartTxn(nil)
	db, err = txn2.GetDatabase(name)
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	segIt := rel.MakeSegmentIt()
	cnt := 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 1, cnt)

	_, err = rel.CreateSegment()
	assert.Nil(t, err)

	segIt = rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 2, cnt)

	txn3, _ := mgr.StartTxn(nil)
	db, _ = txn3.GetDatabase(name)
	rel, _ = db.GetRelationByName(schema.Name)
	segIt = rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 1, cnt)

	err = txn2.Commit()
	assert.Nil(t, err)

	segIt = rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 1, cnt)
}

func TestSegment2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1, _ := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db")
	schema := catalog.MockSchema(1, 0)
	rel, _ := db.CreateRelation(schema)
	segCnt := 10
	for i := 0; i < segCnt; i++ {
		_, err := rel.CreateSegment()
		assert.Nil(t, err)
	}

	it := rel.MakeSegmentIt()
	cnt := 0
	for it.Valid() {
		cnt++
		// iseg := it.GetSegment()
		it.Next()
	}
	assert.Equal(t, segCnt, cnt)
	// err := txn1.Commit()
	// assert.Nil(t, err)
	t.Log(c.SimplePPString(common.PPL1))
}

func TestBlock1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1, _ := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db")
	schema := catalog.MockSchema(1, 0)
	rel, _ := db.CreateRelation(schema)
	seg, _ := rel.CreateSegment()

	blkCnt := 100
	for i := 0; i < blkCnt; i++ {
		_, err := seg.CreateBlock()
		assert.Nil(t, err)
	}

	it := seg.MakeBlockIt()
	cnt := 0
	for it.Valid() {
		cnt++
		it.Next()
	}
	assert.Equal(t, blkCnt, cnt)

	err := txn1.Commit()
	assert.Nil(t, err)
	txn2, _ := mgr.StartTxn(nil)
	db, _ = txn2.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	segIt := rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		seg = segIt.GetSegment()
		it = seg.MakeBlockIt()
		for it.Valid() {
			cnt++
			it.Next()
		}
		segIt.Next()
	}
	assert.Equal(t, blkCnt, cnt)
}

func TestDedup1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	cnt := uint64(10)
	rows := uint64(schema.BlockMaxRows) / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		_, err := db.CreateRelation(schema)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.NoError(t, err)
		err = rel.Append(bats[0])
		assert.ErrorIs(t, err, data.ErrDuplicate)
		assert.Nil(t, txn.Rollback())
	}

	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.ErrorIs(t, err, data.ErrDuplicate)
		assert.Nil(t, txn.Rollback())
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(bats[1])
		assert.Nil(t, err)

		txn2, _ := mgr.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		err = rel2.Append(bats[2])
		assert.Nil(t, err)
		err = rel2.Append(bats[3])
		assert.Nil(t, err)
		assert.Nil(t, txn2.Commit())

		txn3, _ := mgr.StartTxn(nil)
		db3, _ := txn3.GetDatabase("db")
		rel3, _ := db3.GetRelationByName(schema.Name)
		err = rel3.Append(bats[4])
		assert.Nil(t, err)
		err = rel3.Append(bats[5])
		assert.Nil(t, err)
		assert.Nil(t, txn3.Commit())

		err = rel.Append(bats[3])
		// TODO: should be w-w error
		assert.Error(t, err)
		_ = txn.Rollback()
	}
	t.Log(c.SimplePPString(common.PPL1))
}
