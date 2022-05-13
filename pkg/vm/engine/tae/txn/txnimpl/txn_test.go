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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
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

func makeTable(t *testing.T, dir string, colCnt int, bufSize uint64) *txnTable {
	mgr := buffer.NewNodeManager(bufSize, nil)
	driver := wal.NewDriver(dir, "store", nil)
	id := common.NextGlobalSeqNum()
	schema := catalog.MockSchemaAll(colCnt)
	rel := mockTestRelation(id, schema)
	txn := txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
	store := newStore(nil, driver, mgr, nil)
	store.BindTxn(txn)
	return newTxnTable(store, rel)
}

func TestInsertNode(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	tbl := makeTable(t, dir, 2, common.K*6)
	defer tbl.store.driver.Close()
	tbl.GetSchema().PrimaryKey = 1
	bat := compute.MockBatch(tbl.GetSchema().Types(), common.K, int(tbl.GetSchema().PrimaryKey), nil)
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
				n := NewInsertNode(tbl, tbl.store.nodesMgr, cid, tbl.store.driver)
				nodes[i] = n
				h := tbl.store.nodesMgr.Pin(n)
				var err error
				if err = n.Expand(common.K*1, func() error {
					n.Append(bat, 0)
					return nil
				}); err != nil {
					err = n.Expand(common.K*1, func() error {
						n.Append(bat, 0)
						return nil
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
		p.Submit(worker(id))
	}
	wg.Wait()
	t.Log(all)
	t.Log(tbl.store.nodesMgr.String())
	t.Log(common.GPool.String())
}

func TestTable(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 2
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		rel, _ := db.CreateRelation(schema)
		bat := compute.MockBatch(schema.Types(), common.K*100, int(schema.PrimaryKey), nil)
		bats := compute.SplitBatch(bat, 100)
		for _, data := range bats {
			err := rel.Append(data)
			assert.Nil(t, err)
		}
		tbl, _ := txn.GetStore().(*txnStore).getOrSetTable(rel.ID())
		tbl.RangeDeleteLocalRows(1024+20, 1024+30)
		tbl.RangeDeleteLocalRows(1024*2+38, 1024*2+40)
		assert.True(t, tbl.IsLocalDeleted(1024+20))
		assert.True(t, tbl.IsLocalDeleted(1024+30))
		assert.False(t, tbl.IsLocalDeleted(1024+19))
		assert.False(t, tbl.IsLocalDeleted(1024+31))
		err := txn.Commit()
		assert.Nil(t, err)
	}
}

func TestUpdateUncommitted(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 1

	bat := compute.MockBatch(schema.Types(), 1000, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 2)

	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	for _, b := range bats {
		err := rel.Append(b)
		assert.Nil(t, err)
	}

	tbl, _ := txn.GetStore().(*txnStore).getOrSetTable(rel.ID())
	row := uint32(9)
	assert.False(t, tbl.IsLocalDeleted(row))
	rows := tbl.Rows()
	err := tbl.UpdateLocalValue(row, 0, 999)
	assert.Nil(t, err)
	assert.True(t, tbl.IsLocalDeleted(row))
	assert.Equal(t, rows+1, tbl.Rows())
}

func TestAppend(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 1

	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	table, _ := txn.GetStore().(*txnStore).getOrSetTable(rel.ID())
	tbl := table.(*txnTable)
	rows := uint64(txnbase.MaxNodeRows) / 8 * 3
	brows := rows / 3
	bat := compute.MockBatch(tbl.GetSchema().Types(), rows, int(tbl.GetSchema().PrimaryKey), nil)

	bats := compute.SplitBatch(bat, 3)

	err := tbl.BatchDedupLocal(bats[0])
	assert.Nil(t, err)
	err = tbl.Append(bats[0])
	assert.Nil(t, err)
	assert.Equal(t, int(brows), int(tbl.Rows()))
	assert.Equal(t, int(brows), int(tbl.index.Count()))

	err = tbl.BatchDedupLocal(bats[0])
	assert.NotNil(t, err)

	err = tbl.BatchDedupLocal(bats[1])
	assert.Nil(t, err)
	err = tbl.Append(bats[1])
	assert.Nil(t, err)
	assert.Equal(t, 2*int(brows), int(tbl.Rows()))
	assert.Equal(t, 2*int(brows), int(tbl.index.Count()))

	err = tbl.BatchDedupLocal(bats[2])
	assert.Nil(t, err)
	err = tbl.Append(bats[2])
	assert.Nil(t, err)
	assert.Equal(t, 3*int(brows), int(tbl.Rows()))
	assert.Equal(t, 3*int(brows), int(tbl.index.Count()))
}

func TestIndex(t *testing.T) {
	index := NewSimpleTableIndex()
	err := index.Insert(1, 10)
	assert.Nil(t, err)
	err = index.Insert("one", 10)
	assert.Nil(t, err)
	row, err := index.Find("one")
	assert.Nil(t, err)
	assert.Equal(t, 10, int(row))
	err = index.Delete("one")
	assert.Nil(t, err)
	_, err = index.Find("one")
	assert.NotNil(t, err)

	schema := catalog.MockSchemaAll(14)
	schema.PrimaryKey = 1
	bat := compute.MockBatch(schema.Types(), 500, int(schema.PrimaryKey), nil)

	idx := NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Vecs[0])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[0], 0, gvec.Length(bat.Vecs[0]), 0, false)
	assert.NotNil(t, err)

	err = idx.BatchDedup(bat.Vecs[1])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[1], 0, gvec.Length(bat.Vecs[1]), 0, false)
	assert.Nil(t, err)

	window := gvec.New(bat.Vecs[1].Typ)
	gvec.Window(bat.Vecs[1], 20, 22, window)
	assert.Equal(t, 2, gvec.Length(window))
	err = idx.BatchDedup(window)
	assert.NotNil(t, err)

	schema.PrimaryKey = 12
	bat = compute.MockBatch(schema.Types(), 500, int(schema.PrimaryKey), nil)
	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Vecs[12])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[12], 0, gvec.Length(bat.Vecs[12]), 0, false)
	assert.Nil(t, err)

	window = gvec.New(bat.Vecs[12].Typ)
	gvec.Window(bat.Vecs[12], 20, 22, window)
	assert.Equal(t, 2, gvec.Length(window))
	err = idx.BatchDedup(window)
	assert.NotNil(t, err)
}

func TestLoad(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 13

	bat := compute.MockBatch(schema.Types(), 60000, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 5)

	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	table, _ := txn.GetStore().(*txnStore).getOrSetTable(rel.ID())
	tbl := table.(*txnTable)

	err := tbl.Append(bats[0])
	assert.Nil(t, err)

	t.Log(tbl.store.nodesMgr.String())
	v, err := tbl.GetLocalValue(100, 0)
	assert.Nil(t, err)
	t.Log(tbl.store.nodesMgr.String())
	t.Logf("Row %d, Col %d, Val %v", 100, 0, v)
}

func TestNodeCommand(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 13

	bat := compute.MockBatch(schema.Types(), 15000, int(schema.PrimaryKey), nil)

	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)

	table, _ := txn.GetStore().(*txnStore).getOrSetTable(rel.ID())
	tbl := table.(*txnTable)
	err := tbl.Append(bat)
	assert.Nil(t, err)

	err = tbl.RangeDeleteLocalRows(100, 200)
	assert.Nil(t, err)

	for i, inode := range tbl.inodes {
		cmd, entry, err := inode.MakeCommand(uint32(i), false)
		assert.Nil(t, err)
		if i == 0 {
			assert.Equal(t, 2, len(cmd.(*AppendCmd).Cmds))
		} else {
			assert.Equal(t, 1, len(cmd.(*AppendCmd).Cmds))
		}
		if entry != nil {
			entry.WaitDone()
			entry.Free()
		}
		t.Log(cmd.String())
	}
}

func TestBuildCommand(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 13

	bat := compute.MockBatch(schema.Types(), 55000, int(schema.PrimaryKey), nil)
	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)

	table, _ := txn.GetStore().(*txnStore).getOrSetTable(rel.ID())
	tbl := table.(*txnTable)
	err := tbl.Append(bat)
	assert.Nil(t, err)

	err = tbl.RangeDeleteLocalRows(100, 200)
	assert.Nil(t, err)

	t.Log(tbl.store.nodesMgr.String())
	cmdSeq := uint32(1)
	cmd, entries, err := tbl.buildCommitCmd(&cmdSeq)
	assert.Nil(t, err)
	tbl.Close()
	assert.Equal(t, 0, tbl.store.nodesMgr.Count())
	t.Log(cmd.String())
	for _, e := range entries {
		e.WaitDone()
		e.Free()
	}
	t.Log(tbl.store.nodesMgr.String())
}

func TestApplyToColumn1(t *testing.T) {
	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	node.UpdateLocked(3, []byte("update"))
	deletes.AddRange(3, 4)

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_varchar
	col := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 0),
		Lengths: make([]uint32, 0),
	}
	for i := 0; i < 5; i++ {
		col.Offsets = append(col.Offsets, uint32(len(col.Data)))
		data := "val" + strconv.Itoa(i)
		col.Data = append(col.Data, []byte(data)...)
		col.Lengths = append(col.Lengths, uint32(len(data)))
	}
	vec.Col = col

	vec.Nsp = &nulls.Nulls{}
	vec.Nsp.Np = &roaring64.Bitmap{}
	vec.Nsp.Np.Add(2)
	// vec.Nsp.Np.Add(1)
	// vec.Nsp.Np.Add(3)
	vec.Nsp.Np.Add(4)
	// vec.Nsp.Np.Add(0)

	fmt.Printf("%s\n%v\n->\n", vec.Col, vec.Nsp.Np)
	res := node.ApplyToColumn(vec, deletes)
	fmt.Printf("%s\n%v\n", res.Col, res.Nsp.Np)
}

func TestApplyToColumn2(t *testing.T) {
	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	node.UpdateLocked(0, int32(8))
	deletes.AddRange(2, 4)

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_int32
	vec.Col = []int32{1, 2, 3, 4}

	vec.Nsp = &nulls.Nulls{}
	vec.Nsp.Np = &roaring64.Bitmap{}
	vec.Nsp.Np.Add(2)
	vec.Nsp.Np.Add(1)
	vec.Nsp.Np.Add(3)
	vec.Nsp.Np.Add(0)

	fmt.Printf("%v\n%v\n->\n", vec.Col, vec.Nsp.Np)
	res := node.ApplyToColumn(vec, deletes)
	fmt.Printf("%v\n%v\n", res.Col, res.Nsp.Np)
}

func TestApplyToColumn3(t *testing.T) {
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	node.UpdateLocked(3, []byte("update"))

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_varchar
	col := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 0),
		Lengths: make([]uint32, 0),
	}
	for i := 0; i < 5; i++ {
		col.Offsets = append(col.Offsets, uint32(len(col.Data)))
		data := "val" + strconv.Itoa(i)
		col.Data = append(col.Data, []byte(data)...)
		col.Lengths = append(col.Lengths, uint32(len(data)))
	}
	vec.Col = col

	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	fmt.Printf("%s\n->\n", vec.Col)
	res := node.ApplyToColumn(vec, deletes)
	fmt.Printf("%s\n", res.Col)
}

func TestApplyToColumn4(t *testing.T) {
	ts := common.NextGlobalSeqNum()
	chain := updates.MockColumnUpdateChain()
	node := updates.NewCommittedColumnNode(ts, ts, nil, nil)
	node.AttachTo(chain)
	node.UpdateLocked(3, int32(8))

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_int32
	vec.Col = []int32{1, 2, 3, 4}

	fmt.Printf("%v\n->\n", vec.Col)
	res := node.ApplyToColumn(vec, nil)
	fmt.Printf("%v\n", res.Col)
}

func TestTxnManager1(t *testing.T) {
	mgr := txnbase.NewTxnManager(TxnStoreFactory(nil, nil, nil, nil), TxnFactory(nil))
	mgr.Start()
	txn := mgr.StartTxn(nil)
	txn.MockIncWriteCnt()

	lock := sync.Mutex{}
	seqs := make([]int, 0)

	txn.SetPrepareCommitFn(func(i interface{}) error {
		time.Sleep(time.Millisecond * 100)
		lock.Lock()
		seqs = append(seqs, 2)
		lock.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	short := func() {
		defer wg.Done()
		txn2 := mgr.StartTxn(nil)
		txn2.MockIncWriteCnt()
		txn2.SetPrepareCommitFn(func(i interface{}) error {
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
		txn2.Commit()
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go short()
	}

	txn.Commit()
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
	factory := tables.NewDataFactory(mockio.SegmentFileMockFactory, mutBufMgr, nil)
	// factory := tables.NewDataFactory(dataio.SegmentFileMockFactory, mutBufMgr)
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
	// dir := initTestPath(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn1 := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn1.Commit()
	assert.Nil(t, err)

	txn2 := mgr.StartTxn(nil)
	db2, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(db2.String())

	txn3 := mgr.StartTxn(nil)
	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(db3.String())
	schema = catalog.MockSchema(1)
	rel, err := db3.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn2.Commit()
	assert.Nil(t, err)
	err = txn3.Commit()
	assert.Equal(t, txnif.TxnStateRollbacked, txn3.GetTxnState(true))
	t.Log(txn3.String())
	// assert.NotNil(t, err)
	t.Log(db2.String())
	t.Log(rel.String())
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTransaction2(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	name := "db"
	txn1 := mgr.StartTxn(nil)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	t.Log(db.String())

	schema := catalog.MockSchema(1)
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

	txn2 := mgr.StartTxn(nil)
	get, err := txn2.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(get.String())

	dropped, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(dropped.String())

	get, err = txn2.GetDatabase(name)
	assert.Equal(t, catalog.ErrNotFound, err)
	t.Log(err)

	txn3 := mgr.StartTxn(nil)

	err = txn3.UseDatabase(name)
	assert.Nil(t, err)
	err = txn3.UseDatabase("xx")
	assert.NotNil(t, err)

	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	rel, err = db3.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(rel.String())
}

func TestTransaction3(t *testing.T) {
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
			txn := mgr.StartTxn(nil)
			name := fmt.Sprintf("db-%d", i)
			db, err := txn.CreateDatabase(name)
			assert.Nil(t, err)
			schema := catalog.MockSchemaAll(13)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			err = txn.Commit()
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		pool.Submit(flow(i))
	}
	wg.Wait()
}

func TestSegment1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1 := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateSegment()
	assert.Nil(t, err)
	err = txn1.Commit()
	assert.Nil(t, err)

	txn2 := mgr.StartTxn(nil)
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

	txn3 := mgr.StartTxn(nil)
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
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1 := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db")
	schema := catalog.MockSchema(1)
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
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1 := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db")
	schema := catalog.MockSchema(1)
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
	txn2 := mgr.StartTxn(nil)
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
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(t, dir)
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
		err = rel2.Append(bats[2])
		assert.Nil(t, err)
		err = rel2.Append(bats[3])
		assert.Nil(t, err)
		assert.Nil(t, txn2.Commit())

		txn3 := mgr.StartTxn(nil)
		db3, _ := txn3.GetDatabase("db")
		rel3, _ := db3.GetRelationByName(schema.Name)
		err = rel3.Append(bats[4])
		assert.Nil(t, err)
		err = rel3.Append(bats[5])
		assert.Nil(t, err)
		assert.Nil(t, txn3.Commit())

		err = rel.Append(bats[3])
		assert.Nil(t, err)
		err = txn.Commit()
		t.Log(txn.String())
		assert.NotNil(t, err)
	}
	t.Log(c.SimplePPString(common.PPL1))
}
