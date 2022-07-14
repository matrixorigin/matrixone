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

package catalog

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAECATALOG"
)

func TestCompoundPKSchema(t *testing.T) {
	schema := NewEmptySchema(t.Name())
	err := schema.AppendPKCol("pk1", types.Type_INT32.ToType(), 1)
	assert.NoError(t, err)
	err = schema.AppendPKCol("pk0", types.Type_INT32.ToType(), 0)
	assert.NoError(t, err)
	err = schema.AppendPKCol("pk2", types.Type_INT32.ToType(), 2)
	assert.NoError(t, err)
	err = schema.Finalize(false)
	assert.NoError(t, err)
	assert.Equal(t, 3, schema.SortKey.Size())
	assert.Equal(t, int8(0), schema.SortKey.GetDef(0).SortIdx)
	assert.Equal(t, int8(1), schema.SortKey.GetDef(1).SortIdx)
	assert.Equal(t, int8(2), schema.SortKey.GetDef(2).SortIdx)
	assert.Equal(t, "pk0", schema.SortKey.GetDef(0).Name)
	assert.Equal(t, "pk1", schema.SortKey.GetDef(1).Name)
	assert.Equal(t, "pk2", schema.SortKey.GetDef(2).Name)

	schema = NewEmptySchema(t.Name())
	err = schema.AppendPKCol("pk1", types.Type_INT32.ToType(), 0)
	assert.NoError(t, err)
	err = schema.AppendPKCol("pk0", types.Type_INT32.ToType(), 0)
	assert.NoError(t, err)
	err = schema.Finalize(false)
	assert.ErrorIs(t, err, ErrSchemaValidation)
}

func TestCreateDB1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	catalog := MockCatalog(dir, "mock", nil, nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1, _ := txnMgr.StartTxn(nil)

	name := fmt.Sprintf("%s-%d", t.Name(), 1)
	db1, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	t.Log(db1.String())

	assert.Equal(t, 2, len(catalog.entries))
	cnt := 0
	catalog.link.Loop(func(n *common.DLNode) bool {
		t.Log(n.GetPayload().(*DBEntry).GetID())
		cnt++
		return true
	}, true)
	assert.Equal(t, 2, cnt)

	_, err = txn1.CreateDatabase(name)
	assert.Equal(t, ErrDuplicate, err)

	txn2, _ := txnMgr.StartTxn(nil)

	_, err = txn2.CreateDatabase(name)
	assert.Equal(t, txnif.ErrTxnWWConflict, err)

	_, err = txn1.GetDatabase(name)
	assert.Nil(t, err)

	err = txn1.Commit()
	assert.Nil(t, err)

	assert.Nil(t, err)
	// assert.False(t, db1.(*mcokDBHandle).entry.IsCommitting())

	_, err = txn2.CreateDatabase(name)
	assert.Equal(t, ErrDuplicate, err)

	_, err = txn2.DropDatabase(name)
	assert.Equal(t, ErrNotFound, err)

	txn3, _ := txnMgr.StartTxn(nil)
	_, err = txn3.DropDatabase(name)
	assert.Nil(t, err)
	// assert.True(t, db1.(*mcokDBHandle).entry.IsDroppedUncommitted())

	_, err = txn3.CreateDatabase(name)
	assert.Nil(t, err)

	cnt = 0
	catalog.link.Loop(func(n *common.DLNode) bool {
		// t.Log(n.payload.(*DBEntry).String())
		cnt++
		return true
	}, true)
	assert.Equal(t, 3, cnt)

	txn4, _ := txnMgr.StartTxn(nil)

	h, err := txn4.GetDatabase(name)
	assert.Nil(t, err)
	assert.NotNil(t, h)
	// assert.Equal(t, db1.(*mcokDBHandle).entry, h.(*mcokDBHandle).entry)
}

//
// TXN1-S     TXN2-S      TXN1-C  TXN3-S TXN4-S  TXN3-C TXN5-S
//  |            |           |      |      |       |      |                                Time
// -+-+---+---+--+--+----+---+--+---+-+----+-+-----+------+-+------------------------------------>
//    |   |   |     |    |      |     |      |              |
//    |   |   |     |    |      |     |      |            [TXN5]: GET TBL [NOTFOUND]
//    |   |   |     |    |      |     |    [TXN4]: GET TBL [OK] | DROP DB1-TB1 [W-W]
//    |   |   |     |    |      |   [TXN3]: GET TBL [OK] | DROP DB1-TB1 [OK] | GET TBL [NOT FOUND]
//    |   |   |     |    |    [TXN2]: DROP DB [NOTFOUND]
//    |   |   |     |  [TXN2]: DROP DB [NOTFOUND]
//    |   |   |   [TXN2]:  GET DB [NOTFOUND] | CREATE DB [W-W]
//    |   | [TXN1]: CREATE DB1-TB1 [DUP]
//    | [TXN1]: CREATE DB1-TB1 [OK] | GET TBL [OK]
//  [TXN1]: CREATE DB1 [OK] | GET DB [OK]
func TestTableEntry1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	catalog := MockCatalog(dir, "mock", nil, nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1, _ := txnMgr.StartTxn(nil)
	name := "db1"
	db1, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	t.Log(db1.String())

	schema := MockSchema(2, 0)
	schema.Name = "tb1"
	tb1, err := db1.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(tb1.String())

	_, err = db1.GetRelationByName(schema.Name)
	assert.Nil(t, err)

	_, err = db1.CreateRelation(schema)
	assert.Equal(t, ErrDuplicate, err)

	txn2, _ := txnMgr.StartTxn(nil)
	_, err = txn2.GetDatabase(schema.Name)
	assert.Equal(t, err, ErrNotFound)

	_, err = txn2.CreateDatabase(name)
	assert.Equal(t, err, txnif.ErrTxnWWConflict)

	_, err = txn2.DropDatabase(name)
	assert.Equal(t, err, ErrNotFound)

	err = txn1.Commit()
	assert.Nil(t, err)

	_, err = txn2.DropDatabase(name)
	assert.Equal(t, err, ErrNotFound)

	txn3, _ := txnMgr.StartTxn(nil)
	db, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	_, err = db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(tb1.String())

	_, err = db.GetRelationByName(schema.Name)
	assert.Equal(t, ErrNotFound, err)

	txn4, _ := txnMgr.StartTxn(nil)
	db, err = txn4.GetDatabase(name)
	assert.Nil(t, err)
	_, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)

	_, err = db.DropRelationByName(schema.Name)
	assert.Equal(t, txnif.ErrTxnWWConflict, err)

	err = txn3.Commit()
	assert.Nil(t, err)

	t.Log(tb1.String())

	txn5, _ := txnMgr.StartTxn(nil)
	db, err = txn5.GetDatabase(name)
	assert.Nil(t, err)
	_, err = db.GetRelationByName(schema.Name)
	assert.Equal(t, ErrNotFound, err)
}

func TestTableEntry2(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	catalog := MockCatalog(dir, "mock", nil, nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1, _ := txnMgr.StartTxn(nil)
	name := "db1"
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	schema := MockSchema(2, 0)
	schema.Name = "tb1"
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)

	for i := 0; i < 1000; i++ {
		s := MockSchema(1, 0)
		s.Name = fmt.Sprintf("xx%d", i)
		_, err = db.CreateRelation(s)
		assert.Nil(t, err)
	}
	err = txn1.Commit()
	assert.Nil(t, err)

	txn2, _ := txnMgr.StartTxn(nil)
	db, err = txn2.GetDatabase(name)
	assert.Nil(t, err)
	rel, err := db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(rel.String())
	db, err = txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(db.String())

	var wg sync.WaitGroup
	txns := []txnif.AsyncTxn{txn2}
	for i := 0; i < 10; i++ {
		txn, _ := txnMgr.StartTxn(nil)
		txns = append(txns, txn)
	}
	now := time.Now()
	for _, txn := range txns {
		wg.Add(1)
		go func(ttxn txnif.AsyncTxn) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				database, err := ttxn.GetDatabase(name)
				if err != nil {
					// t.Logf("db-ttxn=%d, %s", ttxn.GetID(), err)
				} else {
					// t.Logf("db-ttxn=%d, %v", ttxn.GetID(), err)
					_, err := database.GetRelationByName(schema.Name)
					assert.NoError(t, err)
				}
			}
		}(txn)
	}
	wg.Wait()
	t.Log(time.Since(now))
}

func TestDB1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	catalog := MockCatalog(dir, "mock", nil, nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()
	name := "db1"
	var wg sync.WaitGroup
	flow := func() {
		defer wg.Done()
		txn, _ := txnMgr.StartTxn(nil)
		_, err := txn.GetDatabase(name)
		if err == ErrNotFound {
			_, err = txn.CreateDatabase(name)
			if err != nil {
				return
			}
		} else {
			_, err = txn.DropDatabase(name)
			if err != nil {
				return
			}
		}
		err = txn.Commit()
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go flow()
	}
	wg.Wait()
}

func TestTable1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	catalog := MockCatalog(dir, "mock", nil, nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()
	name := "db1"
	tbName := "tb1"
	var wg sync.WaitGroup
	flow := func() {
		defer wg.Done()
		txn, _ := txnMgr.StartTxn(nil)
		db, err := txn.GetDatabase(name)
		assert.Nil(t, err)
		_, err = db.GetRelationByName(tbName)
		if err == ErrNotFound {
			schema := MockSchema(1, 0)
			schema.Name = tbName
			if _, err = db.CreateRelation(schema); err != nil {
				return
			}
		} else {
			if _, err = db.DropRelationByName(tbName); err != nil {
				return
			}
		}
		err = txn.Commit()
		assert.Nil(t, err)
		// t.Log(rel.String())
	}
	{
		txn, _ := txnMgr.StartTxn(nil)
		_, err := txn.CreateDatabase(name)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
	}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go flow()
	}
	wg.Wait()
}

func TestCommand(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	catalog := MockCatalog(dir, "mock", nil, nil)
	defer catalog.Close()
	name := "db"

	db := NewDBEntry(catalog, name, nil)
	db.CreateAt = common.NextGlobalSeqNum()
	db.CurrOp = OpCreate
	db.ID = uint64(99)

	cdb, err := db.MakeCommand(0)
	assert.Nil(t, err)

	var w bytes.Buffer
	_, err = cdb.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	t.Log(cmd.GetType())
	eCmd := cmd.(*EntryCommand)
	assert.Equal(t, db.CreateAt, eCmd.DB.CreateAt)
	assert.Equal(t, db.name, eCmd.DB.name)
	assert.Equal(t, db.ID, eCmd.entry.ID)

	db.CurrOp = OpSoftDelete
	db.DeleteAt = common.NextGlobalSeqNum()

	cdb, err = db.MakeCommand(1)
	assert.Nil(t, err)

	w.Reset()
	_, err = cdb.WriteTo(&w)
	assert.Nil(t, err)

	buf = w.Bytes()
	r = bytes.NewBuffer(buf)

	cmd, _, err = txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)

	eCmd = cmd.(*EntryCommand)
	assert.Equal(t, db.DeleteAt, eCmd.entry.DeleteAt)
	assert.Equal(t, db.ID, eCmd.entry.ID)

	schema := MockSchemaAll(13, 0)
	tb := NewTableEntry(db, schema, nil, nil)
	tb.CreateAt = common.NextGlobalSeqNum()
	tb.ID = common.NextGlobalSeqNum()

	w.Reset()
	cmd, err = tb.MakeCommand(2)
	assert.Nil(t, err)

	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf = w.Bytes()
	r = bytes.NewBuffer(buf)

	cmd, _, err = txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	eCmd = cmd.(*EntryCommand)
	assert.Equal(t, tb.ID, eCmd.Table.ID)
	assert.Equal(t, tb.CreateAt, eCmd.Table.CreateAt)
	assert.Equal(t, tb.GetSchema().Name, eCmd.Table.GetSchema().Name)
	assert.Equal(t, tb.db.ID, eCmd.DBID)

	tb.DeleteAt = common.NextGlobalSeqNum()
	tb.CurrOp = OpSoftDelete

	cmd, err = tb.MakeCommand(3)
	assert.Nil(t, err)

	w.Reset()
	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf = w.Bytes()
	r = bytes.NewBuffer(buf)

	cmd, _, err = txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	eCmd = cmd.(*EntryCommand)
	assert.Equal(t, tb.ID, eCmd.entry.ID)
	assert.Equal(t, tb.DeleteAt, eCmd.entry.DeleteAt)
	assert.Equal(t, tb.db.ID, eCmd.DBID)
}

// UT Steps
// 1. Start Txn1, create a database "db", table "tb" and segment "seg1", then commit Txn1
// 1. Start Txn2, create a segment "seg2". Txn2 scan "tb" and "seg1, seg2" found
// 2. Start Txn3, scan "tb" and only "seg1" found
// 3. Commit Txn2
// 4. Txn3 scan "tb" and also only "seg1" found
// 5. Start Txn4, scan "tb" and both "seg1" and "seg2" found
func TestSegment1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	catalog := MockCatalog(dir, "mock", nil, nil)
	defer catalog.Close()
	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()
	name := "db"
	tbName := "tb"
	txn1, _ := txnMgr.StartTxn(nil)
	db, err := catalog.CreateDBEntry(name, txn1)
	assert.Nil(t, err)
	schema := MockSchema(1, 0)
	schema.Name = tbName
	tb, err := db.CreateTableEntry(schema, txn1, nil)
	assert.Nil(t, err)
	seg1, err := tb.CreateSegment(txn1, ES_Appendable, nil)
	assert.Nil(t, err)
	err = txn1.Commit()
	assert.Nil(t, err)
	t.Log(seg1.String())
	t.Log(tb.String())
}
