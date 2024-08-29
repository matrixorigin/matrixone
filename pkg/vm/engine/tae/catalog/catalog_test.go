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
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "TAECATALOG"
)

func TestCreateDB1(t *testing.T) {
	defer testutils.AfterTest(t)()
	catalog := MockCatalog()
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn1, _ := txnMgr.StartTxn(nil)

	name := fmt.Sprintf("%s-%d", t.Name(), 1)
	db1, err := txn1.CreateDatabase(name, "", "")
	assert.Nil(t, err)
	t.Log(db1.String())

	assert.Equal(t, 2, len(catalog.entries))
	cnt := 0
	catalog.link.Loop(func(n *common.GenericDLNode[*DBEntry]) bool {
		t.Log(n.GetPayload().ID)
		cnt++
		return true
	}, true)
	assert.Equal(t, 2, cnt)

	_, err = txn1.CreateDatabase(name, "", "")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedDup))

	txn2, _ := txnMgr.StartTxn(nil)

	_, err = txn2.CreateDatabase(name, "", "")
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))

	_, err = txn1.GetDatabase(name)
	assert.Nil(t, err)

	err = txn1.Commit(context.Background())
	assert.Nil(t, err)

	assert.Nil(t, err)
	// assert.False(t, db1.(*mcokDBHandle).entry.IsCommitting())

	_, err = txn2.CreateDatabase(name, "", "")
	assert.NotNil(t, err)

	_, err = txn2.DropDatabase(name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))

	txn3, _ := txnMgr.StartTxn(nil)
	_, err = txn3.DropDatabase(name)
	assert.Nil(t, err)
	// assert.True(t, db1.(*mcokDBHandle).entry.IsDroppedUncommitted())

	_, err = txn3.CreateDatabase(name, "", "")
	assert.Nil(t, err)

	cnt = 0
	catalog.link.Loop(func(n *common.GenericDLNode[*DBEntry]) bool {
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

// TXN1-S     TXN2-S      TXN1-C  TXN3-S TXN4-S  TXN3-C TXN5-S
//
//	|            |           |      |      |       |      |                                Time
//
// -+-+---+---+--+--+----+---+--+---+-+----+-+-----+------+-+------------------------------------>
//
//	  |   |   |     |    |      |     |      |              |
//	  |   |   |     |    |      |     |      |            [TXN5]: GET TBL [NOTFOUND]
//	  |   |   |     |    |      |     |    [TXN4]: GET TBL [OK] | DROP DB1-TB1 [W-W]
//	  |   |   |     |    |      |   [TXN3]: GET TBL [OK] | DROP DB1-TB1 [OK] | GET TBL [NOT FOUND]
//	  |   |   |     |    |    [TXN2]: DROP DB [NOTFOUND]
//	  |   |   |     |  [TXN2]: DROP DB [NOTFOUND]
//	  |   |   |   [TXN2]:  GET DB [NOTFOUND] | CREATE DB [W-W]
//	  |   | [TXN1]: CREATE DB1-TB1 [DUP]
//	  | [TXN1]: CREATE DB1-TB1 [OK] | GET TBL [OK]
//	[TXN1]: CREATE DB1 [OK] | GET DB [OK]
func TestTableEntry1(t *testing.T) {
	defer testutils.AfterTest(t)()
	catalog := MockCatalog()
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn1, _ := txnMgr.StartTxn(nil)
	name := "db1"
	db1, err := txn1.CreateDatabase(name, "", "")
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
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedDup))

	txn2, _ := txnMgr.StartTxn(nil)
	_, err = txn2.GetDatabase(schema.Name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadDB))

	_, err = txn2.CreateDatabase(name, "", "")
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))

	_, err = txn2.DropDatabase(name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))

	err = txn1.Commit(context.Background())
	assert.Nil(t, err)

	_, err = txn2.DropDatabase(name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))

	txn3, _ := txnMgr.StartTxn(nil)
	db, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	_, err = db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(tb1.String())

	_, err = db.GetRelationByName(schema.Name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))

	txn4, _ := txnMgr.StartTxn(nil)
	db, err = txn4.GetDatabase(name)
	assert.Nil(t, err)
	_, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)

	_, err = db.DropRelationByName(schema.Name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))

	err = txn3.Commit(context.Background())
	assert.Nil(t, err)

	t.Log(tb1.String())

	txn5, _ := txnMgr.StartTxn(nil)
	db, err = txn5.GetDatabase(name)
	assert.Nil(t, err)
	_, err = db.GetRelationByName(schema.Name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
}

func TestTableEntry2(t *testing.T) {
	defer testutils.AfterTest(t)()
	catalog := MockCatalog()
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn1, _ := txnMgr.StartTxn(nil)
	name := "db1"
	db, err := txn1.CreateDatabase(name, "", "")
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
	err = txn1.Commit(context.Background())
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
	defer testutils.AfterTest(t)()
	catalog := MockCatalog()
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	name := "db1"
	var wg sync.WaitGroup
	flow := func() {
		defer wg.Done()
		txn, _ := txnMgr.StartTxn(nil)
		_, err := txn.GetDatabase(name)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			_, err = txn.CreateDatabase(name, "", "")
			if err != nil {
				return
			}
		} else {
			_, err = txn.DropDatabase(name)
			if err != nil {
				return
			}
		}
		err = txn.Commit(context.Background())
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go flow()
	}
	wg.Wait()
}

func TestTable1(t *testing.T) {
	defer testutils.AfterTest(t)()
	catalog := MockCatalog()
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
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
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
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
		err = txn.Commit(context.Background())
		assert.Nil(t, err)
		// t.Log(rel.String())
	}
	{
		txn, _ := txnMgr.StartTxn(nil)
		_, err := txn.CreateDatabase(name, "", "")
		assert.Nil(t, err)
		err = txn.Commit(context.Background())
		assert.Nil(t, err)
	}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go flow()
	}
	wg.Wait()
}

// UT Steps
// 1. Start Txn1, create a database "db", table "tb" and Object "obj1", then commit Txn1
// 1. Start Txn2, create a Object "obj2". Txn2 scan "tb" and "obj1, obj2" found
// 2. Start Txn3, scan "tb" and only "obj1" found
// 3. Commit Txn2
// 4. Txn3 scan "tb" and also only "obj1" found
// 5. Start Txn4, scan "tb" and both "obj1" and "obj2" found
func TestObject1(t *testing.T) {
	defer testutils.AfterTest(t)()
	catalog := MockCatalog()
	defer catalog.Close()
	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	name := "db"
	tbName := "tb"
	txn1, _ := txnMgr.StartTxn(nil)
	db, err := catalog.CreateDBEntry(name, "", "", txn1)
	assert.Nil(t, err)
	schema := MockSchema(1, 0)
	schema.Name = tbName
	tb, err := db.CreateTableEntry(schema, txn1, nil)
	assert.Nil(t, err)
	stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)
	obj1, err := tb.CreateObject(txn1, &objectio.CreateObjOpt{Stats: stats, IsTombstone: false}, nil)
	assert.Nil(t, err)
	err = txn1.Commit(context.Background())
	assert.Nil(t, err)
	t.Log(obj1.String())
	t.Log(tb.String())
}

func TestAlterSchema(t *testing.T) {
	schema := MockSchema(10, 5)
	req := api.NewAddColumnReq(0, 0, "xyz", types.NewProtoType(types.T_int32), 2)
	require.NoError(t, schema.ApplyAlterTable(req))
	require.Equal(t, 12, len(schema.NameMap))
	require.Equal(t, 12, len(schema.SeqnumMap))

	require.Equal(t, 2, schema.GetColIdx("xyz"))
	require.Equal(t, uint16(10), schema.GetSeqnum("xyz"))
	require.Equal(t, 2, schema.SeqnumMap[10])
	require.Equal(t, 6, schema.GetColIdx("mock_5"))
	require.Equal(t, uint16(5), schema.GetSeqnum("mock_5"))
	require.Equal(t, 11, schema.GetColIdx(PhyAddrColumnName))
	require.Equal(t, uint16(65535), schema.GetSeqnum(PhyAddrColumnName))
	require.Equal(t, true, schema.HasPK())
	require.Equal(t, true, schema.HasSortKey())
	require.Equal(t, "mock_5", schema.GetSingleSortKey().Name)
	require.Equal(t, uint16(5), schema.GetSingleSortKey().SeqNum)
	require.Equal(t, 6, schema.GetSingleSortKeyIdx())

	req = api.NewRemoveColumnReq(0, 0, 4, 3)
	schema.ApplyAlterTable(req)
	require.Equal(t, 11, len(schema.NameMap))
	require.Equal(t, 11, len(schema.SeqnumMap))

	require.Equal(t, 2, schema.GetColIdx("xyz"))
	require.Equal(t, uint16(10), schema.GetSeqnum("xyz"))
	require.Equal(t, 2, schema.SeqnumMap[10])
	require.Equal(t, 5, schema.GetColIdx("mock_5"))
	require.Equal(t, uint16(5), schema.GetSeqnum("mock_5"))
	require.Equal(t, 10, schema.GetColIdx(PhyAddrColumnName))
	require.Equal(t, uint16(65535), schema.GetSeqnum(PhyAddrColumnName))
	require.Equal(t, true, schema.HasPK())
	require.Equal(t, true, schema.HasSortKey())
	require.Equal(t, "mock_5", schema.GetSingleSortKey().Name)
	require.Equal(t, uint16(5), schema.GetSingleSortKey().SeqNum)
	require.Equal(t, 5, schema.GetSingleSortKeyIdx())

}

func randomTxnID(t *testing.T) []byte {
	bytes := make([]byte, 8)
	_, err := rand.Read(bytes)
	require.NoError(t, err)
	return bytes
}

func TestTxnManager_GetOrCreateTxnWithMeta(t *testing.T) {
	mockCatalog := MockCatalog()
	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(mockCatalog), MockTxnFactory(mockCatalog), types.NewMockHLCClock(1))
	txn1 := randomTxnID(t)
	ts := *types.BuildTSForTest(10, 0)
	meta, err := txnMgr.GetOrCreateTxnWithMeta(nil, txn1, ts)
	require.NoError(t, err)
	require.Equal(t, string(txn1), meta.GetID())

	meta2, err := txnMgr.GetOrCreateTxnWithMeta(nil, txn1, ts)
	require.NoError(t, err)
	require.Equal(t, string(txn1), meta2.GetID())
}
