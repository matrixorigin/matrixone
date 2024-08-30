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

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestCatalog1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	schema := catalog.MockSchema(1, 0)
	txn, _, rel := testutil.CreateRelationNoCommit(t, db, testutil.DefaultTestDB, schema, true)
	// relMeta := rel.GetMeta().(*catalog.TableEntry)
	obj, err := rel.CreateNonAppendableObject(false, nil)
	assert.Nil(t, err)
	testutil.MockObjectStats(t, obj)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, db, schema.Name)
	sobj, err := rel.GetObject(obj.GetID(), false)
	assert.Nil(t, err)
	t.Log(sobj.String())

	t.Log(db.Catalog.SimplePPString(common.PPL1))
	assert.Nil(t, txn.Commit(context.Background()))
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	{
		_, rel = testutil.GetDefaultRelation(t, db, schema.Name)
		it := rel.MakeObjectIt(false)
		cnt := 0
		for it.Next() {
			object := it.GetObject()
			cnt++
			t.Log(object.GetMeta().(*catalog.ObjectEntry).String())
		}
		it.Close()
		assert.Equal(t, 1, cnt)
	}
}

func TestShowDatabaseNames(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()

	{
		txn, _ := tae.StartTxn(nil)
		_, err := txn.CreateDatabase("db1", "", "")
		assert.Nil(t, err)
		names := txn.DatabaseNames()
		assert.Equal(t, 2, len(names))
		assert.Equal(t, "db1", names[1])
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := tae.StartTxn(nil)
		names := txn.DatabaseNames()
		assert.Equal(t, 2, len(names))
		assert.Equal(t, "db1", names[1])
		_, err := txn.CreateDatabase("db2", "", "")
		assert.Nil(t, err)
		names = txn.DatabaseNames()
		t.Log(tae.Catalog.SimplePPString(common.PPL1))
		assert.Equal(t, 3, len(names))
		assert.Equal(t, "db1", names[1])
		assert.Equal(t, "db2", names[2])
		{
			txn, _ := tae.StartTxn(nil)
			names := txn.DatabaseNames()
			assert.Equal(t, 2, len(names))
			assert.Equal(t, "db1", names[1])
			_, err := txn.CreateDatabase("db2", "", "")
			assert.NotNil(t, err)
			err = txn.Rollback(context.Background())
			assert.Nil(t, err)
		}
		{
			txn, _ := tae.StartTxn(nil)
			_, err := txn.CreateDatabase("db3", "", "")
			assert.Nil(t, err)
			names := txn.DatabaseNames()
			assert.Equal(t, "db1", names[1])
			assert.Equal(t, "db3", names[2])
			assert.Nil(t, txn.Commit(context.Background()))
		}
		{
			txn, _ := tae.StartTxn(nil)
			names := txn.DatabaseNames()
			assert.Equal(t, 3, len(names))
			assert.Equal(t, "db1", names[1])
			assert.Equal(t, "db3", names[2])
			_, err := txn.DropDatabase("db1")
			assert.Nil(t, err)
			names = txn.DatabaseNames()
			t.Log(tae.Catalog.SimplePPString(common.PPL1))
			t.Log(names)
			assert.Equal(t, 2, len(names))
			assert.Equal(t, "db3", names[1])
			assert.Nil(t, txn.Commit(context.Background()))
		}
		names = txn.DatabaseNames()
		assert.Equal(t, 3, len(names))
		assert.Equal(t, "db1", names[1])
		assert.Equal(t, "db2", names[2])
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func TestCheckpointCatalog2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(13, 12)
	db, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn.Commit(context.Background())
	assert.Nil(t, err)
	schema.BlockMaxRows = 10
	batchCnt := 10
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows)*batchCnt)
	bats := bat.Split(batchCnt)

	pool, _ := ants.NewPool(20)
	defer pool.Release()
	var wg sync.WaitGroup
	mockRes := func(i int) func() {
		return func() {
			defer wg.Done()
			txn, _ := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			rel, _ := db.GetRelationByName(schema.Name)
			err := rel.Append(context.Background(), bats[i])
			assert.Nil(t, err)
			err = txn.Commit(context.Background())
			assert.Nil(t, err)
		}
	}
	for i := 0; i < batchCnt; i++ {
		wg.Add(1)
		err := pool.Submit(mockRes(i))
		assert.Nil(t, err)
	}
	wg.Wait()
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(ts, false)
	assert.NoError(t, err)
	lsn := tae.BGCheckpointRunner.MaxLSNInRange(ts)
	entry, err := tae.Wal.RangeCheckpoint(1, lsn)
	assert.NoError(t, err)
	assert.NoError(t, entry.WaitDone())
	testutils.WaitExpect(1000, func() bool {
		return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	assert.Equal(t, tae.BGCheckpointRunner.MaxLSN(), tae.Runtime.Scheduler.GetCheckpointedLSN())
}
