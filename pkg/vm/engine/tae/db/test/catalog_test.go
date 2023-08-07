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

	db := initDB(ctx, t, nil)
	defer db.Close()

	schema := catalog.MockSchema(1, 0)
	txn, _, rel := testutil.CreateRelationNoCommit(t, db, defaultTestDB, schema, true)
	// relMeta := rel.GetMeta().(*catalog.TableEntry)
	seg, _ := rel.CreateSegment(false)
	blk, err := seg.CreateBlock(false)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	txn, rel = testutil.GetDefaultRelation(t, db, schema.Name)
	sseg, err := rel.GetSegment(seg.GetID())
	assert.Nil(t, err)
	t.Log(sseg.String())
	err = sseg.SoftDeleteBlock(blk.Fingerprint().BlockID)
	assert.Nil(t, err)

	t.Log(db.Catalog.SimplePPString(common.PPL1))
	blk2, err := sseg.CreateBlock(false)
	assert.Nil(t, err)
	assert.NotNil(t, blk2)
	assert.Nil(t, txn.Commit(context.Background()))
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	{
		_, rel = testutil.GetDefaultRelation(t, db, schema.Name)
		it := rel.MakeBlockIt()
		cnt := 0
		for it.Valid() {
			block := it.GetBlock()
			cnt++
			t.Log(block.String())
			it.Next()
		}
		assert.Equal(t, 1, cnt)
	}
}

func TestShowDatabaseNames(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := initDB(ctx, t, nil)
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
	tae := initDB(ctx, t, opts)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(13, 12)
	db, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn.Commit(context.Background())
	assert.Nil(t, err)

	pool, _ := ants.NewPool(20)
	defer pool.Release()
	var wg sync.WaitGroup
	mockRes := func() {
		defer wg.Done()
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment(false)
		assert.Nil(t, err)
		var id *common.ID
		for i := 0; i < 30; i++ {
			blk, err := seg.CreateBlock(false)
			if i == 2 {
				id = blk.Fingerprint()
			}
			assert.Nil(t, err)
		}
		err = txn.Commit(context.Background())
		assert.Nil(t, err)

		txn, _ = tae.StartTxn(nil)
		db, _ = txn.GetDatabase("db")
		rel, _ = db.GetRelationByName(schema.Name)
		seg, _ = rel.GetSegment(id.SegmentID())
		err = seg.SoftDeleteBlock(id.BlockID)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		err := pool.Submit(mockRes)
		assert.Nil(t, err)
	}
	wg.Wait()
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(ts)
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
