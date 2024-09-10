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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
)

func TestCheckpoint1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()
	schema := catalog.MockSchema(13, 12)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
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
		blk := testutil.GetOneObject(rel)
		err := rel.RangeDelete(blk.Fingerprint(), 3, 3, handle.DT_Normal)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}

	blockCnt := 0
	fn := func() bool {
		blockCnt = 0
		objectFn := func(entry *catalog.ObjectEntry) error {
			blockCnt += entry.BlockCnt()
			return nil
		}
		processor := new(catalog.LoopProcessor)
		processor.ObjectFn = objectFn
		err := db.Catalog.RecurLoop(processor)
		assert.NoError(t, err)
		return blockCnt == 2
	}
	testutils.WaitExpect(1000, fn)
	fn()
	assert.Equal(t, 2, blockCnt)
}

func TestCheckpoint2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := new(options.Options)
	// opts.CheckpointCfg = new(options.CheckpointCfg)
	// opts.CheckpointCfg.ScannerInterval = 10
	// opts.CheckpointCfg.ExecutionLevels = 2
	// opts.CheckpointCfg.ExecutionInterval = 1
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema1 := catalog.MockSchema(4, 2)
	schema1.BlockMaxRows = 10
	schema1.ObjectMaxBlocks = 2
	schema2 := catalog.MockSchema(4, 2)
	schema2.BlockMaxRows = 10
	schema2.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*2))
	defer bat.Close()
	bats := bat.Split(10)
	var (
		meta1 *catalog.TableEntry
		meta2 *catalog.TableEntry
	)
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase("db", "", "")
		rel1, _ := db.CreateRelation(schema1)
		rel2, _ := db.CreateRelation(schema2)
		meta1 = rel1.GetMeta().(*catalog.TableEntry)
		meta2 = rel2.GetMeta().(*catalog.TableEntry)
		t.Log(meta1.String())
		t.Log(meta2.String())
		assert.Nil(t, txn.Commit(context.Background()))
	}
	for i, data := range bats[0:8] {
		var name string
		if i%2 == 0 {
			name = schema1.Name
		} else {
			name = schema2.Name
		}
		testutil.AppendClosure(t, data, name, tae, nil)()
	}
	var meta *catalog.ObjectEntry
	testutils.WaitExpect(1000, func() bool {
		return tae.Wal.GetPenddingCnt() == 9
	})
	assert.Equal(t, uint64(9), tae.Wal.GetPenddingCnt())
	t.Log(tae.Wal.GetPenddingCnt())
	testutil.AppendClosure(t, bats[8], schema1.Name, tae, nil)()
	// t.Log(tae.MTBufMgr.String())

	{
		txn, _ := tae.StartTxn(nil)
		db, err := txn.GetDatabase("db")
		assert.Nil(t, err)
		rel, err := db.GetRelationByName(schema1.Name)
		assert.Nil(t, err)
		blk := testutil.GetOneObject(rel)
		meta = blk.GetMeta().(*catalog.ObjectEntry)
		task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, []*catalog.ObjectEntry{meta}, nil, tae.Runtime)
		assert.Nil(t, err)
		err = tae.Runtime.Scheduler.Schedule(task)
		assert.Nil(t, err)
		err = task.WaitDone(ctx)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func TestSchedule1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	schema := catalog.MockSchema(13, 12)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db", "", "")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(context.Background(), bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	testutil.CompactBlocks(t, 0, db, "db", schema, false)
	t.Log(db.Catalog.SimplePPString(common.PPL1))
	db.Close()
}
