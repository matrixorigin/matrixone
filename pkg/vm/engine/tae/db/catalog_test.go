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

package db

import (
	"bytes"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestCatalog1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchema(1, 0)
	txn, _, rel := createRelationNoCommit(t, db, defaultTestDB, schema, true)
	// relMeta := rel.GetMeta().(*catalog.TableEntry)
	seg, _ := rel.CreateSegment()
	blk, err := seg.CreateBlock()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	txn, rel = getDefaultRelation(t, db, schema.Name)
	sseg, err := rel.GetSegment(seg.GetID())
	assert.Nil(t, err)
	t.Log(sseg.String())
	err = sseg.SoftDeleteBlock(blk.Fingerprint().BlockID)
	assert.Nil(t, err)

	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	blk2, err := sseg.CreateBlock()
	assert.Nil(t, err)
	assert.NotNil(t, blk2)
	assert.Nil(t, txn.Commit())
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	{
		_, rel = getDefaultRelation(t, db, schema.Name)
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
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()

	{
		txn, _ := tae.StartTxn(nil)
		_, err := txn.CreateDatabase("db1")
		assert.Nil(t, err)
		names := txn.DatabaseNames()
		assert.Equal(t, 2, len(names))
		assert.Equal(t, "db1", names[1])
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := tae.StartTxn(nil)
		names := txn.DatabaseNames()
		assert.Equal(t, 2, len(names))
		assert.Equal(t, "db1", names[1])
		_, err := txn.CreateDatabase("db2")
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
			_, err := txn.CreateDatabase("db2")
			assert.NotNil(t, err)
			err = txn.Rollback()
			assert.Nil(t, err)
		}
		{
			txn, _ := tae.StartTxn(nil)
			_, err := txn.CreateDatabase("db3")
			assert.Nil(t, err)
			names := txn.DatabaseNames()
			assert.Equal(t, "db1", names[1])
			assert.Equal(t, "db3", names[2])
			assert.Nil(t, txn.Commit())
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
			assert.Nil(t, txn.Commit())
		}
		names = txn.DatabaseNames()
		assert.Equal(t, 3, len(names))
		assert.Equal(t, "db1", names[1])
		assert.Equal(t, "db2", names[2])
		assert.Nil(t, txn.Commit())
	}
}

func TestLogBlock(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 0)
	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	seg, _ := rel.CreateSegment()
	blk, _ := seg.CreateBlock()
	meta := blk.GetMeta().(*catalog.BlockEntry)
	err := txn.Commit()
	assert.Nil(t, err)
	cmd := meta.MakeLogEntry()
	assert.NotNil(t, cmd)

	var w bytes.Buffer
	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	cmd2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	entryCmd := cmd2.(*catalog.EntryCommand)
	t.Log(meta.StringLocked())
	t.Log(entryCmd.Block.StringLocked())
	assert.Equal(t, meta.ID, entryCmd.Block.ID)
	assert.Equal(t, meta.CurrOp, entryCmd.Block.CurrOp)
	assert.Equal(t, meta.CreateAt, entryCmd.Block.CreateAt)
	assert.Equal(t, meta.DeleteAt, entryCmd.Block.DeleteAt)
}

func TestLogSegment(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 0)
	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	seg, _ := rel.CreateSegment()
	meta := seg.GetMeta().(*catalog.SegmentEntry)
	err := txn.Commit()
	assert.Nil(t, err)
	cmd := meta.MakeLogEntry()
	assert.NotNil(t, cmd)

	var w bytes.Buffer
	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	cmd2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	entryCmd := cmd2.(*catalog.EntryCommand)
	t.Log(meta.StringLocked())
	t.Log(entryCmd.Segment.StringLocked())
	assert.Equal(t, meta.ID, entryCmd.Segment.ID)
	assert.Equal(t, meta.CurrOp, entryCmd.Segment.CurrOp)
	assert.Equal(t, meta.CreateAt, entryCmd.Segment.CreateAt)
	assert.Equal(t, meta.DeleteAt, entryCmd.Segment.DeleteAt)
}

func TestLogTable(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	meta := rel.GetMeta().(*catalog.TableEntry)
	err := txn.Commit()
	assert.Nil(t, err)
	cmd := meta.MakeLogEntry()
	assert.NotNil(t, cmd)

	var w bytes.Buffer
	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	cmd2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	entryCmd := cmd2.(*catalog.EntryCommand)
	t.Log(meta.StringLocked())
	t.Log(entryCmd.Table.StringLocked())
	assert.Equal(t, meta.ID, entryCmd.Table.ID)
	assert.Equal(t, meta.CurrOp, entryCmd.Table.CurrOp)
	assert.Equal(t, meta.CreateAt, entryCmd.Table.CreateAt)
	assert.Equal(t, meta.DeleteAt, entryCmd.Table.DeleteAt)
	assert.Equal(t, meta.GetSchema().Name, entryCmd.Table.GetSchema().Name)
	assert.Equal(t, meta.GetSchema().BlockMaxRows, entryCmd.Table.GetSchema().BlockMaxRows)
	assert.Equal(t, meta.GetSchema().SegmentMaxBlocks, entryCmd.Table.GetSchema().SegmentMaxBlocks)
	assert.Equal(t, meta.GetSchema().GetSingleSortKeyIdx(), entryCmd.Table.GetSchema().GetSingleSortKeyIdx())
	assert.Equal(t, meta.GetSchema().Types(), entryCmd.Table.GetSchema().Types())
}

func TestLogDatabase(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	meta := db.GetMeta().(*catalog.DBEntry)
	err := txn.Commit()
	assert.Nil(t, err)
	cmd := meta.MakeLogEntry()
	assert.NotNil(t, cmd)

	var w bytes.Buffer
	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	cmd2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	entryCmd := cmd2.(*catalog.EntryCommand)
	t.Log(meta.StringLocked())
	t.Log(entryCmd.DB.StringLocked())
	assert.Equal(t, meta.ID, entryCmd.DB.ID)
	assert.Equal(t, meta.CurrOp, entryCmd.DB.CurrOp)
	assert.Equal(t, meta.CreateAt, entryCmd.DB.CreateAt)
	assert.Equal(t, meta.DeleteAt, entryCmd.DB.DeleteAt)
	assert.Equal(t, meta.GetName(), entryCmd.DB.GetName())
}

func TestCheckpointCatalog2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(13, 12)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)

	pool, _ := ants.NewPool(20)
	var wg sync.WaitGroup
	mockRes := func() {
		defer wg.Done()
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		var id *common.ID
		for i := 0; i < 30; i++ {
			blk, err := seg.CreateBlock()
			if i == 2 {
				id = blk.Fingerprint()
			}
			assert.Nil(t, err)
		}
		err = txn.Commit()
		assert.Nil(t, err)

		txn, _ = tae.StartTxn(nil)
		db, _ = txn.GetDatabase("db")
		rel, _ = db.GetRelationByName(schema.Name)
		seg, _ = rel.GetSegment(id.SegmentID)
		err = seg.SoftDeleteBlock(id.BlockID)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		err := pool.Submit(mockRes)
		assert.Nil(t, err)
	}
	wg.Wait()
	ts := tae.Scheduler.GetSafeTS()
	entry := tae.Catalog.PrepareCheckpoint(0, ts)
	maxIndex := entry.GetMaxIndex()
	err = tae.Catalog.Checkpoint(ts)
	assert.Nil(t, err)
	testutils.WaitExpect(1000, func() bool {
		ckp := tae.Scheduler.GetCheckpointedLSN()
		return ckp == maxIndex.LSN
	})
	assert.Equal(t, maxIndex.LSN, tae.Scheduler.GetCheckpointedLSN())
}

func TestCheckpointCatalog(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(2, 0)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)

	pool, _ := ants.NewPool(1)
	var wg sync.WaitGroup
	mockRes := func() {
		defer wg.Done()
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		var id *common.ID
		for i := 0; i < 4; i++ {
			blk, err := seg.CreateBlock()
			if i == 2 {
				id = blk.Fingerprint()
			}
			assert.Nil(t, err)
		}
		err = txn.Commit()
		assert.Nil(t, err)

		txn, _ = tae.StartTxn(nil)
		db, err = txn.GetDatabase("db")
		assert.Nil(t, err)
		rel, err = db.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		seg, err = rel.GetSegment(id.SegmentID)
		assert.Nil(t, err)
		err = seg.SoftDeleteBlock(id.BlockID)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		err := pool.Submit(mockRes)
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Log(tae.Catalog.SimplePPString(common.PPL1))

	startTs := uint64(0)
	endTs := tae.Scheduler.GetSafeTS() - 2
	t.Logf("endTs=%d", endTs)

	entry := tae.Catalog.PrepareCheckpoint(startTs, endTs)
	blkCnt := 0
	blocks := make([]*catalog.BlockEntry, 0)
	for _, cmd := range entry.Entries {
		if cmd.Block != nil {
			blkCnt++
			blocks = append(blocks, cmd.Block)
		}
	}
	entry.PrintItems()
	assert.Equal(t, 8, blkCnt)
	entry2 := tae.Catalog.PrepareCheckpoint(endTs+1, tae.Scheduler.GetSafeTS())

	blkCnt = 0
	for _, cmd := range entry2.Entries {
		if cmd.Block != nil {
			blkCnt++
			t.Logf("%s", cmd.Block.StringLocked())
		}
	}
	assert.Equal(t, 1, blkCnt)
	entry3 := tae.Catalog.PrepareCheckpoint(0, endTs-1)
	entry3.PrintItems()

	blockEntry := blocks[6]
	seg := blockEntry.GetSegment()
	blk, err := seg.GetBlockEntryByID(blockEntry.ID)
	t.Log(blk.String())
	assert.Nil(t, err)
	assert.True(t, blk.HasDropped())
	assert.True(t, blk.DeleteAt > endTs)
	assert.True(t, blk.CreateAt > startTs)
	assert.Equal(t, blk.CreateAt, blockEntry.CreateAt)
	assert.Equal(t, uint64(0), blockEntry.DeleteAt)

	buf, err := entry.Marshal()
	assert.Nil(t, err)
	t.Log(len(buf))

	replayEntry := catalog.NewEmptyCheckpointEntry()
	err = replayEntry.Unmarshal(buf)
	assert.Nil(t, err)
	assert.Equal(t, entry.MinTS, replayEntry.MinTS)
	assert.Equal(t, entry.MaxTS, replayEntry.MaxTS)
	assert.Equal(t, len(entry.Entries), len(replayEntry.Entries))
	for i := 0; i < len(entry.Entries); i++ {
		if entry.Entries[i].Block != nil {
			blk1 := entry.Entries[i].Block
			blk2 := replayEntry.Entries[i].Block
			assert.Equal(t, blk1.ID, blk2.ID)
			assert.Equal(t, blk1.CreateAt, blk2.CreateAt)
			assert.Equal(t, blk1.DeleteAt, blk2.DeleteAt)
			assert.Equal(t, blk1.CurrOp, blk2.CurrOp)
		}
	}
	replayEntry.PrintItems()

	err = tae.Catalog.Checkpoint(endTs)
	assert.Nil(t, err)

	assert.Equal(t, endTs, tae.Catalog.GetCheckpointed().MaxTS)
	t.Log(tae.Catalog.SimplePPString(common.PPL1))

	// logEntry, err := entry.MakeLogEntry()
	// assert.Nil(t, err)
	// lsn, err := tae.Wal.AppendEntry(wal.GroupCatalog, logEntry)
	// logEntry.WaitDone()
	// logEntry.Free()
	// t.Log(lsn)
}
