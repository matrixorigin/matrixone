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

package metadata

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/stretchr/testify/assert"
)

func TestDatabase3(t *testing.T) {
	dir := "/tmp/metadata/testdatabase"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog, indexWal := initTest(dir, blockRows, segmentBlocks, true, true)

	dbName := "db1"
	shardId := uint64(100)
	gen := shard.NewMockIndexAllocator().Shard(shardId)

	err := indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	db, err := catalog.SimpleCreateDatabase(dbName, gen.Curr())
	assert.Nil(t, err)
	assert.Equal(t, dbName, db.Name)
	indexWal.Checkpoint(gen.Curr())

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	_, err = catalog.SimpleCreateDatabase(dbName, gen.Curr())
	assert.NotNil(t, err)
	indexWal.Checkpoint(gen.Curr())

	err = catalog.SimpleHardDeleteDatabase(db.Id)
	assert.NotNil(t, err)

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	err = catalog.SimpleDropDatabaseByName(dbName, gen.Curr())
	assert.Nil(t, err)
	indexWal.Checkpoint(gen.Curr())

	assert.True(t, db.IsSoftDeleted())

	shardId = uint64(101)
	gen = shard.NewMockIndexAllocator().Shard(shardId)
	_, err = catalog.SimpleGetDatabaseByName(dbName)
	assert.NotNil(t, err)

	t.Log(db.PString(PPL1, 0))

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	db2, err := catalog.SimpleCreateDatabase(dbName, gen.Curr())
	assert.Nil(t, err)
	indexWal.Checkpoint(gen.Curr())

	schema := MockSchema(2)
	schema.Name = "t1"

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	t1, err := db2.SimpleCreateTable(schema, gen.Curr())
	assert.Nil(t, err)
	assert.Equal(t, schema.Name, t1.Schema.Name)
	indexWal.Checkpoint(gen.Curr())

	found := db2.SimpleGetTableByName(t1.Schema.Name)
	assert.Equal(t, t1, found)

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	err = db2.SimpleDropTableByName(t1.Schema.Name, gen.Curr())
	assert.Nil(t, err)
	indexWal.Checkpoint(gen.Curr())

	found = db2.SimpleGetTableByName(t1.Schema.Name)
	assert.Nil(t, found)

	err = db2.SimpleHardDeleteTable(t1.Id)
	assert.Nil(t, err)
	assert.True(t, t1.IsHardDeleted())

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	_, err = db2.SimpleCreateTable(schema, gen.Curr())
	assert.Nil(t, err)
	indexWal.Checkpoint(gen.Curr())

	shardId = uint64(102)
	gen = shard.NewMockIndexAllocator().Shard(shardId)

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	db3, err := catalog.SimpleCreateDatabase("db3", gen.Curr())
	assert.Nil(t, err)
	indexWal.Checkpoint(gen.Curr())

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	err = db3.SimpleSoftDelete(gen.Curr())
	assert.Nil(t, err)

	err = db3.SimpleHardDelete()
	assert.NotNil(t, err)

	indexWal.Checkpoint(gen.Curr())
	testutils.WaitExpect(200, func() bool {
		return gen.Get() == db3.GetCheckpointId()
	})
	err = db3.SimpleHardDelete()
	assert.Nil(t, err)

	gen = shard.NewMockIndexAllocator().Shard(103)
	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	db4, err := catalog.SimpleCreateDatabase("db4", gen.Curr())
	assert.Nil(t, err)
	indexWal.Checkpoint(gen.Curr())

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	_, err = db4.SimpleCreateTable(schema, gen.Curr())
	assert.Nil(t, err)

	schema2 := MockSchema(3)
	schema2.Name = "t2"
	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	t2, err := db4.SimpleCreateTable(schema2, gen.Curr())
	assert.Nil(t, err)
	createIdx := gen.Curr()

	err = indexWal.SyncLog(gen.Next())
	assert.Nil(t, err)
	err = t2.SimpleSoftDelete(gen.Curr())
	assert.Nil(t, err)
	dropIdx := gen.Curr()

	dbDeleted := 0
	tableDeleted := 0
	dbCnt := 0
	tblCnt := 0
	processor := new(LoopProcessor)
	processor.DatabaseFn = func(db *Database) error {
		if db.IsHardDeleted() && db.HasCommitted() {
			dbDeleted++
		} else {
			dbCnt++
		}
		return nil
	}
	processor.TableFn = func(table *Table) error {
		if table.IsHardDeleted() && table.HasCommitted() {
			tableDeleted++
		} else {
			tblCnt++
		}
		return nil
	}
	catalog.RecurLoopLocked(processor)
	assert.Equal(t, 1, dbDeleted)
	assert.Equal(t, 1, tableDeleted)
	assert.Equal(t, 3, dbCnt)
	assert.Equal(t, 3, tblCnt)

	catalog.Compact(nil, nil)
	dbDeleted = 0
	tableDeleted = 0
	dbCnt, tblCnt = 0, 0
	catalog.RecurLoopLocked(processor)
	assert.Equal(t, 0, dbDeleted)
	assert.Equal(t, 0, tableDeleted)
	assert.Equal(t, 3, dbCnt)
	assert.Equal(t, 3, tblCnt)

	t.Log(catalog.PString(PPL0, 0))
	indexWal.Close()
	catalog.Close()
	t.Log("----------")
	catalog2, indexWal2 := initTest(dir, blockRows, segmentBlocks, true, false)
	defer indexWal2.Close()
	defer catalog2.Close()
	dbDeleted = 0
	tableDeleted = 0
	dbCnt, tblCnt = 0, 0
	catalog2.RecurLoopLocked(processor)
	t.Log(indexWal.String())
	t.Log(indexWal2.String())
	assert.Equal(t, 0, dbDeleted)
	assert.Equal(t, 0, tableDeleted)
	assert.Equal(t, 3, dbCnt)
	assert.Equal(t, 3, tblCnt)

	assert.Equal(t, indexWal.GetShardCheckpointId(100), indexWal2.GetShardCheckpointId(100))
	assert.Equal(t, indexWal.GetShardCheckpointId(101), indexWal2.GetShardCheckpointId(101))
	assert.Equal(t, indexWal.GetShardCheckpointId(103), indexWal2.GetShardCheckpointId(103))
	assert.Equal(t, indexWal2.GetShardCheckpointId(103), gen.Curr().Id.Id-3)

	db4Replayed, err := catalog2.SimpleGetDatabaseByName(db4.Name)
	assert.Nil(t, err)

	f := db4Replayed.FindTableCommitByIndex(schema2.Name, createIdx)
	assert.NotNil(t, f)
	f = db4Replayed.FindTableCommitByIndex(schema2.Name, dropIdx)
	assert.NotNil(t, f)

	f = db4Replayed.FindCommitByIndexLocked(db4Replayed.FirstCommitLocked().LogIndex)
	assert.NotNil(t, f)
	f = db4Replayed.FindCommitByIndexLocked(db4Replayed.CommitInfo.LogIndex)
	assert.NotNil(t, f)

	t.Log(catalog2.PString(PPL0, 0))
}

func TestTxn(t *testing.T) {
	dir := "/tmp/metadata/testtxn"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog, _ := initTest(dir, blockRows, segmentBlocks, false, true)
	gen := shard.NewMockIndexAllocator()

	shardId := uint64(100)

	txn := catalog.StartTxn(gen.Next(shardId))
	assert.NotNil(t, txn)
	db1, err := catalog.CreateDatabaseInTxn(txn, "db1")
	assert.Nil(t, err)
	assert.False(t, db1.HasCommitted())
	schema := MockSchema(2)
	schema.Name = "t1"
	_, err = db1.CreateTableInTxn(txn, schema)
	assert.Nil(t, err)
	t.Log(db1.PString(PPL0, 0))

	_, err = catalog.SimpleGetDatabaseByName(db1.Name)
	assert.NotNil(t, err)
	_, err = catalog.GetDatabaseByNameInTxn(txn, db1.Name)
	assert.Nil(t, err)

	err = txn.Commit()
	assert.Nil(t, err)
	assert.True(t, db1.HasCommitted())
	t.Log(db1.PString(PPL0, 0))

	_, err = catalog.SimpleGetDatabaseByName(db1.Name)
	assert.Nil(t, err)

	txn = catalog.StartTxn(gen.Next(shardId))
	assert.NotNil(t, txn)
	db1, err = catalog.GetDatabaseByNameInTxn(txn, db1.Name)
	assert.Nil(t, err)
	assert.True(t, db1.HasCommitted())
	schema2 := MockSchema(3)
	schema2.Name = "t2"
	t2, err := db1.CreateTableInTxn(txn, schema2)
	assert.Nil(t, err)
	assert.False(t, t2.HasCommitted())
	foundT := db1.GetTableByNameInTxn(txn, schema2.Name)
	assert.NotNil(t, foundT)
	foundT = db1.SimpleGetTableByName(schema2.Name)
	assert.Nil(t, foundT)

	_, err = db1.DropTableByNameInTxn(txn, schema.Name)
	assert.Nil(t, err)

	err = db1.SimpleDropTableByName(schema.Name, gen.Next(shardId))
	assert.NotNil(t, err)
	err = db1.SimpleDropTableByName(schema2.Name, gen.Next(shardId))
	assert.NotNil(t, err)

	_, err = db1.SimpleCreateTable(schema2, gen.Next(shardId))
	assert.NotNil(t, err)

	err = db1.SoftDeleteInTxn(txn)
	assert.Nil(t, err)
	err = db1.SoftDeleteInTxn(txn)
	assert.NotNil(t, err)

	assert.False(t, db1.HasCommitted())
	err = txn.Commit()
	assert.Nil(t, err)
	assert.True(t, db1.IsDeleted())
	assert.True(t, db1.HasCommitted())

	t.Log(db1.PString(PPL0, 0))
	catalog.Close()

	catalog2, _ := initTest(dir, blockRows, segmentBlocks, false, false)
	defer catalog2.Close()
	t.Log(catalog2.PString(PPL1, 0))
}
