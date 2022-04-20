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

package aoedb

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/stretchr/testify/assert"
)

func TestSnapshot1(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	inst, gen, database := initTestDB1(t)
	shardID := database.GetShardId()
	idxGen := gen.Shard(shardID)
	schemas := make([]*metadata.Schema, 10)
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx := &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: false,
	}
	idx, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, idxGen.Get(), idx)

	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = inst.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)

	idx2 := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx2)
	inst.Close()

	inst2, _, _ := initTestDB3(t)
	defer inst2.Close()
	idx3 := inst2.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx3)

	names = inst2.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	// t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	t.Log(inst2.Store.Catalog.IndexWal.String())
}

func TestSnapshot2(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	inst, gen, database := initTestDB1(t)
	shardID := database.GetShardId()
	idxGen := gen.Shard(shardID)
	schemas := make([]*metadata.Schema, 3)
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx := &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())
	ckID := idxGen.Get()

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	_, err = inst.CreateTable(createCtx)
	assert.Nil(t, err)

	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: false,
	}
	idx, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, ckID, idx)

	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = inst.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)

	idx2 := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx2)
	inst.Close()

	inst2, _, _ := initTestDB3(t)
	defer inst2.Close()
	idx3 := inst2.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx3)

	names = inst2.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	// t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	t.Log(inst2.Store.Catalog.IndexWal.String())
}

func TestSnapshot3(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	inst, gen, database := initTestDB1(t)
	shardID := database.GetShardId()
	idxGen := gen.Shard(shardID)
	schemas := make([]*metadata.Schema, 2)
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx := &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())
	ckID := idxGen.Get()

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	_, err = inst.CreateTable(createCtx)
	assert.Nil(t, err)

	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx.ID = gen.Alloc(database.GetShardId())
	appendCtx.Table = schemas[1].Name
	appendCtx.Data = ck1
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	err = inst.FlushTable(database.Name, schemas[1].Name)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 1
	})
	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 1, stats[0].Count)

	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: false,
	}
	idx, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, ckID, idx)

	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = inst.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)

	idx2 := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx2)
	inst.Close()

	inst2, _, _ := initTestDB3(t)
	defer inst2.Close()
	idx3 := inst2.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx3)

	names = inst2.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	// t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	t.Log(inst2.Store.Catalog.IndexWal.String())
}

func TestSnapshot4(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	inst, gen, database := initTestDB1(t)
	shardID := database.GetShardId()
	idxGen := gen.Shard(shardID)
	schemas := make([]*metadata.Schema, 3)
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx := &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())
	ckID := idxGen.Get()

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx.ID = gen.Alloc(database.GetShardId())
	appendCtx.Table = schemas[1].Name
	appendCtx.Data = ck1
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	err = inst.FlushTable(database.Name, schemas[1].Name)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 1
	})
	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 1, stats[0].Count)

	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: false,
	}
	idx, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, ckID, idx)

	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = inst.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)

	idx2 := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx2)
	inst.Close()

	inst2, _, _ := initTestDB3(t)
	defer inst2.Close()
	idx3 := inst2.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx3)

	names = inst2.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	// t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	t.Log(inst2.Store.Catalog.IndexWal.String())
}

// TODO: update safe id when snapshot
// func TestSnapshot5(t *testing.T) {
// 	initTestEnv(t)
// 	prepareSnapshotPath(defaultSnapshotPath, t)
// 	inst, gen, database := initTestDB1(t)
// 	shardId := database.GetShardId()
// 	idxGen := gen.Shard(shardId)
// 	schemas := make([]*metadata.Schema, 3)
// 	for i := range schemas {
// 		schema := metadata.MockSchema(i + 1)
// 		createCtx := &CreateTableCtx{
// 			DBMutationCtx: *CreateDBMutationCtx(database, gen),
// 			Schema:        schema,
// 		}
// 		_, err := inst.CreateTable(createCtx)
// 		assert.Nil(t, err)
// 		schemas[i] = schema
// 	}

// 	testutils.WaitExpect(200, func() bool {
// 		return idxGen.Get() == database.GetCheckpointId()
// 	})
// 	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())

// 	names := inst.TableNames(database.Name)
// 	assert.Equal(t, len(schemas), len(names))

// 	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
// 	ck0 := mock.MockBatch(schemas[0].Types(), rows)
// 	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
// 	err := inst.Append(appendCtx)
// 	assert.Nil(t, err)

// 	ck1 := mock.MockBatch(schemas[1].Types(), rows)
// 	appendCtx.Id = gen.Alloc(database.GetShardId())
// 	appendCtx.Table = schemas[1].Name
// 	appendCtx.Data = ck1
// 	err = inst.Append(appendCtx)
// 	assert.Nil(t, err)

// 	err = inst.FlushTable(database.Name, schemas[0].Name)
// 	assert.Nil(t, err)
// 	testutils.WaitExpect(200, func() bool {
// 		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
// 		return stats[0].Count == 1
// 	})
// 	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
// 	assert.Equal(t, 1, stats[0].Count)
// 	ckId := inst.GetDBCheckpointId(database.Name)
// 	assert.Equal(t, uint64(4), ckId)

// 	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
// 	createSSCtx := &CreateSnapshotCtx{
// 		DB:   database.Name,
// 		Path: getSnapshotPath(defaultSnapshotPath, t),
// 		Sync: false,
// 	}
// 	idx, err := inst.CreateSnapshot(createSSCtx)
// 	assert.Nil(t, err)
// 	assert.Equal(t, ckId, idx)

// 	applySSCtx := &ApplySnapshotCtx{
// 		DB:   createSSCtx.DB,
// 		Path: createSSCtx.Path,
// 	}
// 	err = inst.ApplySnapshot(applySSCtx)
// 	assert.Nil(t, err)

// 	idx2 := inst.GetDBCheckpointId(database.Name)
// 	assert.Equal(t, idx, idx2)
// 	testutils.WaitExpect(200, func() bool {
// 		return database.GetCheckpointId() == gen.Get(database.GetShardId())
// 	})
// 	assert.Equal(t, database.GetCheckpointId(), gen.Get(database.GetShardId()))

// 	inst.Close()
// 	inst2, _, _ := initTestDB3(t)
// 	defer inst2.Close()

// 	dbReplayed, err := inst2.Store.Catalog.SimpleGetDatabase(database.Id)
// 	assert.Nil(t, err)
// 	assert.True(t, dbReplayed.IsReplaced())
// 	assert.Equal(t, dbReplayed.GetCheckpointId(), dbReplayed.GetCommit().GetIndex())

// 	err = inst.ForceCompactCatalog()
// 	assert.Nil(t, err)
// 	testutils.WaitExpect(500, func() bool {
// 		_, err := inst2.Store.Catalog.SimpleGetDatabase(database.Id)
// 		return err != nil
// 	})
// 	_, err = inst2.Store.Catalog.SimpleGetDatabase(database.Id)
// 	assert.NotNil(t, err)

// 	idx3 := inst2.GetDBCheckpointId(database.Name)
// 	assert.Equal(t, idx, idx3)

// 	names = inst2.TableNames(database.Name)
// 	assert.Equal(t, len(schemas), len(names))

// 	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
// 	t.Log(inst2.Store.Catalog.IndexWal.String())
// }

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        ?     ]
// 2.  Create 3 tables: 1-1 [1,?], 1-2 [2,?], 1-3 [3,?]   [   3,        ?     ]
// 3.  Wait and assert checkpoint                         [   -,        3     ]
// 4.  Append 1/10 (MaxBlockRows) rows into (1-1)         [   4,        3     ]
// 5.  Append 1/10 (MaxBlockRows) rows into (1-2)         [   5,        3     ]
// 6.  FlushTable (1-1), (1-2)                            [   -,        ?     ]
// 7.  Wait and assert checkpoint                         [   -,        5     ]
// 8.  Append 1/10 (MaxBlockRows) rows into (1-1)         [   6,        5     ]
// 9.  Append 1/10 (MaxBlockRows) rows into (1-2)         [   7,        5     ]
// 10. FlushTable (1-2) and wait [7] committed            [   -,        5     ]
// 11. Create snapshot, the snapshot index should be [5]  [   -,        5     ]
// 12. Create another db instance
// 13. Apply previous created snapshot
// 14. Check:
//     1) database exists 2) database checkpoint id is [5]
//     3) total 3 tables with 2 table of one segment and one block
//     4) replay [6] and [7] operation
//     5) check new applied database is same as origin database
// 15. Restart new db instance and check again
func TestSnapshot6(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	// 1. Create a db instance and a database
	inst, gen, database := initTestDB1(t)
	defer inst.Close()
	shardID := database.GetShardId()
	idxGen := gen.Shard(shardID)

	// 2. Create 3 tables
	schemas := make([]*metadata.Schema, 3)
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx := &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	// 3. Wait and check
	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())
	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	// 4. Append rows into table 1-1
	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	// 5. Append rows into table 1-2
	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx.ID = gen.Alloc(database.GetShardId())
	appendCtx.Table = schemas[1].Name
	appendCtx.Data = ck1
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	// 6. FlushTable tables
	err = inst.FlushTable(database.Name, schemas[0].Name)
	assert.Nil(t, err)
	err = inst.FlushTable(database.Name, schemas[1].Name)
	assert.Nil(t, err)

	// 7. Wait checkpointed
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 0
	})
	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 0, stats[0].Count)
	ckID := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, gen.Get(database.GetShardId()), ckID)

	// 8. Append rows into table 1-1
	appendCtx.ID = gen.Alloc(database.GetShardId())
	appendCtx.Table = schemas[0].Name
	appendCtx.Data = ck0
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	// 9. Append rows into table 1-2
	appendCtx.ID = gen.Alloc(database.GetShardId())
	appendCtx.Table = schemas[1].Name
	appendCtx.Data = ck1
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	// 10. FlushTable and wait all table 1-2 data checkpointed
	err = inst.FlushTable(database.Name, schemas[1].Name)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 1
	})
	stats = inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 1, stats[0].Count)
	assert.Equal(t, ckID, database.GetCheckpointId())

	// 11. Create snapshot
	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: false,
	}
	idx, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, ckID, idx)

	// 12. Create another db instance
	aoedb2, gen2, origindb := initTestDBWithOptions(t, "aoedb2", database.Name, defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)

	// 13. Apply snapshot
	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = aoedb2.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)

	// 14. Check
	db2, err := aoedb2.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)
	names = aoedb2.TableNames(db2.Name)
	assert.Equal(t, len(schemas), len(names))
	assert.Equal(t, idx, db2.GetCheckpointId())
	gen2.Reset(db2.GetShardId(), db2.GetCheckpointId())

	// PXU TODO: Idempotence
	// appendCtx.OpIndex = gen2.Alloc(db2.GetShardId())
	// appendCtx.TableName = schemas[0].Name
	// appendCtx.Data = ck0
	// err = inst.Append(appendCtx)
	// assert.Nil(t, err)
	// appendCtx.OpIndex = gen2.Alloc(db2.GetShardId())
	// appendCtx.TableName = schemas[1].Name
	// appendCtx.Data = ck1
	// err = inst.Append(appendCtx)

	// 15. Restart and check data replay
	aoedb2.Close()
	aoedb2, _, _ = initTestDBWithOptions(t, "aoedb2", "", defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)

	defer aoedb2.Close()

	dbReplayed, err := aoedb2.Store.Catalog.SimpleGetDatabase(origindb.Id)
	assert.Nil(t, err)
	assert.True(t, dbReplayed.IsReplaced())
	assert.Equal(t, dbReplayed.GetCheckpointId(), dbReplayed.GetCommit().GetIndex())

	err = aoedb2.ForceCompactCatalog()
	assert.Nil(t, err)
	testutils.WaitExpect(500, func() bool {
		_, err := aoedb2.Store.Catalog.SimpleGetDatabase(database.Id)
		return err != nil
	})
	_, err = aoedb2.Store.Catalog.SimpleGetDatabase(database.Id)
	assert.NotNil(t, err)

	idx3 := aoedb2.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx3)

	names = aoedb2.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))
	dbReplayed, err = aoedb2.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)
	t1 := dbReplayed.SimpleGetTableByName(schemas[0].Name)
	assert.NotNil(t, t1)
	t1Idx := t1.MaxLogIndex().Id.Id
	assert.Equal(t, uint64(4), t1Idx)

	t2 := dbReplayed.SimpleGetTableByName(schemas[1].Name)
	assert.NotNil(t, t2)
	t2Idx := t2.MaxLogIndex().Id.Id
	assert.Equal(t, gen.Get(database.GetShardId()), t2Idx)

	// t.Log(aoedb2.Store.Catalog.PString(metadata.PPL0, 0))
}

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        ?     ]
// 2.  Create 3 tables: 1-1 [1,?], 1-2 [2,?], 1-3 [3,?]   [   3,        ?     ]
// 3.  Wait and assert checkpoint                         [   -,        3     ]
// 4.  Append 1/10 (MaxBlockRows) rows into (1-1)         [   4,        3     ]
// 5.  Append 1/10 (MaxBlockRows) rows into (1-2)         [   5,        3     ]
// 6.  FlushTable (1-1) (1-2)                             [   -,        ?     ]
// 7.  Wait and assert checkpoint                         [   -,        5     ]
// 8.  Append 1/10 (MaxBlockRows) rows into (1-1)         [   6,        5     ]
// 9.  Drop (1-2)                                         [   7,        5     ]
// 10. Wait [7] committed                                 [   -,        5     ]
// 11. Create snapshot, the snapshot index should be [5]  [   -,        5     ]
// 12. Create another db instance
// 13. Apply previous created snapshot
// 14. Check:
//     1) database exists 2) database checkpoint id is [5]
//     3) total 3 tables with 2 table of one segment and one block
//     4) replay [6] and [7] operation
//     5) check new applied database is same as origin database
// 15. Restart new db instance and check again
func TestSnapshot7(t *testing.T) {
	// TODO
}

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        0     ]
// 2.  Disable flush segment                              [   0,        0     ]
// 3.  Create 2 tables: 1-1 [1,?], 1-2 [2,?]              [   2,        ?     ]
// 4.  Append 35/10 (MaxBlockRows) rows into (1-1)        [   3,        2     ]
// 5.  Append 35/10 (MaxBlockRows) rows into (1-2)        [   4,        2     ]
// 6.  FlushTable (1-1) (1-2)                             [   -,        ?     ]
// 7.  Create snapshot, the snapshot index should be [4]  [   -,        4     ]
// 8.  Create another db instance
// 9.  Apply previous created snapshot
// 10. Check:
//     1) database exists 2) database checkpoint id is
//     3) check segment flushed
// 11. Restart new db instance and check again
func TestSnapshot8(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	// 1. Create a db instance and a database
	inst, gen, database := initTestDB1(t)
	defer inst.Close()
	// shardId := database.GetShardId()
	// idxGen := gen.Shard(shardId)

	// 2. Disable flush segment
	err := inst.Scheduler.ExecCmd(sched.TurnOffFlushSegmentCmd)
	assert.Nil(t, err)

	// 3. Create 2 tables
	schemas := make([]*metadata.Schema, 2)
	var createCtx *CreateTableCtx
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx = &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	// 4. Append rows to 1-1
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * 35 / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	// 5. Append rows to 1-2
	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx = CreateAppendCtx(database, gen, schemas[1].Name, ck1)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	{
		t1 := database.SimpleGetTableByName(schemas[1].Name)
		data1, _ := inst.GetTableData(t1)
		defer data1.Unref()
		assert.Equal(t, vector.Length(ck1.Vecs[0]), int(data1.GetRowCount()))
	}

	// 6. Create snapshot
	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: true,
	}
	index, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, 0, database.UncheckpointedCnt())
	assert.Equal(t, database.GetCheckpointId(), index)

	// 12. Create another db instance
	aoedb2, gen2, _ := initTestDBWithOptions(t, "aoedb2", database.Name, defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)

	// 13. Apply snapshot
	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = aoedb2.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)
	t.Log(aoedb2.Store.Catalog.PString(metadata.PPL0, 0))
	db2, err := aoedb2.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)
	sorted := 0
	processor := new(metadata.LoopProcessor)
	processor.SegmentFn = func(segment *metadata.Segment) error {
		segment.RLock()
		defer segment.RUnlock()
		if segment.IsSortedLocked() {
			sorted++
		}
		return nil
	}
	testutils.WaitExpect(200, func() bool {
		sorted = 0
		db2.RLock()
		defer db2.RUnlock()
		err := db2.RecurLoopLocked(processor)
		assert.Nil(t, err)
		return sorted == 2
	})
	assert.Equal(t, 2, sorted)

	gen2.Reset(db2.GetShardId(), db2.GetCheckpointId())
	appendCtx = CreateAppendCtx(db2, gen2, schemas[0].Name, ck0)
	err = aoedb2.Append(appendCtx)
	assert.Nil(t, err)

	t0 := db2.SimpleGetTableByName(schemas[0].Name)
	assert.Equal(t, 4, t0.SimpleGetSegmentCount())

	data0, _ := aoedb2.GetTableData(t0)
	defer data0.Unref()
	assert.Equal(t, 2*vector.Length(ck0.Vecs[0]), int(data0.GetRowCount()))

	t1 := db2.SimpleGetTableByName(schemas[1].Name)
	assert.Equal(t, 2, t1.SimpleGetSegmentCount())

	data1, _ := aoedb2.GetTableData(t1)
	defer data1.Unref()
	assert.Equal(t, vector.Length(ck1.Vecs[0]), int(data1.GetRowCount()))
	t.Log(t1.PString(metadata.PPL1, 0))

	aoedb2.Close()
}

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        0     ]
// 2.  Disable upgrade segment                            [   0,        0     ]
// 3.  Create 2 tables: 1-1 [1,?], 1-2 [2,?]              [   2,        ?     ]
// 4.  Append 35/10 (MaxBlockRows) rows into (1-1)        [   3,        2     ]
// 5.  Append 35/10 (MaxBlockRows) rows into (1-2)        [   4,        2     ]
// 6.  FlushTable (1-1) (1-2)                             [   -,        ?     ]
// 7.  Create snapshot, the snapshot index should be [4]  [   -,        4     ]
// 8.  Create another db instance
// 9.  Apply previous created snapshot
// 10. Check:
//     1) database exists 2) database checkpoint id is
//     3) check segment flushed
// 11. Restart new db instance and check again
func TestSnapshot9(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	// 1. Create a db instance and a database
	inst, gen, database := initTestDB1(t)
	defer inst.Close()
	// shardId := database.GetShardId()
	// idxGen := gen.Shard(shardId)

	// 2. Disable upgrade segment
	err := inst.Scheduler.ExecCmd(sched.TurnOffUpgradeSegmentMetaCmd)
	assert.Nil(t, err)

	// 3. Create 2 tables
	schemas := make([]*metadata.Schema, 2)
	var createCtx *CreateTableCtx
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx = &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	// 4. Append rows to 1-1
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * 35 / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	// 5. Append rows to 1-2
	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx = CreateAppendCtx(database, gen, schemas[1].Name, ck1)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	{
		t1 := database.SimpleGetTableByName(schemas[1].Name)
		data1, _ := inst.GetTableData(t1)
		defer data1.Unref()
		assert.Equal(t, vector.Length(ck1.Vecs[0]), int(data1.GetRowCount()))
	}

	// 6. Create snapshot
	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: true,
	}
	index, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, 0, database.UncheckpointedCnt())
	assert.Equal(t, database.GetCheckpointId(), index)

	// 12. Create another db instance
	aoedb2, gen2, _ := initTestDBWithOptions(t, "aoedb2", database.Name, defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)

	// 13. Apply snapshot
	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = aoedb2.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)
	t.Log(aoedb2.Store.Catalog.PString(metadata.PPL1, 0))
	db2, err := aoedb2.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)
	sorted := 0
	processor := new(metadata.LoopProcessor)
	processor.SegmentFn = func(segment *metadata.Segment) error {
		segment.RLock()
		defer segment.RUnlock()
		if segment.IsSortedLocked() {
			sorted++
		}
		return nil
	}
	testutils.WaitExpect(200, func() bool {
		sorted = 0
		db2.RLock()
		defer db2.RUnlock()
		err := db2.RecurLoopLocked(processor)
		assert.Nil(t, err)
		return sorted == 2
	})
	assert.Equal(t, 2, sorted)

	gen2.Reset(db2.GetShardId(), db2.GetCheckpointId())
	appendCtx = CreateAppendCtx(db2, gen2, schemas[0].Name, ck0)
	err = aoedb2.Append(appendCtx)
	assert.Nil(t, err)

	t0 := db2.SimpleGetTableByName(schemas[0].Name)
	assert.Equal(t, 4, t0.SimpleGetSegmentCount())

	data0, _ := aoedb2.GetTableData(t0)
	defer data0.Unref()
	assert.Equal(t, 2*vector.Length(ck0.Vecs[0]), int(data0.GetRowCount()))

	t1 := db2.SimpleGetTableByName(schemas[1].Name)
	assert.Equal(t, 2, t1.SimpleGetSegmentCount())

	data1, _ := aoedb2.GetTableData(t1)
	defer data1.Unref()
	assert.Equal(t, vector.Length(ck1.Vecs[0]), int(data1.GetRowCount()))
	t.Log(t1.PString(metadata.PPL1, 0))

	aoedb2.Close()
}

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        0     ]
// 2.  Disable upgrade segment                            [   0,        0     ]
// 3.  Create 2 tables: 1-1 [1,?], 1-2 [2,?]              [   2,        ?     ]
// 4.  Append 35/10 (MaxBlockRows) rows into (1-1)        [   3,        2     ]
// 5.  Append 35/10 (MaxBlockRows) rows into (1-2)        [   4,        2     ]
// 6.  FlushTable (1-1) (1-2)                             [   -,        ?     ]
// 7.  Create snapshot, the snapshot index should be [4]  [   -,        4     ]
// 8.  Create another db instance
// 9.  Apply previous created snapshot
// 10. Check:
//     1) database exists 2) database checkpoint id is
//     3) check segment flushed
// 11. Restart new db instance and check again
func TestSnapshot10(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	// 1. Create a db instance and a database
	inst, gen, database := initTestDB1(t)
	defer inst.Close()

	// 2. Create 2 tables
	schemas := make([]*metadata.Schema, 2)
	var createCtx *CreateTableCtx
	for i := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx = &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	// 3. Append rows to 1-1
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * 35 / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	// 4. Append rows to 1-2
	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx = CreateAppendCtx(database, gen, schemas[1].Name, ck1)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	{
		t1 := database.SimpleGetTableByName(schemas[1].Name)
		data1, _ := inst.GetTableData(t1)
		defer data1.Unref()
		assert.Equal(t, vector.Length(ck1.Vecs[0]), int(data1.GetRowCount()))
	}

	// 5. Create snapshot
	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: getSnapshotPath(defaultSnapshotPath, t),
		Sync: true,
	}
	index, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, 0, database.UncheckpointedCnt())
	assert.Equal(t, database.GetCheckpointId(), index)

	// 6. Create another db instance
	aoedb2, gen2, _ := initTestDBWithOptions(t, "aoedb2", "", defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)

	// 13. Apply snapshot
	applySSCtx := &ApplySnapshotCtx{
		DB:   createSSCtx.DB,
		Path: createSSCtx.Path,
	}
	err = aoedb2.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)
	db2, err := aoedb2.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)
	sorted := 0
	processor := new(metadata.LoopProcessor)
	processor.SegmentFn = func(segment *metadata.Segment) error {
		segment.RLock()
		defer segment.RUnlock()
		if segment.IsSortedLocked() {
			sorted++
		}
		return nil
	}
	testutils.WaitExpect(200, func() bool {
		sorted = 0
		db2.RLock()
		defer db2.RUnlock()
		err := db2.RecurLoopLocked(processor)
		assert.Nil(t, err)
		return sorted == 2
	})
	assert.Equal(t, 2, sorted)

	gen2.Reset(db2.GetShardId(), db2.GetCheckpointId())
	appendCtx = CreateAppendCtx(db2, gen2, schemas[0].Name, ck0)
	err = aoedb2.Append(appendCtx)
	assert.Nil(t, err)

	t0 := db2.SimpleGetTableByName(schemas[0].Name)
	assert.Equal(t, 4, t0.SimpleGetSegmentCount())

	data0, _ := aoedb2.GetTableData(t0)
	defer data0.Unref()
	assert.Equal(t, 2*vector.Length(ck0.Vecs[0]), int(data0.GetRowCount()))

	t1 := db2.SimpleGetTableByName(schemas[1].Name)
	assert.Equal(t, 2, t1.SimpleGetSegmentCount())

	data1, _ := aoedb2.GetTableData(t1)
	defer data1.Unref()
	assert.Equal(t, vector.Length(ck1.Vecs[0]), int(data1.GetRowCount()))
	t.Log(t1.PString(metadata.PPL1, 0))

	aoedb2.Close()
	aoedb2, _, _ = initTestDBWithOptions(t, "aoedb2", "", defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)
	aoedb2.Close()
}
