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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/internal/invariants"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/stretchr/testify/assert"
)

var (
	defaultSnapshotPath      = "snapshot"
	moduleName               = "AOEDB"
	defaultDBPath            = "aoedb"
	defaultDBName            = "default"
	emptyDBName              = ""
	defaultTestBlockRows     = uint64(2000)
	defaultTestSegmentBlocks = uint64(2)
)

func getTestPath(t *testing.T) string {
	return testutils.GetDefaultTestPath(moduleName, t)
}

func initTestEnv(t *testing.T) string {
	testutils.RemoveDefaultTestPath(moduleName, t)
	return testutils.MakeDefaultTestPath(moduleName, t)
}

func getSnapshotPath(dir string, t *testing.T) string {
	workdir := getTestPath(t)
	return filepath.Join(workdir, dir)
}

func prepareSnapshotPath(dir string, t *testing.T) string {
	path := getSnapshotPath(dir, t)
	os.MkdirAll(path, os.FileMode(0755))
	return path
}

func initTestDB1(t *testing.T) (*DB, *shard.MockIndexAllocator, *metadata.Database) {
	return initTestDBWithOptions(t, defaultDBPath, defaultDBName, defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)
}

func initTestDB2(t *testing.T) (*DB, *shard.MockIndexAllocator, *metadata.Database) {
	return initTestDBWithOptions(t, defaultDBPath, emptyDBName, defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.BrokerRole)
}

func initTestDB3(t *testing.T) (*DB, *shard.MockIndexAllocator, *metadata.Database) {
	return initTestDBWithOptions(t, defaultDBPath, defaultDBName, defaultTestBlockRows, defaultTestSegmentBlocks, nil, wal.HolderRole)
}

func initTestDBWithOptions(t *testing.T, dir, dbName string, blockRows, segBlocks uint64, cleanCfg *storage.MetaCleanerCfg, walRole wal.Role) (*DB, *shard.MockIndexAllocator, *metadata.Database) {
	opts := new(storage.Options)
	opts.MetaCleanerCfg = cleanCfg
	opts.WalRole = walRole
	path := filepath.Join(getTestPath(t), dir)
	config.NewCustomizedMetaOptions(path, config.CST_Customize, blockRows, segBlocks, opts)
	inst, _ := Open(path, opts)
	gen := shard.NewMockIndexAllocator()
	var database *metadata.Database
	if dbName != "" {
		ctx := &CreateDBCtx{
			DB: dbName,
		}
		database, _ = inst.CreateDatabase(ctx)
	}
	return inst, gen, database
}

func CreateDBMutationCtx(database *metadata.Database, gen *shard.MockIndexAllocator) *DBMutationCtx {
	ctx := &DBMutationCtx{
		DB:     database.Name,
		Id:     gen.Alloc(database.GetShardId()),
		Offset: 0,
		Size:   1,
	}
	return ctx
}

func CreateTableMutationCtx(database *metadata.Database, gen *shard.MockIndexAllocator, name string) *TableMutationCtx {
	return &TableMutationCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Table:         name,
	}
}

func CreateAppendCtx(database *metadata.Database, gen *shard.MockIndexAllocator, name string, data *batch.Batch) *AppendCtx {
	return &AppendCtx{
		TableMutationCtx: *CreateTableMutationCtx(database, gen, name),
		Data:             data,
	}
}

// func CreateDBMutationCtx(database *metadata.Database, gen *shard.MockIndexAllocator) *DBMutationCtx {
// 	ctx := &DBMutationCtx{
// 		DB:     database.Name,
// 		Id:     gen.Alloc(database.GetShardId()),
// 		Offset: 0,
// 		Size:   1,
// 	}
// 	return ctx
// }

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1. Create 2 tables: 1-1, 1-2                           [  2,         ?     ]
// 2. Append 1/10 (MaxBlockRows) rows into (1-2)          [  3,         ?     ]
// 3. Drop table 1-1                                      [  4,         ?     ]
// 4. Wait checkpoint                                     [  -,         2     ]
// 5. Compact, 1-1 should not be marked as hard delete    [  -,         2     ]
func TestReplay1(t *testing.T) {
	initTestEnv(t)
	inst1, gen, database := initTestDB1(t)
	schema1 := metadata.MockSchema(2)
	schema2 := metadata.MockSchema(3)
	createTableCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema1,
	}
	_, err := inst1.CreateTable(createTableCtx)
	assert.Nil(t, err)
	createTableCtx.Schema = schema2
	createTableCtx.DBMutationCtx = *CreateDBMutationCtx(database, gen)
	_, err = inst1.CreateTable(createTableCtx)
	assert.Nil(t, err)
	ckId := gen.Get(database.GetShardId())

	rows := inst1.Store.Catalog.Cfg.BlockMaxRows / 10
	ck2 := mock.MockBatch(schema2.Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schema2.Name, ck2)
	err = inst1.Append(appendCtx)
	assert.Nil(t, err)

	t1, err := inst1.Store.Catalog.SimpleGetTableByName(database.Name, schema1.Name)
	assert.Nil(t, err)

	dropCtx := CreateTableMutationCtx(database, gen, schema1.Name)
	_, err = inst1.DropTable(dropCtx)
	dropIdx := dropCtx.Id

	testutils.WaitExpect(200, func() bool {
		return database.UncheckpointedCnt() == 1 && t1.IsHardDeleted()
	})
	assert.Equal(t, 1, database.UncheckpointedCnt())
	assert.True(t, t1.IsHardDeleted())
	assert.Equal(t, ckId, database.GetCheckpointId())
	t.Log(inst1.Store.Catalog.PString(metadata.PPL0, 0))

	err = inst1.ForceCompactCatalog()
	assert.Nil(t, err)

	inst1.Close()

	inst, _, _ := initTestDB2(t)
	defer inst.Close()
	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))

	db2, err := inst.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)
	assert.Equal(t, ckId, db2.GetCheckpointId())
	t2Replayed := db2.GetTable(t1.Id)
	assert.True(t, t2Replayed.IsHardDeleted())
	t.Log(db2.GetIdempotentIndex().String())
	assert.Equal(t, dropIdx, db2.GetIdempotentIndex().Id.Id)

	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	_, err = inst.DropTable(dropCtx)
	assert.Equal(t, db.ErrIdempotence, err)

	testutils.WaitExpect(200, func() bool {
		return db2.UncheckpointedCnt() == 1
	})
	assert.Equal(t, 1, db2.UncheckpointedCnt())

	err = inst.FlushDatabase(db2.Name)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return db2.UncheckpointedCnt() == 0
	})
	assert.Equal(t, 0, db2.UncheckpointedCnt())
	assert.Equal(t, gen.Get(db2.GetShardId()), db2.GetCheckpointId())
}

func TestReplay2(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	schema := metadata.MockSchema(2)
	shardId := database.GetShardId()
	createTableCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	meta, err := inst.CreateTable(createTableCtx)
	assert.Nil(t, err)

	irows := inst.Store.Catalog.Cfg.BlockMaxRows / 2
	ibat := mock.MockBatch(meta.Schema.Types(), irows)

	insertFn := func() {
		appendCtx := CreateAppendCtx(database, gen, schema.Name, ibat)
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	rel, err := inst.Relation(database.Name, schema.Name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*2, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*3, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return gen.Get(shardId) == database.GetCheckpointId()
	})
	time.Sleep(time.Duration(50) * time.Millisecond)

	rel.Close()
	inst.Close()

	inst, _, _ = initTestDB2(t)

	t.Log(inst.Store.Catalog.PString(metadata.PPL1, 0))
	segmentedIdx := inst.GetShardCheckpointId(shardId)
	assert.Equal(t, gen.Get(shardId), segmentedIdx)

	meta, err = inst.Opts.Meta.Catalog.SimpleGetTableByName(database.Name, schema.Name)
	assert.Nil(t, err)

	rel, err = inst.Relation(database.Name, meta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, int(irows*3), int(rel.Rows()))
	t.Log(rel.Rows())

	insertFn()
	t.Log(rel.Rows())
	insertFn()
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(rel.Rows())
	t.Log(inst.MutationBufMgr.String())

	err = inst.FlushTable(database.Name, meta.Schema.Name)
	assert.Nil(t, err)
	insertFn()
	t.Log(rel.Rows())
	insertFn()
	t.Log(rel.Rows())

	rel.Close()
	inst.Close()
}

func TestReplay3(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	schema := metadata.MockSchema(2)
	shardId := database.GetShardId()
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	meta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)

	rel, err := inst.Relation(database.Name, meta.Schema.Name)
	assert.Nil(t, err)

	irows := inst.Store.Catalog.Cfg.BlockMaxRows / 2
	ibat := mock.MockBatch(meta.Schema.Types(), irows)

	appendCtx := new(AppendCtx)
	insertFn := func() {
		appendCtx = CreateAppendCtx(database, gen, schema.Name, ibat)
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	insertFn()
	assert.Equal(t, irows, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*2, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	ibat2 := mock.MockBatch(meta.Schema.Types(), inst.Store.Catalog.Cfg.BlockMaxRows)
	insertFn2 := func() {
		appendCtx.Id = gen.Alloc(shardId)
		appendCtx.Offset = 0
		appendCtx.Size = 2
		appendCtx.Data = ibat
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
		appendCtx.Offset = 1
		appendCtx.Size = 2
		appendCtx.Data = ibat2
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	insertFn2()
	assert.Equal(t, irows*5, uint64(rel.Rows()))
	t.Log(rel.Rows())
	testutils.WaitExpect(200, func() bool {
		return gen.Get(shardId)-1 == database.GetCheckpointId()
	})
	assert.Equal(t, gen.Get(shardId)-1, database.GetCheckpointId())
	time.Sleep(time.Duration(80) * time.Millisecond)

	rel.Close()
	inst.Close()

	inst, _, _ = initTestDB2(t)
	replayDatabase, err := inst.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)

	assert.Equal(t, database.GetSize(), replayDatabase.GetSize())
	assert.Equal(t, database.GetCount(), replayDatabase.GetCount())

	segmentedIdx := inst.GetShardCheckpointId(shardId)
	assert.Equal(t, gen.Get(shardId)-1, segmentedIdx)

	rel, err = inst.Relation(database.Name, meta.Schema.Name)
	t.Log(rel.Rows())
	assert.Nil(t, err)
	assert.Equal(t, int64(irows*4), rel.Rows())

	insertFn3 := func() {
		appendCtx.Id = segmentedIdx + 1
		appendCtx.Offset = 0
		appendCtx.Size = 2
		appendCtx.Data = ibat
		err = inst.Append(appendCtx)
		assert.Equal(t, db.ErrIdempotence, err)
		appendCtx.Id = segmentedIdx + 1
		appendCtx.Offset = 1
		appendCtx.Size = 2
		appendCtx.Data = ibat2
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	insertFn3()
	t.Log(rel.Rows())
	insertFn()
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(rel.Rows())
	t.Log(inst.MutationBufMgr.String())

	err = inst.FlushTable(meta.Database.Name, meta.Schema.Name)
	assert.Nil(t, err)
	insertFn()
	t.Log(rel.Rows())
	insertFn()
	t.Log(rel.Rows())

	rel.Close()
	inst.Close()
}

func TestReplay4(t *testing.T) {
	waitTime := time.Duration(20) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime = time.Duration(100) * time.Millisecond
	}
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	schema := metadata.MockSchema(2)
	shardId := database.GetShardId()
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	blkCnt := 2
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * uint64(blkCnt)
	ck := mock.MockBatch(tblMeta.Schema.Types(), rows)
	assert.Equal(t, uint64(rows), uint64(ck.Vecs[0].Length()))
	insertCnt := 4
	appendCtx := new(AppendCtx)
	for i := 0; i < insertCnt; i++ {
		appendCtx = CreateAppendCtx(database, gen, schema.Name, ck)
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}
	time.Sleep(waitTime)
	if invariants.RaceEnabled {
		time.Sleep(waitTime)
	}
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())

	tbl, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return uint64(insertCnt) == inst.GetShardCheckpointId(shardId)
	})
	segmentedIdx := inst.GetShardCheckpointId(shardId)
	t.Logf("SegmentedIdx: %d", segmentedIdx)
	assert.Equal(t, uint64(insertCnt), segmentedIdx)

	t.Logf("Row count: %d", tbl.GetRowCount())
	assert.Equal(t, rows*uint64(insertCnt), tbl.GetRowCount())

	t.Log(tbl.GetMeta().PString(metadata.PPL2, 0))
	inst.Close()

	dataDir := common.MakeDataDir(inst.Dir)
	invalidFileName := filepath.Join(dataDir, "invalid")
	f, err := os.Create(invalidFileName)
	assert.Nil(t, err)
	f.Close()

	inst, _, _ = initTestDB2(t)

	os.Stat(invalidFileName)
	_, err = os.Stat(invalidFileName)
	assert.True(t, os.IsNotExist(err))

	// t.Log(inst.MTBufMgr.String())
	// t.Log(inst.SSTBufMgr.String())

	replaytblMeta, err := inst.Opts.Meta.Catalog.SimpleGetTableByName(database.Name, schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, tblMeta.Schema.Name, replaytblMeta.Schema.Name)

	tbl, err = inst.Store.DataTables.WeakRefTable(replaytblMeta.Id)
	assert.Nil(t, err)
	t.Logf("Row count: %d, %d", tbl.GetRowCount(), rows*uint64(insertCnt))
	assert.Equal(t, rows*uint64(insertCnt)-tblMeta.Schema.BlockMaxRows, tbl.GetRowCount())

	replayIndex := tbl.GetMeta().MaxLogIndex()
	assert.Equal(t, tblMeta.Schema.BlockMaxRows, replayIndex.Count)
	assert.False(t, replayIndex.IsApplied())

	for i := int(segmentedIdx) + 1; i < int(segmentedIdx)+1+insertCnt; i++ {
		appendCtx.Id = uint64(i)
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}
	createCtx.Id = segmentedIdx + 1 + uint64(insertCnt)
	_, err = inst.CreateTable(createCtx)
	assert.NotNil(t, err)

	testutils.WaitExpect(200, func() bool {
		return 2*rows*uint64(insertCnt)-2*tblMeta.Schema.BlockMaxRows == tbl.GetRowCount()
	})
	t.Logf("Row count: %d", tbl.GetRowCount())
	assert.Equal(t, 2*rows*uint64(insertCnt)-2*tblMeta.Schema.BlockMaxRows, tbl.GetRowCount())

	preSegmentedIdx := segmentedIdx

	testutils.WaitExpect(200, func() bool {
		return preSegmentedIdx+uint64(insertCnt)-1 == inst.GetShardCheckpointId(shardId)
	})

	segmentedIdx = inst.GetShardCheckpointId(shardId)
	t.Logf("SegmentedIdx: %d", segmentedIdx)
	assert.Equal(t, preSegmentedIdx+uint64(insertCnt)-1, segmentedIdx)

	inst.Close()
}
