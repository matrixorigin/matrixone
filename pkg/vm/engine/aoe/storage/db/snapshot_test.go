package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	defaultSnapshotPath = "snapshot"
)

func getTestPath(t *testing.T) string {
	return testutils.GetDefaultTestPath("DB", t)
}

func initTestEnv(t *testing.T) string {
	testutils.RemoveDefaultTestPath("DB", t)
	return testutils.MakeDefaultTestPath("DB", t)
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

func TestSnapshot1(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	inst, gen, database := initTestDB1(t)
	shardId := database.GetShardId()
	idxGen := gen.Shard(shardId)
	schemas := make([]*metadata.Schema, 10)
	for i, _ := range schemas {
		schema := metadata.MockSchema(i + 1)
		_, err := inst.CreateTable(database.Name, schema, idxGen.Next())
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t), false)
	assert.Nil(t, err)
	assert.Equal(t, idxGen.Get(), idx)

	err = inst.ApplySnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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
	shardId := database.GetShardId()
	idxGen := gen.Shard(shardId)
	schemas := make([]*metadata.Schema, 3)
	for i, _ := range schemas {
		schema := metadata.MockSchema(i + 1)
		_, err := inst.CreateTable(database.Name, schema, idxGen.Next())
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())
	ckId := idxGen.Get()

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := dbi.AppendCtx{
		OpIndex:   gen.Alloc(database.GetShardId()),
		OpSize:    1,
		TableName: schemas[0].Name,
		DBName:    database.Name,
		Data:      ck0,
	}
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	schema := metadata.MockSchema(2)
	_, err = inst.CreateTable(database.Name, schema, idxGen.Next())

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t), false)
	assert.Nil(t, err)
	assert.Equal(t, ckId, idx)

	err = inst.ApplySnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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
	shardId := database.GetShardId()
	idxGen := gen.Shard(shardId)
	schemas := make([]*metadata.Schema, 2)
	for i, _ := range schemas {
		schema := metadata.MockSchema(i + 1)
		_, err := inst.CreateTable(database.Name, schema, idxGen.Next())
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())
	ckId := idxGen.Get()

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := dbi.AppendCtx{
		OpIndex:   gen.Alloc(database.GetShardId()),
		OpSize:    1,
		TableName: schemas[0].Name,
		DBName:    database.Name,
		Data:      ck0,
	}
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	schema := metadata.MockSchema(2)
	_, err = inst.CreateTable(database.Name, schema, idxGen.Next())

	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx.OpIndex = gen.Alloc(database.GetShardId())
	appendCtx.TableName = schemas[1].Name
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

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t), false)
	assert.Nil(t, err)
	assert.Equal(t, ckId, idx)

	err = inst.ApplySnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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
	shardId := database.GetShardId()
	idxGen := gen.Shard(shardId)
	schemas := make([]*metadata.Schema, 3)
	for i, _ := range schemas {
		schema := metadata.MockSchema(i + 1)
		_, err := inst.CreateTable(database.Name, schema, idxGen.Next())
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())
	ckId := idxGen.Get()

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := dbi.AppendCtx{
		OpIndex:   gen.Alloc(database.GetShardId()),
		OpSize:    1,
		TableName: schemas[0].Name,
		DBName:    database.Name,
		Data:      ck0,
	}
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx.OpIndex = gen.Alloc(database.GetShardId())
	appendCtx.TableName = schemas[1].Name
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

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t), false)
	assert.Nil(t, err)
	assert.Equal(t, ckId, idx)

	err = inst.ApplySnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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

func TestSnapshot5(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	inst, gen, database := initTestDB1(t)
	shardId := database.GetShardId()
	idxGen := gen.Shard(shardId)
	schemas := make([]*metadata.Schema, 3)
	for i, _ := range schemas {
		schema := metadata.MockSchema(i + 1)
		_, err := inst.CreateTable(database.Name, schema, idxGen.Next())
		assert.Nil(t, err)
		schemas[i] = schema
	}

	testutils.WaitExpect(200, func() bool {
		return idxGen.Get() == database.GetCheckpointId()
	})
	assert.Equal(t, idxGen.Get(), database.GetCheckpointId())

	names := inst.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := dbi.AppendCtx{
		OpIndex:   gen.Alloc(database.GetShardId()),
		OpSize:    1,
		TableName: schemas[0].Name,
		DBName:    database.Name,
		Data:      ck0,
	}
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx.OpIndex = gen.Alloc(database.GetShardId())
	appendCtx.TableName = schemas[1].Name
	appendCtx.Data = ck1
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	err = inst.FlushTable(database.Name, schemas[0].Name)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 1
	})
	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 1, stats[0].Count)
	ckId := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, uint64(4), ckId)

	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t), false)
	assert.Nil(t, err)
	assert.Equal(t, ckId, idx)

	err = inst.ApplySnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
	assert.Nil(t, err)

	idx2 := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx2)
	time.Sleep(time.Duration(500) * time.Millisecond)
	testutils.WaitExpect(200, func() bool {
		return database.GetCheckpointId() == gen.Get(database.GetShardId())
	})
	assert.Equal(t, database.GetCheckpointId(), gen.Get(database.GetShardId()))

	inst.Close()
	inst2, _, _ := initTestDB3(t)
	defer inst2.Close()

	dbReplayed, err := inst2.Store.Catalog.SimpleGetDatabase(database.Id)
	assert.Nil(t, err)
	assert.True(t, dbReplayed.IsReplaced())
	assert.Equal(t, dbReplayed.GetCheckpointId(), dbReplayed.GetCommit().GetIndex())

	gcreq := gcreqs.NewCatalogCompactionRequest(inst.Store.Catalog, time.Duration(1)*time.Millisecond)
	err = gcreq.Execute()
	assert.Nil(t, err)
	testutils.WaitExpect(500, func() bool {
		_, err := inst2.Store.Catalog.SimpleGetDatabase(database.Id)
		return err != nil
	})
	_, err = inst2.Store.Catalog.SimpleGetDatabase(database.Id)
	assert.NotNil(t, err)

	idx3 := inst2.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx3)

	names = inst2.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	t.Log(inst2.Store.Catalog.IndexWal.String())
}

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
	shardId := database.GetShardId()
	idxGen := gen.Shard(shardId)

	// 2. Create 3 tables
	schemas := make([]*metadata.Schema, 3)
	for i, _ := range schemas {
		schema := metadata.MockSchema(i + 1)
		_, err := inst.CreateTable(database.Name, schema, idxGen.Next())
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
	appendCtx := dbi.AppendCtx{
		OpIndex:   gen.Alloc(database.GetShardId()),
		OpSize:    1,
		TableName: schemas[0].Name,
		DBName:    database.Name,
		Data:      ck0,
	}
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	// 5. Append rows into table 1-2
	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx.OpIndex = gen.Alloc(database.GetShardId())
	appendCtx.TableName = schemas[1].Name
	appendCtx.Data = ck1
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	// 6. FlushTable tables
	err = inst.FlushTable(database.Name, schemas[0].Name)
	assert.Nil(t, err)
	err = inst.FlushTable(database.Name, schemas[1].Name)

	// 7. Wait checkpointed
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 0
	})
	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 0, stats[0].Count)
	ckId := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, gen.Get(database.GetShardId()), ckId)

	// 8. Append rows into table 1-1
	appendCtx.OpIndex = gen.Alloc(database.GetShardId())
	appendCtx.TableName = schemas[0].Name
	appendCtx.Data = ck0
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	// 9. Append rows into table 1-2
	appendCtx.OpIndex = gen.Alloc(database.GetShardId())
	appendCtx.TableName = schemas[1].Name
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
	assert.Equal(t, ckId, database.GetCheckpointId())

	// 11. Create snapshot
	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t), false)
	assert.Nil(t, err)
	assert.Equal(t, ckId, idx)

	// 12. Create another db instance
	aoedb2, gen2, origindb := initTestBrokerDB(t, "aoedb2", database.Name, nil)

	// 13. Apply snapshot
	err = aoedb2.ApplySnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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
	aoedb2, _, _ = initTestBrokerDB(t, "aoedb2", "", nil)
	defer aoedb2.Close()

	dbReplayed, err := aoedb2.Store.Catalog.SimpleGetDatabase(origindb.Id)
	assert.Nil(t, err)
	assert.True(t, dbReplayed.IsReplaced())
	assert.Equal(t, dbReplayed.GetCheckpointId(), dbReplayed.GetCommit().GetIndex())

	gcreq := gcreqs.NewCatalogCompactionRequest(aoedb2.Store.Catalog, time.Duration(1)*time.Millisecond)
	err = gcreq.Execute()
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
