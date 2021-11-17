package aoedb2

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils/config"
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
	return initTestDBWithOptions(t, defaultDBPath, defaultDBName, defaultTestBlockRows, defaultTestSegmentBlocks, nil)
}

func initTestDB2(t *testing.T) (*DB, *shard.MockIndexAllocator, *metadata.Database) {
	return initTestDBWithOptions(t, defaultDBPath, emptyDBName, defaultTestBlockRows, defaultTestSegmentBlocks, nil)
}

func initTestDBWithOptions(t *testing.T, dir, dbName string, blockRows, segBlocks uint64, cleanCfg *storage.MetaCleanerCfg) (*DB, *shard.MockIndexAllocator, *metadata.Database) {
	opts := new(storage.Options)
	opts.MetaCleanerCfg = cleanCfg
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

	gcreq := gcreqs.NewCatalogCompactionRequest(inst1.Store.Catalog, time.Duration(1)*time.Millisecond)
	err = gcreq.Execute()
	assert.Nil(t, err)

	inst1.Close()

	inst2, _, _ := initTestDB2(t)
	defer inst2.Close()
	t.Log(inst2.Store.Catalog.PString(metadata.PPL0, 0))

	db2, err := inst2.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)
	assert.Equal(t, ckId, db2.GetCheckpointId())
	t2Replayed := db2.GetTable(t1.Id)
	assert.True(t, t2Replayed.IsHardDeleted())
	t.Log(db2.GetIdempotentIndex().String())
	assert.Equal(t, dropIdx, db2.GetIdempotentIndex().Id.Id)

	err = inst2.Append(appendCtx)
	assert.Nil(t, err)

	_, err = inst2.DropTable(dropCtx)
	assert.Equal(t, db.ErrIdempotence, err)

	testutils.WaitExpect(200, func() bool {
		return db2.UncheckpointedCnt() == 1
	})
	assert.Equal(t, 1, db2.UncheckpointedCnt())

	err = inst2.FlushDatabase(db2.Name)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return db2.UncheckpointedCnt() == 0
	})
	assert.Equal(t, 0, db2.UncheckpointedCnt())
	assert.Equal(t, gen.Get(db2.GetShardId()), db2.GetCheckpointId())
}
