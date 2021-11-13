package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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

	err = inst.Flush(database.Name, schemas[1].Name)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 1
	})
	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 1, stats[0].Count)

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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

	err = inst.Flush(database.Name, schemas[1].Name)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
		return stats[0].Count == 1
	})
	stats := inst.Store.Catalog.IndexWal.GetAllPendingEntries()
	assert.Equal(t, 1, stats[0].Count)

	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
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

	err = inst.Flush(database.Name, schemas[0].Name)
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
	idx, err := inst.CreateSnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
	assert.Nil(t, err)
	assert.Equal(t, ckId, idx)

	err = inst.ApplySnapshot(database.Name, getSnapshotPath(defaultSnapshotPath, t))
	assert.Nil(t, err)

	idx2 := inst.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx2)
	time.Sleep(time.Duration(500) * time.Millisecond)
	t.Log(inst.Store.Catalog.IndexWal.String())
	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	inst.Close()
	inst2, _, _ := initTestDB3(t)
	defer inst2.Close()
	idx3 := inst2.GetDBCheckpointId(database.Name)
	assert.Equal(t, idx, idx3)

	names = inst2.TableNames(database.Name)
	assert.Equal(t, len(schemas), len(names))

	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	t.Log(inst2.Store.Catalog.IndexWal.String())
}
