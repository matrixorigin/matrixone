package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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

	// t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
	t.Log(inst2.Store.Catalog.IndexWal.String())
}
