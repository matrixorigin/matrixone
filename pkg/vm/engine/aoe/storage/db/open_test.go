package db

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"os"
	"strconv"

	"github.com/stretchr/testify/assert"

	// bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	// dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	// "matrixone/pkg/vm/engine/aoe/storage/layout"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/table"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	// "matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	// "matrixone/pkg/vm/engine/aoe/storage/mock/type/vector"
	// mops "matrixone/pkg/vm/engine/aoe/storage/ops/meta"
	// w "matrixone/pkg/vm/engine/aoe/storage/worker"
	// "runtime"
	// "sync"
	"testing"
	// "time"
)

var (
	TEST_DB_DIR = "/tmp/open_test"
)

func init() {
	os.RemoveAll(TEST_DB_DIR)
}

func TestLoadMetaInfo(t *testing.T) {
	cfg := &md.Configuration{
		Dir:              TEST_DB_DIR,
		SegmentMaxBlocks: 10,
		BlockMaxRows:     10,
	}
	info := loadMetaInfo(cfg)
	assert.Equal(t, uint64(0), info.CheckPoint)
	assert.Equal(t, uint64(0), info.Sequence.NextBlockID)
	assert.Equal(t, uint64(0), info.Sequence.NextSegmentID)
	assert.Equal(t, uint64(0), info.Sequence.NextTableID)

	tbl, err := info.CreateTable()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), info.Sequence.NextTableID)

	err = info.RegisterTable(tbl)
	assert.Nil(t, err)

	info.CheckPoint++

	filename := e.MakeFilename(cfg.Dir, e.FTCheckpoint, strconv.Itoa(int(info.CheckPoint)), false)

	w, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	defer w.Close()
	err = info.Serialize(w)
	assert.Nil(t, err)

	tbl, err = info.CreateTable()
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), info.Sequence.NextTableID)
	err = info.RegisterTable(tbl)
	assert.Nil(t, err)
	info.CheckPoint++

	filename = e.MakeFilename(cfg.Dir, e.FTCheckpoint, strconv.Itoa(int(info.CheckPoint)), false)

	w, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	defer w.Close()
	err = info.Serialize(w)
	assert.Nil(t, err)

	info2 := loadMetaInfo(cfg)
	assert.NotNil(t, info2)
	assert.Equal(t, info.CheckPoint, info2.CheckPoint)
	assert.Equal(t, info.Sequence.NextTableID, info2.Sequence.NextTableID)
	assert.Equal(t, info.Tables, info2.Tables)
}
