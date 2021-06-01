package db

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"os"
	"strconv"
	"testing"
)

var (
	TEST_DB_DIR = "/tmp/open_test"
)

func initTest() {
	os.RemoveAll(TEST_DB_DIR)
}

func TestLoadMetaInfo(t *testing.T) {
	initTest()
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

func TestCleanStaleMeta(t *testing.T) {
	initTest()
	cfg := &md.Configuration{
		Dir:              TEST_DB_DIR,
		SegmentMaxBlocks: 10,
		BlockMaxRows:     10,
	}
	dir := e.MakeMetaDir(cfg.Dir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		assert.Nil(t, err)
	}

	invalids := []string{"ds234", "234ds"}
	for _, invalid := range invalids {
		fname := e.MakeFilename(cfg.Dir, e.FTCheckpoint, invalid, false)

		f, err := os.Create(fname)
		assert.Nil(t, err)
		f.Close()

		f1 := func() {
			cleanStaleMeta(cfg.Dir)
		}
		assert.Panics(t, f1)
		err = os.Remove(fname)
		assert.Nil(t, err)
	}

	valids := []string{"1", "2", "3", "100"}
	for _, valid := range valids {
		fname := e.MakeFilename(cfg.Dir, e.FTCheckpoint, valid, false)
		f, err := os.Create(fname)
		assert.Nil(t, err)
		f.Close()
		t.Log(fname)
	}

	files, err := ioutil.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, len(valids), len(files))

	cleanStaleMeta(cfg.Dir)

	files, err = ioutil.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(files))

	fname := e.MakeFilename(cfg.Dir, e.FTCheckpoint, "100", false)
	_, err = os.Stat(fname)
	assert.Nil(t, err)
}

func TestCleanStaleData(t *testing.T) {
	// TODO
}
