package db

import (
	"io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	TEST_OPEN_DIR = "/tmp/open_test"
)

func initTest() {
	os.RemoveAll(TEST_OPEN_DIR)
}

func TestLoadMetaInfo(t *testing.T) {
	initTest()
	cfg := &md.Configuration{
		Dir:              TEST_OPEN_DIR,
		SegmentMaxBlocks: 10,
		BlockMaxRows:     10,
	}
	info := loadMetaInfo(cfg)
	assert.Equal(t, uint64(0), info.CheckPoint)
	assert.Equal(t, uint64(0), info.Sequence.NextBlockID)
	assert.Equal(t, uint64(0), info.Sequence.NextSegmentID)
	assert.Equal(t, uint64(0), info.Sequence.NextTableID)

	schema := md.MockSchema(2)
	schema.Name = "mock1"
	tbl, err := info.CreateTable(schema)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), info.Sequence.NextTableID)

	err = info.RegisterTable(tbl)
	assert.Nil(t, err)

	info.CheckPoint++

	filename := e.MakeFilename(cfg.Dir, e.FTCheckpoint, strconv.Itoa(int(info.CheckPoint)), false)

	w, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	err = info.Serialize(w)
	assert.Nil(t, err)
	schema2 := md.MockSchema(2)
	schema2.Name = "mock2"
	tbl, err = info.CreateTable(schema2)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), info.Sequence.NextTableID)
	err = info.RegisterTable(tbl)
	assert.Nil(t, err)
	info.CheckPoint++

	filename = e.MakeFilename(cfg.Dir, e.FTCheckpoint, strconv.Itoa(int(info.CheckPoint)), false)
	w.Close()

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
		Dir:              TEST_OPEN_DIR,
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

func TestOpen(t *testing.T) {
	initTest()
	cfg := &md.Configuration{
		Dir:              TEST_OPEN_DIR,
		SegmentMaxBlocks: 10,
		BlockMaxRows:     10,
	}
	opts := &e.Options{}
	opts.Meta.Conf = cfg
	dbi, err := Open(TEST_OPEN_DIR, opts)
	assert.Nil(t, err)
	assert.NotNil(t, dbi)
	err = dbi.Close()
	assert.Nil(t, err)
}

func TestReplay(t *testing.T) {
	initDBTest()
	dbi := initDB()
	schema := md.MockSchema(2)
	schema.Name = "mocktbl"
	_, err := dbi.CreateTable(schema)
	assert.Nil(t, err)
	blkCnt := 2
	rows := dbi.store.MetaInfo.Conf.BlockMaxRows * uint64(blkCnt)
	ck := chunk.MockChunk(schema.Types(), rows)
	assert.Equal(t, uint64(rows), ck.GetCount())
	logIdx := &md.LogIndex{
		ID:       uint64(0),
		Capacity: ck.GetCount(),
	}
	insertCnt := 4
	for i := 0; i < insertCnt; i++ {
		err = dbi.Append(schema.Name, ck, logIdx)
		assert.Nil(t, err)
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(dbi.MTBufMgr.String())
	t.Log(dbi.SSTBufMgr.String())
	dbi.Close()

	dataDir := e.MakeDataDir(dbi.Dir)
	invalidFileName := filepath.Join(dataDir, "invalid")
	f, err := os.OpenFile(invalidFileName, os.O_RDONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	f.Close()

	dbi = initDB()

	os.Stat(invalidFileName)
	_, err = os.Stat(invalidFileName)
	assert.True(t, os.IsNotExist(err))

	t.Log(dbi.MTBufMgr.String())
	t.Log(dbi.SSTBufMgr.String())

	_, err = dbi.CreateTable(schema)
	assert.NotNil(t, err)
	for i := 0; i < insertCnt; i++ {
		err = dbi.Append(schema.Name, ck, logIdx)
		assert.Nil(t, err)
	}

	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(dbi.MTBufMgr.String())
	t.Log(dbi.SSTBufMgr.String())
	dbi.Close()
}
