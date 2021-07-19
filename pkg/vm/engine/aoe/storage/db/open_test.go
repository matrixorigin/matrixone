package db

import (
	"io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"path/filepath"
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
	handle := NewMetaHandle(cfg.Dir)
	info := handle.RebuildInfo(cfg)
	// info := loadMetaInfo(cfg)
	assert.Equal(t, uint64(0), info.CheckPoint)
	assert.Equal(t, uint64(0), info.Sequence.NextBlockID)
	assert.Equal(t, uint64(0), info.Sequence.NextSegmentID)
	assert.Equal(t, uint64(0), info.Sequence.NextTableID)

	schema := md.MockSchema(2)
	schema.Name = "mock1"
	tbl, err := info.CreateTable(md.NextGloablSeqnum(), schema)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), info.Sequence.NextTableID)

	err = info.RegisterTable(tbl)
	assert.Nil(t, err)

	filename := e.MakeTableCkpFileName(cfg.Dir, tbl.GetFileName(), tbl.GetID(), false)
	w, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	defer w.Close()
	err = tbl.Serialize(w)
	assert.Nil(t, err)

	info.CheckPoint++

	filename = e.MakeInfoCkpFileName(cfg.Dir, info.GetFileName(), false)

	w, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	err = info.Serialize(w)
	assert.Nil(t, err)
	schema2 := md.MockSchema(2)
	schema2.Name = "mock2"
	tbl, err = info.CreateTable(md.NextGloablSeqnum(), schema2)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), info.Sequence.NextTableID)
	err = info.RegisterTable(tbl)
	assert.Nil(t, err)

	filename = e.MakeTableCkpFileName(cfg.Dir, tbl.GetFileName(), tbl.GetID(), false)
	w, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	defer w.Close()
	err = tbl.Serialize(w)
	assert.Nil(t, err)

	info.CheckPoint++

	filename = e.MakeInfoCkpFileName(cfg.Dir, info.GetFileName(), false)
	w.Close()

	w, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	defer w.Close()
	err = info.Serialize(w)
	assert.Nil(t, err)

	handle2 := NewMetaHandle(cfg.Dir)
	info2 := handle2.RebuildInfo(cfg)
	assert.NotNil(t, info2)
	assert.Equal(t, info.CheckPoint, info2.CheckPoint)
	assert.Equal(t, info.Sequence.NextTableID, info2.Sequence.NextTableID)
	assert.Equal(t, len(info.Tables), len(info2.Tables))
	for id, sTbl := range info.Tables {
		tTbl := info2.Tables[id]
		assert.Equal(t, sTbl.ID, tTbl.ID)
		assert.Equal(t, sTbl.CheckPoint, tTbl.CheckPoint)
		assert.Equal(t, sTbl.TimeStamp, tTbl.TimeStamp)
		assert.Equal(t, sTbl.Schema, tTbl.Schema)
	}
	t.Log(info.String())
	t.Log(info2.String())
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
		fname := e.MakeInfoCkpFileName(cfg.Dir, invalid, false)

		f, err := os.Create(fname)
		assert.Nil(t, err)
		f.Close()

		f1 := func() {
			NewMetaHandle(cfg.Dir)
		}
		assert.Panics(t, f1)
		err = os.Remove(fname)
		assert.Nil(t, err)
	}

	valids := []string{"1", "2", "3", "100"}
	for _, valid := range valids {
		fname := e.MakeInfoCkpFileName(cfg.Dir, valid, false)
		f, err := os.Create(fname)
		assert.Nil(t, err)
		f.Close()
		t.Log(fname)
	}

	files, err := ioutil.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, len(valids), len(files))

	// files, err = ioutil.ReadDir(dir)
	// assert.Nil(t, err)
	// assert.Equal(t, 1, len(files))

	fname := e.MakeInfoCkpFileName(cfg.Dir, "100", false)
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
	inst, err := Open(TEST_OPEN_DIR, opts)
	assert.Nil(t, err)
	assert.NotNil(t, inst)
	err = inst.Close()
	assert.Nil(t, err)
}

func TestReplay(t *testing.T) {
	initDBTest()
	inst := initDB()
	tableInfo := md.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mocktbl"})
	assert.Nil(t, err)
	tblMeta, err := inst.Opts.Meta.Info.ReferenceTable(tid)
	assert.Nil(t, err)
	blkCnt := 2
	rows := inst.Store.MetaInfo.Conf.BlockMaxRows * uint64(blkCnt)
	ck := chunk.MockBatch(tblMeta.Schema.Types(), rows)
	assert.Equal(t, uint64(rows), uint64(ck.Vecs[0].Length()))
	insertCnt := 4
	for i := 0; i < insertCnt; i++ {
		err = inst.Append(dbi.AppendCtx{
			OpIndex:   uint64(i),
			Data:      ck,
			TableName: tableInfo.Name,
		})
		assert.Nil(t, err)
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())

	lastTableID := inst.Opts.Meta.Info.Sequence.NextTableID
	lastSegmentID := inst.Opts.Meta.Info.Sequence.NextSegmentID
	lastBlockID := inst.Opts.Meta.Info.Sequence.NextBlockID
	lastIndexID := inst.Opts.Meta.Info.Sequence.NextIndexID
	tbl, err := inst.Store.DataTables.WeakRefTable(tblMeta.ID)
	assert.Nil(t, err)
	t.Logf("Row count: %d", tbl.GetRowCount())

	inst.Close()

	dataDir := e.MakeDataDir(inst.Dir)
	invalidFileName := filepath.Join(dataDir, "invalid")
	f, err := os.OpenFile(invalidFileName, os.O_RDONLY|os.O_CREATE, 0666)
	assert.Nil(t, err)
	f.Close()

	inst = initDB()

	os.Stat(invalidFileName)
	_, err = os.Stat(invalidFileName)
	assert.True(t, os.IsNotExist(err))

	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())

	lastTableID2 := inst.Opts.Meta.Info.Sequence.NextTableID
	lastSegmentID2 := inst.Opts.Meta.Info.Sequence.NextSegmentID
	lastBlockID2 := inst.Opts.Meta.Info.Sequence.NextBlockID
	lastIndexID2 := inst.Opts.Meta.Info.Sequence.NextIndexID
	assert.Equal(t, lastTableID, lastTableID2)
	assert.Equal(t, lastSegmentID, lastSegmentID2)
	assert.Equal(t, lastBlockID, lastBlockID2)
	assert.Equal(t, lastIndexID, lastIndexID2)

	replaytblMeta, err := inst.Opts.Meta.Info.ReferenceTableByName(tableInfo.Name)
	assert.Nil(t, err)
	assert.Equal(t, tblMeta.Schema.Name, replaytblMeta.Schema.Name)

	tbl, err = inst.Store.DataTables.WeakRefTable(replaytblMeta.ID)
	assert.Nil(t, err)
	t.Logf("Row count: %d", tbl.GetRowCount())

	_, err = inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: tableInfo.Name})
	assert.NotNil(t, err)
	for i := 0; i < insertCnt; i++ {
		err = inst.Append(dbi.AppendCtx{
			TableName: tableInfo.Name,
			Data:      ck,
			OpIndex:   uint64(i),
		})
		assert.Nil(t, err)
	}

	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	inst.Close()
}
