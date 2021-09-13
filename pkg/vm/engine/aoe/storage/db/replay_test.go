package db

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReplay1(t *testing.T) {
	initDBTest()
	// inst := initDB(engine.NORMAL_FT)
	inst := initDB(engine.MUTABLE_FT)
	tInfo := metadata.MockTableInfo(2)
	name := "mockcon"
	tid, err := inst.CreateTable(tInfo, dbi.TableOpCtx{TableName: name, OpIndex: metadata.NextGloablSeqnum()})
	assert.Nil(t, err)

	meta, err := inst.Opts.Meta.Info.ReferenceTable(tid)
	assert.Nil(t, err)
	rel, err := inst.Relation(meta.Schema.Name)
	assert.Nil(t, err)

	irows := inst.Store.MetaInfo.Conf.BlockMaxRows / 2
	ibat := chunk.MockBatch(meta.Schema.Types(), irows)

	insertFn := func() {
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   metadata.NextGloablSeqnum(),
			Data:      ibat,
			TableName: meta.Schema.Name,
		})
		assert.Nil(t, err)
	}

	insertFn()
	assert.Equal(t, irows, uint64(rel.Rows()))
	err = inst.Flush(name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*2, uint64(rel.Rows()))
	err = inst.Flush(name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*3, uint64(rel.Rows()))
	err = inst.Flush(name)
	assert.Nil(t, err)
	time.Sleep(time.Duration(10) * time.Millisecond)
	err = inst.Flush(name)
	assert.Nil(t, err)

	rel.Close()
	inst.Close()

	time.Sleep(time.Duration(20) * time.Millisecond)

	inst = initDB(engine.MUTABLE_FT)
	// inst = initDB(engine.NORMAL_FT)

	segmentedIdx, err := inst.GetSegmentedId(*dbi.NewTabletSegmentedIdCtx(meta.Schema.Name))
	assert.Nil(t, err)
	assert.Equal(t, metadata.GetGloableSeqnum(), segmentedIdx)

	rel, err = inst.Relation(meta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, irows*3, uint64(rel.Rows()))
	t.Log(rel.Rows())

	insertFn()
	t.Log(rel.Rows())
	insertFn()
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(rel.Rows())
	t.Log(inst.MutationBufMgr.String())

	err = inst.Flush(meta.Schema.Name)
	assert.Nil(t, err)
	insertFn()
	t.Log(rel.Rows())
	insertFn()
	t.Log(rel.Rows())

	time.Sleep(time.Duration(10) * time.Millisecond)
	defer rel.Close()
	defer inst.Close()
}

func mockBlkFile(id common.ID, dir string, t *testing.T) string {
	name := id.ToBlockFileName()
	fname := engine.MakeBlockFileName(dir, name, id.TableID, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	return fname
}

func mockTBlkFile(id common.ID, dir string, t *testing.T) string {
	name := id.ToBlockFileName()
	fname := engine.MakeBlockFileName(dir, name, id.TableID, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	return fname
}

func initDataAndMetaDir(dir string) {
	dataDir := engine.MakeDataDir(dir)
	os.MkdirAll(dataDir, os.ModePerm)
	metaDir := engine.MakeMetaDir(dir)
	os.MkdirAll(metaDir, os.ModePerm)
}

func TestReplay2(t *testing.T) {
	dir := "/tmp/testreplay2"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)

	mu := &sync.RWMutex{}
	colCnt := 2
	blkRowCount, segBlkCount := uint64(16), uint64(4)
	info := metadata.MockInfo(mu, blkRowCount, segBlkCount)
	info.Conf.Dir = dir
	schema := metadata.MockSchema(colCnt)
	totalBlks := segBlkCount
	tbl := metadata.MockTable(info, schema, totalBlks)
	opts := new(engine.Options)
	opts.Meta.Info = info
	opts.FillDefaults(dir)

	blkfiles := make([]string, 0)
	seg := tbl.Segments[0]
	for i := len(seg.Blocks) - 1; i >= 1; i-- {
		blk := seg.Blocks[i]
		blk.TryUpgrade()
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	ckpointer := opts.Meta.CKFactory.Create()
	err := ckpointer.PreCommit(info)
	assert.Nil(t, err)
	err = ckpointer.Commit(info)
	assert.Nil(t, err)

	ckpointer = opts.Meta.CKFactory.Create()
	err = ckpointer.PreCommit(tbl)
	assert.Nil(t, err)
	err = ckpointer.Commit(tbl)
	assert.Nil(t, err)
	t.Log(info.String())

	replayHandle := NewReplayHandle(dir, nil)
	assert.NotNil(t, replayHandle)
	info2 := replayHandle.RebuildInfo(&opts.Mu, opts.Meta.Info.Conf)
	t.Log(info2.String())
	replayHandle.Cleanup()

}
