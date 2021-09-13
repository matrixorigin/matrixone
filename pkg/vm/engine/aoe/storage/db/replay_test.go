package db

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"sort"
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

func mockTBlkFile(id common.ID, version uint32, dir string, t *testing.T) string {
	name := id.ToTBlockFileName(version)
	fname := engine.MakeTBlockFileName(dir, name, false)
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

type replayObserver struct {
	removed []string
}

func (o *replayObserver) OnRemove(name string) {
	o.removed = append(o.removed, name)
}

func flushInfo(opts *engine.Options, info *metadata.MetaInfo, t *testing.T) {
	ckpointer := opts.Meta.CKFactory.Create()
	err := ckpointer.PreCommit(info)
	assert.Nil(t, err)
	err = ckpointer.Commit(info)
	assert.Nil(t, err)
}

func flushTable(opts *engine.Options, meta *metadata.Table, t *testing.T) {
	ckpointer := opts.Meta.CKFactory.Create()
	err := ckpointer.PreCommit(meta)
	assert.Nil(t, err)
	err = ckpointer.Commit(meta)
	assert.Nil(t, err)
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
	for i := len(seg.Blocks) - 2; i >= 0; i-- {
		blk := seg.Blocks[i]
		blk.DataState = metadata.FULL
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	flushInfo(opts, info, t)
	flushTable(opts, tbl, t)

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, nil)
	assert.NotNil(t, replayHandle)
	info2 := replayHandle.RebuildInfo(&opts.Mu, opts.Meta.Info.Conf)
	t.Log(info2.String())
	replayHandle.Cleanup()

	assert.Equal(t, 0, len(observer.removed))
}

func buildOpts(dir string) *engine.Options {
	mu := &sync.RWMutex{}
	blkRowCount, segBlkCount := uint64(16), uint64(4)
	info := metadata.MockInfo(mu, blkRowCount, segBlkCount)
	info.Conf.Dir = dir
	opts := new(engine.Options)
	opts.Meta.Info = info
	opts.FillDefaults(dir)
	return opts
}

func TestReplay3(t *testing.T) {
	dir := "/tmp/testreplay3"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	info := opts.Meta.Info
	totalBlks := info.Conf.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(info, schema, totalBlks)
	blkfiles := make([]string, 0)
	tblkfiles := make([]string, 0)
	seg := tbl.Segments[0]
	for i := 0; i < len(seg.Blocks)-1; i++ {
		blk := seg.Blocks[i]
		blk.DataState = metadata.FULL
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.Blocks[len(seg.Blocks)-1]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	tblkfiles = append(tblkfiles, name)
	flushInfo(opts, info, t)
	flushTable(opts, tbl, t)

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, nil)
	assert.NotNil(t, replayHandle)
	replayHandle.RebuildInfo(&opts.Mu, opts.Meta.Info.Conf)
	replayHandle.Cleanup()

	assert.Equal(t, 0, len(observer.removed))
}

func TestReplay4(t *testing.T) {
	dir := "/tmp/testreplay4"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	info := opts.Meta.Info
	totalBlks := info.Conf.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(info, schema, totalBlks)
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.Segments[0]
	for i := 0; i < len(seg.Blocks)-2; i++ {
		blk := seg.Blocks[i]
		blk.DataState = metadata.FULL
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	unblk := seg.Blocks[len(seg.Blocks)-2]
	name := mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(blkfiles, name)

	unblk = seg.Blocks[len(seg.Blocks)-1]
	name = mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(blkfiles, name)
	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})

	flushInfo(opts, info, t)
	flushTable(opts, tbl, t)

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, observer)
	assert.NotNil(t, replayHandle)
	info2 := replayHandle.RebuildInfo(&opts.Mu, opts.Meta.Info.Conf)
	replayHandle.Cleanup()
	t.Log(info2.String())

	assert.Equal(t, 2, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
}
