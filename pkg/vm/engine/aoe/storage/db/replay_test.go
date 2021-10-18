// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

package db

import (
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/internal/invariants"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
	"matrixone/pkg/vm/engine/aoe/storage/mock"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReplay1(t *testing.T) {
	initDBTest()
	// inst := initDB(storage.NORMAL_FT)
	inst := initDB(storage.MUTABLE_FT)
	tInfo := adaptor.MockTableInfo(2)
	name := "mockcon"
	tid, err := inst.CreateTable(tInfo, dbi.TableOpCtx{TableName: name, OpIndex: common.NextGlobalSeqNum()})
	assert.Nil(t, err)

	meta := inst.Opts.Meta.Catalog.SimpleGetTable(tid)
	assert.NotNil(t, meta)
	rel, err := inst.Relation(meta.Schema.Name)
	assert.Nil(t, err)

	irows := inst.Store.Catalog.Cfg.BlockMaxRows / 2
	ibat := mock.MockBatch(meta.Schema.Types(), irows)

	insertFn := func() {
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   common.NextGlobalSeqNum(),
			OpSize:    1,
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
	if invariants.RaceEnabled {
		time.Sleep(time.Duration(40) * time.Millisecond)
	}
	err = inst.Flush(name)
	assert.Nil(t, err)

	rel.Close()
	inst.Close()

	time.Sleep(time.Duration(20) * time.Millisecond)

	inst = initDB(storage.MUTABLE_FT)
	// inst = initDB(storage.NORMAL_FT)

	t.Log(inst.Store.Catalog.PString(metadata.PPL1))
	segmentedIdx, err := inst.GetSegmentedId(*dbi.NewTabletSegmentedIdCtx(meta.Schema.Name))
	assert.Nil(t, err)
	meta = inst.Opts.Meta.Catalog.SimpleGetTable(tid)
	assert.Equal(t, common.GetGlobalSeqNum(), segmentedIdx)

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
	rel.Close()
	inst.Close()
}

func mockSegFile(id common.ID, dir string, t *testing.T) string {
	name := id.ToSegmentFileName()
	fname := common.MakeSegmentFileName(dir, name, id.TableID, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	return fname
}

func mockBlkFile(id common.ID, dir string, t *testing.T) string {
	name := id.ToBlockFileName()
	fname := common.MakeBlockFileName(dir, name, id.TableID, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	return fname
}

func mockTBlkFile(id common.ID, version uint32, dir string, t *testing.T) string {
	name := id.ToTBlockFileName(version)
	fname := common.MakeTBlockFileName(dir, name, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	return fname
}

func initDataAndMetaDir(dir string) {
	dataDir := common.MakeDataDir(dir)
	os.MkdirAll(dataDir, os.ModePerm)
	metaDir := common.MakeMetaDir(dir)
	os.MkdirAll(metaDir, os.ModePerm)
}

type replayObserver struct {
	removed []string
}

func (o *replayObserver) OnRemove(name string) {
	o.removed = append(o.removed, name)
}

func TestReplay2(t *testing.T) {
	dir := "/tmp/testreplay2"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)

	mu := &sync.RWMutex{}
	colCnt := 2
	blkRowCount, segBlkCount := uint64(16), uint64(4)
	cfg := &metadata.CatalogCfg{
		Dir:              dir,
		SegmentMaxBlocks: segBlkCount,
		BlockMaxRows:     blkRowCount,
	}
	catalog, err := metadata.OpenCatalog(mu, cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()
	schema := metadata.MockSchema(colCnt)
	totalBlks := segBlkCount
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	opts := new(storage.Options)
	opts.Meta.Catalog = catalog
	opts.FillDefaults(dir)

	blkfiles := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := len(seg.BlockSet) - 2; i >= 0; i-- {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}

	catalog.Close()
	catalog, err = metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()

	assert.Equal(t, 0, len(observer.removed))
	catalog.Close()
}

func buildOpts(dir string) *storage.Options {
	blkRowCount, segBlkCount := uint64(16), uint64(4)
	cfg := &storage.MetaCfg{
		BlockMaxRows:     blkRowCount,
		SegmentMaxBlocks: segBlkCount,
	}
	opts := new(storage.Options)
	opts.Meta.Conf = cfg
	opts.FillDefaults(dir)
	return opts
}

func TestReplay3(t *testing.T) {
	dir := "/tmp/testreplay3"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	tblkfiles := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-1; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-1]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	tblkfiles = append(tblkfiles, name)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()

	assert.Equal(t, 0, len(observer.removed))
	catalog.Close()
}

func TestReplay4(t *testing.T) {
	dir := "/tmp/testreplay4"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	totalBlks := opts.Meta.Catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(opts.Meta.Catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-2; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	unblk := seg.BlockSet[len(seg.BlockSet)-2]
	name := mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(blkfiles, name)

	unblk = seg.BlockSet[len(seg.BlockSet)-1]
	name = mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(blkfiles, name)
	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})

	opts.Meta.Catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), opts.Meta.Catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, opts.Meta.Catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()

	assert.Equal(t, 2, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	catalog.Close()
}

func TestReplay5(t *testing.T) {
	dir := "/tmp/testreplay5"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-2; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-3]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*tblk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	unblk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()
	tbl2 := catalog.SimpleGetTable(tbl.Id)
	assert.NotNil(t, tbl2)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[1].IsFull())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[2].IsFull())

	assert.Equal(t, 3, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk := tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	t.Log(tbl2.PString(metadata.PPL1))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[2], blk)

	catalog.Close()
}

func TestReplay6(t *testing.T) {
	dir := "/tmp/testreplay6"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	tblkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-2; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-3]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*tblk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	unblk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*unblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*unblk.AsCommonID(), uint32(1), dir, t)
	tblkfiles = append(tblkfiles, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()
	tbl2 := catalog.SimpleGetTable(tbl.Id)
	assert.NotNil(t, tbl2)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[1].IsFull())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[2].IsFull())

	assert.Equal(t, 4, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk := tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[2], blk)

	catalog.Close()
}

func TestReplay7(t *testing.T) {
	dir := "/tmp/testreplay7"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-3; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	blk := seg.BlockSet[len(seg.BlockSet)-3]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	blk = seg.BlockSet[len(seg.BlockSet)-2]
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()
	tbl2 := catalog.SimpleGetTable(tbl.Id)
	assert.NotNil(t, tbl2)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFull())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[1].IsFull())

	assert.Equal(t, 4, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk = tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[1], blk)

	catalog.Close()
}

func TestReplay8(t *testing.T) {
	dir := "/tmp/testreplay8"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-3; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	blk := seg.BlockSet[len(seg.BlockSet)-3]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)

	blk = seg.BlockSet[len(seg.BlockSet)-2]
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)
	t.Log(tbl.PString(metadata.PPL1))

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2 := catalog.SimpleGetTable(tbl.Id)
	assert.NotNil(t, tbl2)
	t.Log(tbl2.PString(metadata.PPL1))
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFull())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[1].IsFull())

	assert.Equal(t, 4, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk = tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[1], blk)

	catalog.Close()
}

func TestReplay9(t *testing.T) {
	dir := "/tmp/testreplay9"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-3; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	blk := seg.BlockSet[len(seg.BlockSet)-3]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)

	blk = seg.BlockSet[len(seg.BlockSet)-2]
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	blk = seg.BlockSet[len(seg.BlockSet)-1]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	seg = tbl.SegmentSet[1]
	for i := 0; i < len(seg.BlockSet); i++ {
		blk := seg.BlockSet[i]
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		toRemove = append(toRemove, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
		toRemove = append(toRemove, name)
	}

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2 := catalog.SimpleGetTable(tbl.Id)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFull())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[1].IsFull())

	assert.Equal(t, len(toRemove), len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	t.Log(tbl2.PString(metadata.PPL1))
	blk = tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[1], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[2], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[3], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[0], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[1], blk)

	tmp := blk

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[2], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[3], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[2].BlockSet[0], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(tmp)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[2], blk)

	catalog.Close()
}

func TestReplay10(t *testing.T) {
	dir := "/tmp/testreplay10"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet); i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
		toRemove = append(toRemove, name)
	}

	seg = tbl.SegmentSet[1]

	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockBlkFile(*tblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	name = mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)

	blk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2 := catalog.SimpleGetTable(tbl.Id)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[3].IsFull())
	assert.False(t, tbl2.SegmentSet[1].BlockSet[1].IsFull())
	t.Log(tbl2.PString(metadata.PPL1))

	assert.Equal(t, len(toRemove), len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)
	catalog.Close()
}

func TestReplay11(t *testing.T) {
	dir := "/tmp/testreplay11"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	tbl := metadata.MockTable(catalog, schema, totalBlks, nil)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet); i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		toRemove = append(toRemove, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
		toRemove = append(toRemove, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
		toRemove = append(toRemove, name)
	}
	mockSegFile(*seg.AsCommonID(), dir, t)

	seg = tbl.SegmentSet[1]

	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockBlkFile(*tblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	name = mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)

	blk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	// t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg, nil)
	assert.Nil(t, err)
	catalog.StartSyncer()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2 := catalog.SimpleGetTable(tbl.Id)
	assert.NotNil(t, tbl2)
	t.Log(tbl2.PString(metadata.PPL1))
	assert.True(t, tbl2.SegmentSet[0].BlockSet[3].IsFull())
	assert.False(t, tbl2.SegmentSet[1].BlockSet[1].IsFull())

	assert.Equal(t, len(toRemove), len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)
	catalog.Close()
}

func TestReplay12(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	//inst := initDB(storage.MUTABLE_FT)
	tInfo := adaptor.MockTableInfo(2)
	name := "mockcon"
	tid, err := inst.CreateTable(tInfo, dbi.TableOpCtx{TableName: name, OpIndex: common.NextGlobalSeqNum()})
	assert.Nil(t, err)

	meta := inst.Opts.Meta.Catalog.SimpleGetTable(tid)
	assert.NotNil(t, meta)
	rel, err := inst.Relation(meta.Schema.Name)
	assert.Nil(t, err)

	irows := inst.Store.Catalog.Cfg.BlockMaxRows / 2
	ibat := mock.MockBatch(meta.Schema.Types(), irows)

	insertFn := func() {
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   common.NextGlobalSeqNum(),
			OpSize:    1,
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

	ibat2 := mock.MockBatch(meta.Schema.Types(), inst.Store.Catalog.Cfg.BlockMaxRows)
	insertFn2 := func() {
		index := common.NextGlobalSeqNum()
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   index,
			OpOffset:  0,
			OpSize:    2,
			Data:      ibat,
			TableName: meta.Schema.Name,
		})
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   index,
			OpOffset:  1,
			OpSize:    2,
			Data:      ibat2,
			TableName: meta.Schema.Name,
		})
		assert.Nil(t, err)
	}

	insertFn2()
	assert.Equal(t, irows*5, uint64(rel.Rows()))
	time.Sleep(time.Duration(10) * time.Millisecond)
	if invariants.RaceEnabled {
		time.Sleep(time.Duration(40) * time.Millisecond)
	}

	rel.Close()
	inst.Close()

	time.Sleep(time.Duration(20) * time.Millisecond)

	inst = initDB(storage.NORMAL_FT)
	// inst = initDB(engine.NORMAL_FT)

	segmentedIdx, err := inst.GetSegmentedId(*dbi.NewTabletSegmentedIdCtx(meta.Schema.Name))
	assert.Nil(t, err)
	assert.Equal(t, common.GetGlobalSeqNum()-1, segmentedIdx)

	rel, err = inst.Relation(meta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, irows*4, uint64(rel.Rows()))
	t.Log(rel.Rows())

	insertFn3 := func() {
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   segmentedIdx + 1,
			OpOffset:  0,
			OpSize:    2,
			Data:      ibat,
			TableName: meta.Schema.Name,
		})
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   segmentedIdx + 1,
			OpOffset:  1,
			OpSize:    2,
			Data:      ibat2,
			TableName: meta.Schema.Name,
		})
		assert.Nil(t, err)
	}

	insertFn3()
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
	rel.Close()
	inst.Close()
}
