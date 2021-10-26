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
// limitations under the License.

package db

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/internal/invariants"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"

	"github.com/stretchr/testify/assert"
)

var (
	TEST_OPEN_DIR = "/tmp/open_test"
)

func initTest() {
	os.RemoveAll(TEST_OPEN_DIR)
}

func TestOpen(t *testing.T) {
	initTest()
	cfg := &storage.MetaCfg{
		SegmentMaxBlocks: 10,
		BlockMaxRows:     10,
	}
	opts := &storage.Options{}
	opts.Meta.Conf = cfg
	inst, err := Open(TEST_OPEN_DIR, opts)
	assert.Nil(t, err)
	assert.NotNil(t, inst)
	err = inst.Close()
	assert.Nil(t, err)
}

func TestDBReplay(t *testing.T) {
	waitTime := time.Duration(20) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime = time.Duration(100) * time.Millisecond
	}
	initDBTest()
	// ft := storage.MUTABLE_FT
	ft := storage.MUTABLE_FT
	inst := initDB(ft, wal.BrokerRole)
	tableInfo := adaptor.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mocktbl", OpIndex: uint64(1)})
	assert.Nil(t, err)
	tblMeta := inst.Opts.Meta.Catalog.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	blkCnt := 2
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * uint64(blkCnt)
	ck := mock.MockBatch(tblMeta.Schema.Types(), rows)
	assert.Equal(t, uint64(rows), uint64(ck.Vecs[0].Length()))
	insertCnt := 4
	for i := 0; i < insertCnt; i++ {
		err = inst.Append(dbi.AppendCtx{
			OpIndex:   uint64(i + 2),
			OpOffset:  0,
			OpSize:    1,
			Data:      ck,
			TableName: tableInfo.Name,
		})
		assert.Nil(t, err)
	}
	time.Sleep(waitTime)
	if invariants.RaceEnabled {
		time.Sleep(waitTime)
	}
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())

	tbl, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return uint64(insertCnt) == inst.GetShardCheckpointId(0)
	})
	segmentedIdx, err := inst.GetSegmentedId(*dbi.NewTabletSegmentedIdCtx(tableInfo.Name))
	assert.Nil(t, err)
	t.Logf("SegmentedIdx: %d", segmentedIdx)
	assert.Equal(t, uint64(insertCnt), segmentedIdx)
	assert.Equal(t, uint64(insertCnt), inst.GetShardCheckpointId(0))

	t.Logf("Row count: %d", tbl.GetRowCount())
	assert.Equal(t, rows*uint64(insertCnt), tbl.GetRowCount())

	t.Log(tbl.GetMeta().PString(metadata.PPL2))
	inst.Close()

	dataDir := common.MakeDataDir(inst.Dir)
	invalidFileName := filepath.Join(dataDir, "invalid")
	f, err := os.Create(invalidFileName)
	assert.Nil(t, err)
	f.Close()

	inst = initDB(ft, wal.BrokerRole)

	os.Stat(invalidFileName)
	_, err = os.Stat(invalidFileName)
	assert.True(t, os.IsNotExist(err))

	// t.Log(inst.MTBufMgr.String())
	// t.Log(inst.SSTBufMgr.String())

	replaytblMeta := inst.Opts.Meta.Catalog.SimpleGetTableByName(tableInfo.Name)
	assert.NotNil(t, replaytblMeta)
	assert.Equal(t, tblMeta.Schema.Name, replaytblMeta.Schema.Name)

	tbl, err = inst.Store.DataTables.WeakRefTable(replaytblMeta.Id)
	assert.Nil(t, err)
	t.Logf("Row count: %d, %d", tbl.GetRowCount(), rows*uint64(insertCnt))
	assert.Equal(t, rows*uint64(insertCnt)-tblMeta.Schema.BlockMaxRows, tbl.GetRowCount())

	replayIndex := tbl.GetMeta().GetReplayIndex()
	assert.Equal(t, tblMeta.Schema.BlockMaxRows, replayIndex.Count)
	assert.False(t, replayIndex.IsApplied())

	for i := int(segmentedIdx) + 1; i < int(segmentedIdx)+1+insertCnt; i++ {
		err = inst.Append(dbi.AppendCtx{
			TableName: tableInfo.Name,
			Data:      ck,
			OpIndex:   uint64(i),
			OpSize:    1,
		})
		assert.Nil(t, err)
	}
	_, err = inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: tableInfo.Name, OpIndex: segmentedIdx + 1 + uint64(insertCnt)})
	assert.NotNil(t, err)

	// time.Sleep(waitTime)
	// if invariants.RaceEnabled {
	// 	time.Sleep(waitTime)
	// }
	// t.Log(inst.MTBufMgr.String())
	// t.Log(inst.SSTBufMgr.String())
	testutils.WaitExpect(200, func() bool {
		return 2*rows*uint64(insertCnt)-2*tblMeta.Schema.BlockMaxRows == tbl.GetRowCount()
	})
	t.Logf("Row count: %d", tbl.GetRowCount())
	assert.Equal(t, 2*rows*uint64(insertCnt)-2*tblMeta.Schema.BlockMaxRows, tbl.GetRowCount())

	preSegmentedIdx := segmentedIdx

	testutils.WaitExpect(200, func() bool {
		return preSegmentedIdx+uint64(insertCnt)-1 == inst.GetShardCheckpointId(0)
	})

	segmentedIdx, err = inst.GetSegmentedId(*dbi.NewTabletSegmentedIdCtx(tableInfo.Name))
	assert.Nil(t, err)
	t.Logf("SegmentedIdx: %d", segmentedIdx)
	assert.Equal(t, preSegmentedIdx+uint64(insertCnt)-1, segmentedIdx)
	assert.Equal(t, preSegmentedIdx+uint64(insertCnt)-1, inst.GetShardCheckpointId(0))

	inst.Close()
}

func TestMultiInstance(t *testing.T) {
	dir := "/tmp/multi"
	os.RemoveAll(dir)
	var dirs []string
	for i := 0; i < 10; i++ {
		dirs = append(dirs, path.Join(dir, fmt.Sprintf("wd%d", i)))
	}
	var insts []*DB
	for _, d := range dirs {
		opts := storage.Options{}
		inst, _ := Open(d, &opts)
		insts = append(insts, inst)
		defer inst.Close()
	}

	info := adaptor.MockTableInfo(2)
	for _, inst := range insts {
		info = adaptor.MockTableInfo(2)
		_, err := inst.CreateTable(info, dbi.TableOpCtx{TableName: info.Name, OpIndex: common.NextGlobalSeqNum()})
		assert.Nil(t, err)
	}
	meta := insts[0].Store.Catalog.SimpleGetTableByName(info.Name)
	bat := mock.MockBatch(meta.Schema.Types(), 100)
	for _, inst := range insts {
		err := inst.Append(dbi.AppendCtx{TableName: info.Name, Data: bat, OpIndex: common.NextGlobalSeqNum(), OpSize: 1})
		assert.Nil(t, err)
	}

	time.Sleep(time.Duration(50) * time.Millisecond)
}
