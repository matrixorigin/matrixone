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

package metadata

import (
	"encoding/json"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"sync"
	"testing"
)

const (
	blockRowCount     = uint64(16)
	segmentBlockCount = uint64(4)
)

func TestSegment(t *testing.T) {
	mu := &sync.RWMutex{}
	info := MockInfo(mu, blockRowCount, segmentBlockCount)
	info.Conf.Dir = "/tmp"
	schema := MockSchema(2)
	t1 := NowMicro()
	tbl := NewTable(NextGlobalSeqNum(), info, schema)
	seg1 := NewSegment(tbl, info.Sequence.GetSegmentID())
	seg2 := NewSegment(tbl, info.Sequence.GetSegmentID())
	blk1 := NewBlock(info.Sequence.GetBlockID(), seg2)
	err := seg1.RegisterBlock(blk1)
	assert.Error(t, err)

	for i := 0; i < int(seg1.MaxBlockCount); i++ {
		blk1, err = seg1.CreateBlock()
		assert.Nil(t, err)
		err = seg1.RegisterBlock(blk1)
		assert.Nil(t, err)
	}
	blk2 := NewBlock(info.Sequence.GetBlockID(), seg1)
	err = seg1.RegisterBlock(blk2)
	assert.Error(t, err)
	t.Log(err)

	_, err = seg1.ReferenceBlock(blk1.ID)
	assert.Nil(t, err)
	_, err = seg1.ReferenceBlock(blk2.ID)
	assert.Error(t, err)
	t.Log(seg1.String())

	ids := seg1.BlockIDs(t1)
	assert.Equal(t, len(ids), 0)
	// ts := NowMicro()
	ids = seg1.BlockIDs()
	assert.Equal(t, len(ids), int(seg1.MaxBlockCount))
}

func TestTable(t *testing.T) {
	mu := &sync.RWMutex{}
	info := MockInfo(mu, blockRowCount, segmentBlockCount)
	info.Conf.Dir = "/tmp"
	schema := MockSchema(2)
	tbl := NewTable(NextGlobalSeqNum(), info, schema)
	seg, err := tbl.CreateSegment()
	assert.Nil(t, err)

	assert.Equal(t, seg.GetBoundState(), Standalone)

	err = tbl.RegisterSegment(seg)
	assert.Nil(t, err)
	t.Log(tbl.String())
	assert.Equal(t, seg.GetBoundState(), Attached)

	sizeStep := uint64(20)
	rowStep := uint64(10)
	loopCnt := 1000
	pool, _ := ants.NewPool(20)
	var wg sync.WaitGroup
	f := func() {
		tbl.AppendStat(rowStep, sizeStep)
		wg.Done()
	}
	for i := 0; i < loopCnt; i++ {
		wg.Add(1)
		pool.Submit(f)
	}

	wg.Wait()
	assert.Equal(t, sizeStep*uint64(loopCnt), tbl.Stat.Size)
	assert.Equal(t, rowStep*uint64(loopCnt), tbl.Stat.Rows)
}

func TestInfo(t *testing.T) {
	mu := &sync.RWMutex{}
	info := MockInfo(mu, blockRowCount, segmentBlockCount)
	info.Conf.Dir = "/tmp"
	schema := MockSchema(2)
	tbl, err := info.CreateTable(NextGlobalSeqNum(), schema)
	assert.Nil(t, err)

	assert.Equal(t, tbl.GetBoundState(), Standalone)

	err = info.RegisterTable(tbl)
	assert.Nil(t, err)
	t.Log(info.String())
	assert.Equal(t, tbl.GetBoundState(), Attached)
}

func TestCreateDropTable(t *testing.T) {
	colCnt := 2
	tblInfo := MockTableInfo(colCnt)

	mu := &sync.RWMutex{}
	info := MockInfo(mu, blockRowCount, segmentBlockCount)
	info.Conf.Dir = "/tmp"
	tbl, err := info.CreateTableFromTableInfo(tblInfo, dbi.TableOpCtx{TableName: tblInfo.Name, OpIndex: NextGlobalSeqNum()})
	assert.Nil(t, err)
	assert.Equal(t, tblInfo.Name, tbl.Schema.Name)

	assert.Equal(t, len(tblInfo.Indices), len(tbl.Schema.Indices))
	for idx, indexInfo := range tblInfo.Indices {
		assert.Equal(t, indexInfo.Type, uint64(tbl.Schema.Indices[idx].Type))
		for iidx := range indexInfo.Columns {
			assert.Equal(t, indexInfo.Columns[iidx], uint64(tbl.Schema.Indices[idx].Columns[iidx]))
		}
	}
	for idx, colInfo := range tblInfo.Columns {
		assert.Equal(t, colInfo.Type, tbl.Schema.ColDefs[idx].Type)
		assert.Equal(t, colInfo.Name, tbl.Schema.ColDefs[idx].Name)
		assert.Equal(t, idx, tbl.Schema.ColDefs[idx].Idx)
	}

	rTbl, err := info.ReferenceTableByName(tbl.Schema.Name)
	assert.Nil(t, err)
	assert.NotNil(t, rTbl)

	ts := NowMicro()
	assert.False(t, rTbl.IsDeleted(ts))

	tid, err := info.SoftDeleteTable(tbl.Schema.Name, NextGlobalSeqNum())
	assert.Nil(t, err)
	assert.Equal(t, rTbl.ID, tid)

	_, err = info.SoftDeleteTable(tbl.Schema.Name, NextGlobalSeqNum())
	assert.NotNil(t, err)

	rTbl2, err := info.ReferenceTableByName(tbl.Schema.Name)
	assert.NotNil(t, err)
	assert.Nil(t, rTbl2)

	ts = NowMicro()
	assert.True(t, rTbl.IsDeleted(ts))

	rTbl3, err := info.ReferenceTable(tid)
	assert.Nil(t, err)
	assert.Equal(t, rTbl3.ID, tid)
	assert.True(t, rTbl3.IsDeleted(ts))

	tblBytes, err := rTbl3.Marshal()
	assert.Nil(t, err)
	t.Log(string(tblBytes))

	infoBytes, err := json.Marshal(info)
	assert.Nil(t, err)
	t.Log(string(infoBytes))

	newInfo := new(MetaInfo)
	err = newInfo.Unmarshal(infoBytes)
	assert.Nil(t, err)
	assert.Equal(t, newInfo.Tables[tid].ID, tid)
	assert.Equal(t, newInfo.Tables[tid].TimeStamp, rTbl3.TimeStamp)
}
