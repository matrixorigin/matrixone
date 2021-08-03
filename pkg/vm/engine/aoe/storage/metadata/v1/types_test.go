package metadata

import (
	"encoding/json"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestBlock(t *testing.T) {
	ts1 := NowMicro()
	time.Sleep(time.Duration(1) * time.Microsecond)
	info := MockInfo(BLOCK_ROW_COUNT, SEGMENT_BLOCK_COUNT)
	info.Conf.Dir = "/tmp"
	schema := MockSchema(2)
	tbl := NewTable(NextGloablSeqnum(), info, schema)
	seg := NewSegment(tbl, info.Sequence.GetSegmentID())
	blk := NewBlock(info.Sequence.GetBlockID(), seg)
	time.Sleep(time.Duration(1) * time.Microsecond)
	ts2 := NowMicro()
	t.Logf("%d %d %d", ts1, blk.CreatedOn, ts2)
	assert.False(t, blk.Select(ts1))
	assert.True(t, blk.Select(ts2))
	time.Sleep(time.Duration(1) * time.Microsecond)
	ts3 := NowMicro()

	err := blk.Delete(ts3)
	assert.Nil(t, err)
	time.Sleep(time.Duration(1) * time.Microsecond)
	ts4 := NowMicro()

	assert.False(t, blk.Select(ts1))
	assert.True(t, blk.Select(ts2))
	assert.False(t, blk.Select(ts3))
	assert.False(t, blk.Select(ts4))
}

func TestSegment(t *testing.T) {
	info := MockInfo(BLOCK_ROW_COUNT, SEGMENT_BLOCK_COUNT)
	info.Conf.Dir = "/tmp"
	schema := MockSchema(2)
	t1 := NowMicro()
	tbl := NewTable(NextGloablSeqnum(), info, schema)
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
	info := MockInfo(BLOCK_ROW_COUNT, SEGMENT_BLOCK_COUNT)
	info.Conf.Dir = "/tmp"
	schema := MockSchema(2)
	tbl := NewTable(NextGloablSeqnum(), info, schema)
	seg, err := tbl.CreateSegment()
	assert.Nil(t, err)

	assert.Equal(t, seg.GetBoundState(), STANDLONE)

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
	info := MockInfo(BLOCK_ROW_COUNT, SEGMENT_BLOCK_COUNT)
	info.Conf.Dir = "/tmp"
	schema := MockSchema(2)
	tbl, err := info.CreateTable(NextGloablSeqnum(), schema)
	assert.Nil(t, err)

	assert.Equal(t, tbl.GetBoundState(), STANDLONE)

	err = info.RegisterTable(tbl)
	assert.Nil(t, err)
	t.Log(info.String())
	assert.Equal(t, tbl.GetBoundState(), Attached)
}

func TestCreateDropTable(t *testing.T) {
	colCnt := 2
	tblInfo := MockTableInfo(colCnt)

	info := MockInfo(BLOCK_ROW_COUNT, SEGMENT_BLOCK_COUNT)
	info.Conf.Dir = "/tmp"
	tbl, err := info.CreateTableFromTableInfo(tblInfo, dbi.TableOpCtx{TableName: tblInfo.Name, OpIndex: NextGloablSeqnum()})
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

	tid, err := info.SoftDeleteTable(tbl.Schema.Name, NextGloablSeqnum())
	assert.Nil(t, err)
	assert.Equal(t, rTbl.ID, tid)

	_, err = info.SoftDeleteTable(tbl.Schema.Name, NextGloablSeqnum())
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
