package metadata

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"sync"
	"testing"
	"time"
)

func TestTable(t *testing.T) {
	mu := &sync.RWMutex{}
	info := MockInfo(mu, blockRowCount, segmentBlockCount)
	tbl1 := NewTable(NextGlobalSeqNum(), info, MockSchema(2))
	tbl1.Schema.Name = "tbl1"
	assert.Nil(t, info.RegisterTable(tbl1))
	tbl2 := NewTable(NextGlobalSeqNum(), info, MockSchema(2), uint64(999))
	tbl2.Schema.Name = "tbl2"
	assert.Nil(t, info.RegisterTable(tbl2))
	assert.Equal(t, tbl2.GetID(), uint64(999))
	schema := MockSchema(2)
	schema.Name = "tbl3"
	tbl3 := MockTable(info, schema, segmentBlockCount*10)
	tbl4 := MockTable(info, nil, segmentBlockCount)
	for i := 1; i < 11; i++ {
		assert.Equal(t, uint64(i), tbl3.GetActiveSegment().GetID())
		tbl3.NextActiveSegment()
	}
	assert.Equal(t, tbl4.GetTableId(), uint64(3))


	assert.Nil(t, tbl1.GetReplayIndex())
	assert.Panics(t, func() {
		tbl1.ResetReplayIndex()
	})
	tbl1.ReplayIndex = &LogIndex{}
	assert.NotNil(t, tbl1.GetReplayIndex())
	assert.NotPanics(t, func() {
		tbl1.ResetReplayIndex()
	})
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			tbl1.AppendStat(1, 1)
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, tbl1.GetRows(), uint64(1000))
	assert.Equal(t, tbl1.GetSize(), uint64(1000))

	assert.Nil(t, tbl2.GetActiveSegment())
	assert.Nil(t, tbl2.NextActiveSegment())

	seg1, err := tbl2.CreateSegment()
	assert.Nil(t, err)
	assert.Nil(t, tbl2.RegisterSegment(seg1))
	_, err = tbl2.ReferenceSegment(seg1.GetID()+1)
	assert.NotNil(t, err)
	_, err = tbl2.CloneSegment(seg1.GetID()+1, CopyCtx{})
	assert.NotNil(t, err)
	_, err = tbl2.ReferenceBlock(seg1.GetID()+1, 0)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(tbl2.GetSegmentBlockIDs(seg1.GetID()+1)))
	assert.Equal(t, 0, len(tbl2.GetSegmentBlockIDs(seg1.GetID())))
	_, err = tbl2.ReferenceSegment(seg1.GetID())
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), tbl2.GetSegmentCount())
	assert.NotNil(t, tbl2.RegisterSegment(seg1))
	assert.NotNil(t, tbl2.RegisterSegment(&Segment{Table: &Table{ID: uint64(1000)}}))
	assert.NotNil(t, tbl2.RegisterSegment(&Segment{BoundSate: Attached, Table: &Table{ID: uint64(999)}}))
	assert.NotNil(t, tbl2.RegisterSegment(&Segment{ID: seg1.GetID(), Table: &Table{ID: uint64(999)}}))
	assert.Equal(t, "Tbl(999) 0[Seg(999-12) [blkPos=0][State=0][]\n]", tbl2.String())
	ts1 := NowMicro()
	seg2, err := tbl2.CreateSegment()
	assert.Nil(t, err)
	assert.Nil(t, tbl2.RegisterSegment(seg2))
	assert.Equal(t, "Tbl(999) 0[Seg(999-12) [blkPos=0][State=0][]\nSeg(999-13) [blkPos=0][State=0][]\n]", tbl2.String())
	time.Sleep(time.Microsecond)
	assert.Equal(t, 2, len(tbl2.SegmentIDs()))
	assert.Equal(t, 2, len(tbl2.SegmentIDs(NowMicro())))
	assert.Equal(t, 1, len(tbl2.SegmentIDs(ts1)))
	for i := 0; i < int(segmentBlockCount); i++ {
		blk, err := seg2.CreateBlock()
		assert.Nil(t, err)
		assert.Nil(t, seg2.RegisterBlock(blk))
		seg2.NextActiveBlk()
	}
	assert.Nil(t, tbl2.NextActiveSegment())
	tbl2_ := tbl2.Copy(CopyCtx{})
	assert.Equal(t, tbl2_.GetID(), tbl2.GetID())
	tbl2_ = tbl2.Copy(CopyCtx{Ts: ts1})
	assert.Equal(t, uint64(1), tbl2_.GetSegmentCount())
	assert.Equal(t, tbl2.GetID(), tbl2.LiteCopy().GetID())
	s, err := tbl2.GetInfullSegment()
	assert.Equal(t, uint64(12), s.GetID())
	ms, mb := tbl2.GetMaxSegIDAndBlkID()
	assert.Equal(t, uint64(13), ms)
	assert.Equal(t, uint64(12*4), mb)
	tbl2.Replay() // TODO: test replay
	assert.Equal(t, "999_v6", tbl2.GetFileName())
	assert.Equal(t, "999_v5", tbl2.GetLastFileName())
	assert.Nil(t, tbl2.Serialize(bytes.NewBuffer(make([]byte, 100))))
	n, err := tbl2.Marshal()
	assert.Nil(t, err)
	var tbl5 Table
	assert.Nil(t, tbl5.Unmarshal(n))
	assert.Equal(t, tbl5.GetID(), tbl2.GetID())
	buf := bytes.NewBuffer(n)
	var tbl6 Table
	_, err = tbl6.ReadFrom(buf)
	assert.Nil(t, err)
	assert.Equal(t, tbl5.GetID(), tbl6.GetID())
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

	_, err = rTbl3.Marshal()
	assert.Nil(t, err)
	//t.Log(string(tblBytes))

	infoBytes, err := json.Marshal(info)
	assert.Nil(t, err)
	//t.Log(string(infoBytes))

	newInfo := new(MetaInfo)
	err = newInfo.Unmarshal(infoBytes)
	assert.Nil(t, err)
	assert.Equal(t, newInfo.Tables[tid].ID, tid)
	assert.Equal(t, newInfo.Tables[tid].TimeStamp, rTbl3.TimeStamp)
}
