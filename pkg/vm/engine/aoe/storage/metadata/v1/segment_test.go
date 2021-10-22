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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSegment(t *testing.T) {
	mu := &sync.RWMutex{}
	info := MockInfo(mu, blockRowCount, segmentBlockCount)
	schema := MockSchema(2)
	t1 := NowMicro()
	tbl := NewTable(NextGlobalSeqNum(), info, schema)
	seg1 := NewSegment(tbl, info.Sequence.GetSegmentID())
	assert.Equal(t, seg1.Table.ID, seg1.GetTableID())
	assert.Equal(t, seg1.GetID(), seg1.AsCommonID().SegmentID)
	seg2 := NewSegment(tbl, info.Sequence.GetSegmentID())
	seg2.ReplayState()
	blk1 := NewBlock(info.Sequence.GetBlockID(), seg2)
	err := seg1.RegisterBlock(blk1)
	assert.NotNil(t, err)

	for i := 0; i < int(seg1.MaxBlockCount); i++ {
		blk1, err = seg1.CreateBlock()
		assert.Nil(t, err)
		err = seg1.RegisterBlock(blk1)
		assert.Nil(t, err)
	}
	blk2 := NewBlock(info.Sequence.GetBlockID(), seg1)
	err = seg1.RegisterBlock(blk2)
	assert.NotNil(t, err)
	seg2.Table = NewTable(NextGlobalSeqNum(), info, schema, 10)
	assert.NotNil(t, seg2.RegisterBlock(blk2))
	seg2.Table = tbl
	//t.Log(err)

	_, err = seg1.ReferenceBlock(blk1.ID)
	assert.Nil(t, err)
	_, err = seg1.ReferenceBlock(blk2.ID)
	assert.NotNil(t, err)
	_ = seg1.String()

	ids := seg1.BlockIDs(t1)
	assert.Equal(t, len(ids), 0)
	// ts := NowMicro()
	ids = seg1.BlockIDs()
	assert.Equal(t, len(ids), int(seg1.MaxBlockCount))

	list := seg1.BlockIDList(t1)
	assert.Equal(t, len(list), 0)
	list = seg1.BlockIDList()
	assert.Equal(t, len(list), int(seg1.MaxBlockCount))

	_, err = seg1.CloneBlock(1000, CopyCtx{})
	assert.NotNil(t, err)
	//for id := range seg1.IdMap {
	//	t.Log(id)
	//}
	_, err = seg1.CloneBlock(blk2.ID-1, CopyCtx{})
	assert.Nil(t, err)

	for i := 0; i < int(seg2.MaxBlockCount)-1; i++ {
		blk, err := seg2.CreateBlock()
		assert.Nil(t, err)
		err = seg2.RegisterBlock(blk)
		assert.Nil(t, err)
		assert.Nil(t, blk.SetCount(blockRowCount))
		seg2.ReplayState()
	}
	assert.False(t, seg2.TryClose())
	assert.NotNil(t, seg2.TrySorted())
	blk3, err := seg2.CreateBlock()
	assert.Nil(t, blk3.Attach())
	assert.NotNil(t, seg2.RegisterBlock(blk3))
	assert.Nil(t, blk3.Detach())
	//t.Log(blk3.GetBoundState())
	blk3.ID = blk3.ID - 1
	assert.NotNil(t, seg2.RegisterBlock(blk3))
	blk3.ID = blk3.ID + 1
	assert.Nil(t, blk3.Detach())
	assert.Nil(t, blk3.SetCount(blk3.MaxRowCount))
	blk3.DataState = FULL
	assert.True(t, seg2.HasUncommitted())
	assert.Nil(t, seg2.RegisterBlock(blk3))
	assert.Equal(t, seg2.DataState, seg2.Copy(CopyCtx{Ts: NowMicro()}).DataState)
	assert.Nil(t, blk3.Delete(NowMicro()))
	time.Sleep(time.Duration(1) * time.Microsecond)
	assert.Equal(t, seg2.DataState, seg2.Copy(CopyCtx{}).DataState)
	assert.True(t, seg2.TryClose())
	assert.Nil(t, seg2.TrySorted())

	assert.Equal(t, blk3.ID, seg2.GetMaxBlkID())
	seg2.ReplayState()
	m, err := seg2.Marshal()
	assert.Nil(t, err)
	assert.Equal(t, 795, len(m))
	assert.NotNil(t, seg2.GetActiveBlk())
	assert.NotNil(t, seg2.NextActiveBlk())
	seg2.ActiveBlk = int(segmentBlockCount) - 1
	assert.Nil(t, seg2.NextActiveBlk())
	seg2.ActiveBlk = int(segmentBlockCount)
	assert.Nil(t, seg2.GetActiveBlk())
	assert.False(t, seg2.HasUncommitted())
}
