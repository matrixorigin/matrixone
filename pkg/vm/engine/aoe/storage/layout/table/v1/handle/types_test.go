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

package handle

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSnapshot(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	seg_cnt := 4
	blk_cnt := 2
	capacity := typeSize * row_count * uint64(seg_cnt) * uint64(blk_cnt) * 2
	indexBufMgr := bmgr.MockBufMgr(capacity)
	mtBufMgr := bmgr.MockBufMgr(capacity)
	sstBufMgr := bmgr.MockBufMgr(capacity)
	tables := table.NewTables(new(sync.RWMutex), ldio.DefaultFsMgr, mtBufMgr, sstBufMgr, indexBufMgr)

	info := md.MockInfo(&opts.Mu, row_count, uint64(blk_cnt))
	tableMeta := md.MockTable(info, schema, uint64(blk_cnt*seg_cnt))

	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	t.Logf("TableData RefCount=%d", tableData.RefCount())
	segIDs := table.MockSegments(tableMeta, tableData)
	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	root := tableData.WeakRefRoot()
	assert.Equal(t, int64(1), root.RefCount())

	now := time.Now()

	cols := []int{0, 1}
	tableData.Ref()
	ss := NewSnapshot(segIDs, cols, tableData)
	segIt := ss.NewIt()
	actualSegCnt := 0
	actualBlkCnt := 0
	for segIt.Valid() {
		actualSegCnt++
		segment := segIt.GetHandle()
		blkIt := segment.NewIt()
		for blkIt.Valid() {
			actualBlkCnt++
			blk := blkIt.GetHandle()
			h := blk.Prefetch()
			h.Close()
			// blk.Close()
			blkIt.Next()
		}
		blkIt.Close()
		// segment.Close()
		segIt.Next()
	}
	segIt.Close()
	assert.Equal(t, seg_cnt, actualSegCnt)
	assert.Equal(t, seg_cnt*blk_cnt, actualBlkCnt)
	du := time.Since(now)
	t.Log(du)
	t.Log(sstBufMgr.String())
	t.Log(tableData.String())
	ss.Close()
	assert.Equal(t, int64(1), root.RefCount())

	tableData.Ref()
	ss2 := NewLinkAllSnapshot(cols, tableData)
	linkSegIt := ss2.NewIt()
	actualSegCnt = 0
	actualBlkCnt = 0
	for linkSegIt.Valid() {
		actualSegCnt++
		segment := linkSegIt.GetHandle()
		blkIt := segment.NewIt()
		for blkIt.Valid() {
			actualBlkCnt++
			blk := blkIt.GetHandle()
			h := blk.Prefetch()
			h.Close()
			// blk.Close()
			blkIt.Next()
		}
		blkIt.Close()
		// segment.Close()
		linkSegIt.Next()
	}
	linkSegIt.Close()
	assert.Equal(t, seg_cnt, actualSegCnt)
	assert.Equal(t, seg_cnt*blk_cnt, actualBlkCnt)

	linkSegIt = ss2.NewIt()
	for linkSegIt.Valid() {
		segment := linkSegIt.GetHandle()
		ids := segment.BlockIds()
		for _, id := range ids {
			blk := segment.GetBlock(id)
			assert.NotNil(t, blk)
			blkH := blk.Prefetch()
			blkH.Close()
		}
		linkSegIt.Next()
	}
	linkSegIt.Close()

	ss2.Close()
	assert.Equal(t, int64(1), root.RefCount())
	t.Log(tableData.String())
	t.Log(mtBufMgr.String())
	t.Log(sstBufMgr.String())
}
