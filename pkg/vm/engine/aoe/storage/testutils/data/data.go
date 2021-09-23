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

package data

// import (
// 	"github.com/stretchr/testify/assert"
// 	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
// 	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
// 	"matrixone/pkg/vm/engine/aoe/storage/common"
// 	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
// 	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
// 	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
// 	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
// 	w "matrixone/pkg/vm/engine/aoe/storage/worker"
// 	"testing"
// 	// log "github.com/sirupsen/logrus"
// )

// func MakeBufMagr(capacity uint64) mgrif.IBufferManager {
// 	flusher := w.NewOpWorker("Mock Flusher")
// 	bufMgr := bmgr.NewBufferManager(capacity, flusher)
// 	return bufMgr
// }

// func MakeSegment(fsMgr ldio.IManager, mtBufMgr, sstBufMgr mgrif.IBufferManager, colIdx int, meta *md.Segment, t *testing.T) col.IColumnSegment {
// 	seg := col.NewColumnSegment(fsMgr, mtBufMgr, sstBufMgr, colIdx, meta)
// 	for _, blkMeta := range meta.Blocks {
// 		blk, err := seg.RegisterBlock(blkMeta)
// 		assert.Nil(t, err)
// 		blk.UnRef()
// 	}
// 	return seg
// }

// func MakeSegments(fsMgr ldio.IManager, mtBufMgr, sstBufMgr mgrif.IBufferManager, meta *md.Table, tblData table.ITableData, t *testing.T) []common.RelationName {
// 	var segIDs []common.RelationName
// 	for _, segMeta := range meta.Segments {
// 		var colSegs []col.IColumnSegment
// 		for colIdx, _ := range segMeta.Schema.ColDefs {
// 			colSeg := MakeSegment(fsMgr, mtBufMgr, sstBufMgr, colIdx, segMeta, t)
// 			colSegs = append(colSegs, colSeg)
// 		}
// 		tblData.AppendColSegments(colSegs)
// 		segIDs = append(segIDs, *segMeta.AsCommonID())
// 	}
// 	return segIDs
// }
