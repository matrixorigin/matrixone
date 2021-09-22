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

package col

// import (
// 	"matrixone/pkg/vm/engine/aoe/storage/common"
// 	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
// 	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
// 	"runtime"
// 	"sync/atomic"

// 	log "github.com/sirupsen/logrus"
// )

// type StrColumnBlock struct {
// 	ColumnBlock
// 	Parts []IColumnPart
// }

// func NewStrColumnBlock(seg IColumnSegment, id common.RelationName, blkType base.BlockType) IColumnBlock {
// 	blk := &StrColumnBlock{
// 		ColumnBlock: ColumnBlock{
// 			RelationName:     id,
// 			Type:   blkType,
// 			ColIdx: seg.GetColIdx(),
// 		},
// 		Parts: make([]IColumnPart, 0),
// 	}
// 	seg.Append(blk)
// 	runtime.SetFinalizer(blk, func(o IColumnBlock) {
// 		id := o.GetID()
// 		o.SetNext(nil)
// 		log.Infof("[GC]: StrColumnSegment %s [%d]", id.BlockString(), o.GetBlockType())
// 		o.Close()
// 	})
// 	return blk
// }

// func (blk *StrColumnBlock) Ref() IColumnBlock {
// 	atomic.AddInt64(&blk.RefCount, int64(1))
// 	return blk
// }

// func (blk *StrColumnBlock) UnRef() {
// 	if atomic.AddInt64(&blk.RefCount, int64(-1)) == 0 {
// 		blk.Close()
// 	}
// }

// func (blk *StrColumnBlock) CloneWithUpgrade(seg IColumnSegment, meta *md.Block) IColumnBlock {
// 	return nil
// }

// func (blk *StrColumnBlock) Close() error {
// 	// for _, part := range blk.Parts {
// 	// 	err := part.Close()
// 	// 	if err != nil {
// 	// 		return err
// 	// 	}
// 	// }
// 	return nil
// }

// func (blk *StrColumnBlock) GetPartRoot() IColumnPart {
// 	if len(blk.Parts) == 0 {
// 		return nil
// 	}
// 	return blk.Parts[0]
// }

// func (blk *StrColumnBlock) Append(part IColumnPart) {
// 	if !blk.RelationName.IsSameBlock(part.GetID()) {
// 		panic("logic error")
// 	}
// 	if len(blk.Parts) != 0 {
// 		blk.Parts[len(blk.Parts)-1].SetNext(part)
// 	}
// 	blk.Parts = append(blk.Parts, part)
// }

// func (blk *StrColumnBlock) InitScanCursor(cursor *ScanCursor) error {
// 	if len(blk.Parts) != 0 {
// 		return blk.Parts[0].InitScanCursor(cursor)
// 	}
// 	return nil
// }

// func (blk *StrColumnBlock) String() string {
// 	return ""
// }
