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

import (
	"bytes"
	"fmt"
	ro "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"time"
)

type stdColumnBlock struct {
	columnBlock
	part IColumnPart
}

func NewStdColumnBlock(host iface.IBlock, colIdx int) IColumnBlock {
	defer host.Unref()
	blk := &stdColumnBlock{
		columnBlock: columnBlock{
			colIdx:  colIdx,
			meta:    host.GetMeta(),
			segFile: host.GetSegmentFile(),
			typ:     host.GetType(),
		},
	}
	capacity := metadata.EstimateColumnBlockSize(colIdx, blk.meta)
	host.Ref()
	blk.Ref()
	part := NewColumnPart(host, blk, capacity)
	for part == nil {
		blk.Ref()
		host.Ref()
		part = NewColumnPart(host, blk, capacity)
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	part.Unref()
	blk.OnZeroCB = blk.close
	blk.Ref()
	return blk
}

func (blk *stdColumnBlock) CloneWithUpgrade(host iface.IBlock) IColumnBlock {
	defer host.Unref()
	if blk.typ == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if host.GetMeta().CommitInfo.Op != metadata.OpUpgradeFull {
		panic(fmt.Sprintf("logic error: blk %s not upgraded", host.GetMeta().AsCommonID().BlockString()))
	}
	cloned := &stdColumnBlock{
		columnBlock: columnBlock{
			typ:         host.GetType(),
			colIdx:      blk.colIdx,
			meta:        host.GetMeta(),
			//indexHolder: host.GetIndexHolder(),
			segFile:     host.GetSegmentFile(),
		},
	}
	cloned.Ref()
	blk.RLock()
	part := blk.part.CloneWithUpgrade(cloned, host.GetSSTBufMgr())
	blk.RUnlock()
	if part == nil {
		panic("logic error")
	}
	cloned.part = part
	cloned.OnZeroCB = cloned.close
	cloned.Ref()
	return cloned
}

func (blk *stdColumnBlock) RegisterPart(part IColumnPart) {
	blk.Lock()
	defer blk.Unlock()
	if blk.meta.Id != part.GetID() || blk.part != nil {
		panic("logic error")
	}
	blk.part = part
}

func (blk *stdColumnBlock) close() {
	if blk.indexHolder != nil {
		blk.indexHolder.Unref()
		blk.indexHolder = nil
	}
	if blk.part != nil {
		blk.part.Close()
	}
	blk.part = nil
	// log.Infof("destroy colblk %d, colidx %d", blk.meta.RelationName, blk.colIdx)
}

func (blk *stdColumnBlock) LoadVectorWrapper() (*vector.VectorWrapper, error) {
	return blk.part.LoadVectorWrapper()
}

func (blk *stdColumnBlock) ForceLoad(compressed, deCompressed *bytes.Buffer) (*ro.Vector, error) {
	return blk.part.ForceLoad(compressed, deCompressed)
}

func (blk *stdColumnBlock) Prefetch() error {
	return blk.part.Prefetch()
}

func (blk *stdColumnBlock) GetVector() vector.IVector {
	return blk.part.GetVector()
}

func (blk *stdColumnBlock) GetVectorReader() dbi.IVectorReader {
	return blk.part.GetVector().(dbi.IVectorReader)
}

func (blk *stdColumnBlock) Size() uint64 {
	return blk.part.Size()
}

func (blk *stdColumnBlock) String() string {
	s := fmt.Sprintf("<Std[%s](T=%s)(RefCount=%d)(Size=%d)>", blk.meta.String(), blk.typ.String(), blk.RefCount(), blk.meta.Count)
	return s
}
