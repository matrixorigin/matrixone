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

package index

import (
	"fmt"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
)

type BlockHolder struct {
	common.RefHelper
	ID common.ID
	sync.RWMutex
	Indices     []*Node
	ColIndices  map[int][]int
	Type        base.BlockType
	BufMgr      mgrif.IBufferManager
	Inited      bool
	PostCloseCB PostCloseCB
}

func newBlockHolder(bufMgr mgrif.IBufferManager, id common.ID, t base.BlockType, cb PostCloseCB) *BlockHolder {
	holder := &BlockHolder{
		ID:          id,
		Type:        t,
		BufMgr:      bufMgr,
		Inited:      false,
		PostCloseCB: cb,
	}
	holder.ColIndices = make(map[int][]int)
	holder.Indices = make([]*Node, 0)
	holder.OnZeroCB = holder.close
	holder.Ref()
	return holder
}

func (holder *BlockHolder) Init(segFile base.ISegmentFile) {
	if holder.Inited {
		panic("logic error")
	}
	indicesMeta := segFile.GetBlockIndicesMeta(holder.ID)
	if indicesMeta == nil {
		return
	}
	for _, meta := range indicesMeta.Data {
		vf := segFile.MakeVirtualBlkIndexFile(&holder.ID, meta)
		col := int(meta.Cols.ToArray()[0])
		node := newNode(holder.BufMgr, vf, false, ZoneMapIndexConstructor, meta.Cols, nil)
		idxes, ok := holder.ColIndices[col]
		if !ok {
			idxes = make([]int, 0)
			holder.ColIndices[col] = idxes
		}
		holder.ColIndices[col] = append(holder.ColIndices[col], len(holder.Indices))
		holder.Indices = append(holder.Indices, node)
	}
	holder.Inited = true
}

func (holder *BlockHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	idxes, ok := holder.ColIndices[colIdx]
	if !ok {
		// TODO
		ctx.BoolRes = true
		return nil
	}
	var err error
	for _, idx := range idxes {
		node := holder.Indices[idx].GetManagedNode()
		err = node.DataNode.(Index).Eval(ctx)
		if err != nil {
			node.Close()
			return err
		}
		node.Close()
	}
	return nil
}

func (holder *BlockHolder) close() {
	for _, index := range holder.Indices {
		index.Unref()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}

func (holder *BlockHolder) GetIndexNode(idx int) *Node {
	node := holder.Indices[idx]
	node.Ref()
	return node
}

func (holder *BlockHolder) Any() bool {
	return len(holder.Indices) > 0
}

func (holder *BlockHolder) IndexCount() int {
	return len(holder.Indices)
}

func (holder *BlockHolder) String() string {
	holder.RLock()
	defer holder.RUnlock()
	return holder.stringNoLock()
}

func (holder *BlockHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexBlkHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.BlockString(), holder.Type, len(holder.Indices), holder.RefCount())
	for _, i := range holder.Indices {
		s = fmt.Sprintf("%s\n\tIndex: [RefCount=%d]", s, i.RefCount())
	}
	// s = fmt.Sprintf("%s\n%vs, holder.ColIndices)
	return s
}
