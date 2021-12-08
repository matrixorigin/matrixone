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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
)

type BlockIndexHolder struct {
	common.RefHelper
	ID common.ID
	self struct {
		sync.RWMutex
		colIndices map[int][]*Node
    }
	BufMgr      mgrif.IBufferManager
	Inited      bool
	PostCloseCB PostCloseCB
}

func newBlockHolder(bufMgr mgrif.IBufferManager, id common.ID, t base.BlockType, cb PostCloseCB) *BlockIndexHolder {
	holder := &BlockIndexHolder{
		ID:          id,
		BufMgr:      bufMgr,
		Inited:      false,
		PostCloseCB: cb,
	}
	holder.self.colIndices = make(map[int][]*Node)
	holder.OnZeroCB = holder.close
	holder.Ref()
	return holder
}

func (holder *BlockIndexHolder) Init(segFile base.ISegmentFile) {
	holder.self.Lock()
	defer holder.self.Unlock()
	if holder.Inited {
		panic("logic error")
	}
	indicesMeta := segFile.GetBlockIndicesMeta(holder.ID)
	if indicesMeta == nil || len(indicesMeta.Data) == 0 {
		return
	}
	// init embed segment indices
	for _, meta := range indicesMeta.Data {
		vf := segFile.MakeVirtualIndexFile(meta)
		col := int(meta.Cols.ToArray()[0])
		var node *Node
		switch meta.Type {
		case base.ZoneMap:
			node = newNode(holder.BufMgr, vf, false, BlockZoneMapIndexConstructor, meta.Cols, nil)
		default:
			panic("unsupported embedded index type")
		}
		idxes, ok := holder.self.colIndices[col]
		if !ok {
			idxes = make([]*Node, 0)
			holder.self.colIndices[col] = idxes
		}
		holder.self.colIndices[col] = append(holder.self.colIndices[col], node)
		logutil.Infof("[BLK] Zone map load successfully, current indices count for column %d: %d | %s", col, len(holder.self.colIndices[col]), holder.ID.BlockString())
	}
	holder.Inited = true
}

func (holder *BlockIndexHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok {
		// no indices found, just skip the block.
		ctx.BoolRes = true
		return nil
	}
	var err error
	for _, idx := range idxes {
		node := idx.GetManagedNode()
		err = node.DataNode.(Index).Eval(ctx)
		if err != nil {
			node.Close()
			return err
		}
		node.Close()
	}
	return nil
}

func (holder *BlockIndexHolder) close() {
	for _, idxes := range holder.self.colIndices {
		for _, idx := range idxes {
			idx.Unref()
		}
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}

func (holder *BlockIndexHolder) stringNoLock() string {
	//s := fmt.Sprintf("<IndexBlkHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.BlockString(), holder.Type, len(holder.Indices), holder.RefCount())
	//for _, i := range holder.Indices {
	//	s = fmt.Sprintf("%s\n\tIndex: [RefCount=%d]", s, i.RefCount())
	//}
	// s = fmt.Sprintf("%s\n%vs, holder.colIndices)
	// TODO(zzl)
	return ""
}

