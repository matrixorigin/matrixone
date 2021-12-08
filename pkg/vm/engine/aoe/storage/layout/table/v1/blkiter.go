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

package table

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
)

type BacktrackingBlockIterator struct {
	currColumn   []*vector.Vector
	blocks       []iface.IBlock
	currIdx      uint16
	currBlk      int
	nodes        []*common.MemNode
}

func NewBacktrackingBlockIterator(blocks []iface.IBlock, col uint16) *BacktrackingBlockIterator {
	return &BacktrackingBlockIterator{
		blocks:       blocks,
		currIdx:      col,
		currBlk:      0,
	}
}

func (iter *BacktrackingBlockIterator) FetchColumn() ([]*vector.Vector, error) {
	for i, blk := range iter.blocks {
		vec, err := blk.GetVectorWrapper(int(iter.currIdx))
		if err != nil {
			return nil, err
		}
		iter.currColumn = append(iter.currColumn, &vec.Vector)
		iter.currBlk = i
		iter.nodes = append(iter.nodes, vec.MNode)
	}
	return iter.currColumn, nil
}

func (iter *BacktrackingBlockIterator) Clear() {
	for _, node := range iter.nodes {
		common.GPool.Free(node)
	}
	iter.nodes = iter.nodes[:0]
	iter.currColumn = nil
}

func (iter *BacktrackingBlockIterator) Reset(col uint16) {
	iter.currIdx = col
	iter.currBlk = 0
	iter.Clear()
}

func (iter *BacktrackingBlockIterator) BlockCount() uint32 {
	return uint32(len(iter.blocks))
}

//func (iter *BacktrackingBlockIterator) FinalizeColumn() error {
//	f, _ := os.Create("/tmp/xxxx.tmp")
//	binary.Write(f, binary.BigEndian, len(iter.blocks))
//	for _, vec := range iter.currColumn {
//		buf, _ := vec.Show()
//		binary.Write(f, binary.BigEndian, len(buf))
//		binary.Write(f, binary.BigEndian, buf)
//	}
//	f.Close()
//	iter.Clear()
//	return nil
//}
