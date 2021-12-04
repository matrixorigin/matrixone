package table

import (
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"os"
)

type BlockIterator struct {
	currColumn   []*vector.Vector
	blocks       []iface.IBlock
	currIdx      uint16
	currBlk      int
	nodes        []*common.MemNode
}

func NewBlockIterator(blocks []iface.IBlock, col uint16) *BlockIterator {
	return &BlockIterator{
		blocks:       blocks,
		currIdx:      col,
		currBlk:      0,
	}
}

func (iter *BlockIterator) FetchColumn() ([]*vector.Vector, error) {
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

func (iter *BlockIterator) Clear() {
	for _, node := range iter.nodes {
		common.GPool.Free(node)
	}
	iter.nodes = iter.nodes[:0]
	iter.currColumn = nil
}

func (iter *BlockIterator) Reset(col uint16) {
	iter.currIdx = col
	iter.currBlk = 0
	iter.Clear()
}

func (iter *BlockIterator) BlockCount() uint32 {
	return uint32(len(iter.blocks))
}

func (iter *BlockIterator) FinalizeColumn() error {
	f, _ := os.Create("/tmp/xxxx.tmp")
	binary.Write(f, binary.BigEndian, len(iter.blocks))
	for _, vec := range iter.currColumn {
		buf, _ := vec.Show()
		binary.Write(f, binary.BigEndian, len(buf))
		binary.Write(f, binary.BigEndian, buf)
	}
	f.Close()
	iter.Clear()
	return nil
}
