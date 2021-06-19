package index

import (
	"github.com/pilosa/pilosa/roaring"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
)

type Node struct {
	*bmgr.Node
	RefHelper
	Cols *roaring.Bitmap
}

func NewNode(bufMgr bmgrif.IBufferManager, vf bmgrif.IVFile, constructor buf.MemoryNodeConstructor,
	capacity uint64, cols *roaring.Bitmap) *Node {
	node := new(Node)
	node.Cols = cols
	node.Node = bufMgr.CreateNode(vf, constructor, capacity).(*bmgr.Node)
	node.OnZeroCB = node.close
	node.Ref()
	return node
}

func (node *Node) close() {
	if node.Node != nil {
		node.Node.Close()
	}
}

func (node *Node) ContainsCol(v uint64) bool {
	return node.Cols.Contains(v)
}

func (node *Node) ContainsOnlyCol(v uint64) bool {
	return node.Cols.Contains(v) && node.Cols.Count() == 1
}

func (node *Node) AllCols() []uint64 {
	return node.Cols.Slice()
}
