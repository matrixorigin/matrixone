package index

import (
	"github.com/pilosa/pilosa/roaring"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
)

type Node struct {
	*bmgr.Node
	Cols *roaring.Bitmap
}

func NewNode(cols *roaring.Bitmap, bufMgr bmgrif.IBufferManager, vf bmgrif.IVFile,
	constructor buf.MemoryNodeConstructor, capacity uint64) *Node {
	node := new(Node)
	node.Cols = cols
	node.Node = bufMgr.CreateNode(vf, constructor, capacity).(*bmgr.Node)
	return node
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
