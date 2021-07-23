package index

import (
	"github.com/RoaringBitmap/roaring"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

type Node struct {
	*bmgr.Node
	common.RefHelper
	Cols        *roaring.Bitmap
	PostCloseCB PostCloseCB
}

func newNode(bufMgr bmgrif.IBufferManager, vf common.IVFile, useCompress bool, constructor buf.MemoryNodeConstructor,
	cols *roaring.Bitmap, cb PostCloseCB) *Node {
	node := new(Node)
	node.Cols = cols
	node.Node = bufMgr.CreateNode(vf, useCompress, constructor).(*bmgr.Node)
	node.OnZeroCB = node.close
	node.PostCloseCB = cb
	node.Ref()
	return node
}

func (node *Node) close() {
	if node.Node != nil {
		node.Node.Close()
	}
	if node.PostCloseCB != nil {
		node.PostCloseCB(node)
	}
}

func (node *Node) ContainsCol(v uint64) bool {
	return node.Cols.Contains(uint32(v))
}

func (node *Node) ContainsOnlyCol(v uint64) bool {
	return node.Cols.Contains(uint32(v)) && node.Cols.GetCardinality() == 1
}

func (node *Node) AllCols() []uint32 {
	return node.Cols.ToArray()
}
