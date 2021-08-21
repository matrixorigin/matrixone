package mutation

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type MutableBlockNode struct {
	buffer.Node
	TableData iface.ITableData
	Meta      *metadata.Block
	File      *dataio.TransientBlockFile
	Data      batch.IBatch
}

func NewMutableBlockNode(mgr base.INodeManager, file *dataio.TransientBlockFile,
	tabledata iface.ITableData, meta *metadata.Block) *MutableBlockNode {
	n := &MutableBlockNode{
		Node:      *buffer.NewNode(mgr, *meta.AsCommonID(), 0),
		File:      file,
		Meta:      meta,
		TableData: tabledata,
	}
	n.UnloadFunc = n.unload
	n.LoadFunc = n.load
	return n
}

func (n *MutableBlockNode) load() {
	n.Data = n.File.LoadBatch(n.Meta)
}

func (n *MutableBlockNode) unload() {
	ok := n.File.PreSync(uint32(n.Data.Length()))
	if !ok {
		return
	}
	vecs := make([]*vector.Vector, len(n.Meta.Segment.Table.Schema.ColDefs))
	for i, _ := range n.Meta.Segment.Table.Schema.ColDefs {
		iv := n.Data.GetVectorByAttr(i)
		vecs[i] = iv.GetLatestView().CopyToVector()
	}
	Meta := n.Meta.Copy()
	n.File.Sync(vecs, Meta, Meta.Segment.Table.Conf.Dir)
	n.Data.Close()
	n.Data = nil
}
