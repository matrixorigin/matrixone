package mutation

import (
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type blockFlusher struct{}

func (f blockFlusher) flush(node base.INode, data batch.IBatch, meta *metadata.Block, file *dataio.TransientBlockFile) error {
	return file.Sync(data, meta)
}

type MutableBlockNode struct {
	buffer.Node
	TableData iface.ITableData
	Meta      *metadata.Block
	File      *dataio.TransientBlockFile
	Data      batch.IBatch
	Flusher   mb.BlockFlusher
}

func NewMutableBlockNode(mgr base.INodeManager, file *dataio.TransientBlockFile,
	tabledata iface.ITableData, meta *metadata.Block, flusher mb.BlockFlusher) *MutableBlockNode {
	if flusher == nil {
		t := blockFlusher{}
		flusher = t.flush
	}
	n := &MutableBlockNode{
		File:      file,
		Meta:      meta,
		TableData: tabledata,
		Flusher:   flusher,
	}
	n.Node = *buffer.NewNode(n, mgr, *meta.AsCommonID(), 0)
	n.UnloadFunc = n.unload
	n.LoadFunc = n.load
	return n
}

func (n *MutableBlockNode) Flush() error {
	n.RLock()
	currSize := n.Data.Length()
	if ok := n.File.PreSync(uint32(currSize)); !ok {
		n.RUnlock()
		return nil
	}
	cols := len(n.Meta.Segment.Table.Schema.ColDefs)
	attrs := make([]int, cols)
	vecs := make([]vector.IVector, cols)
	for i, _ := range n.Meta.Segment.Table.Schema.ColDefs {
		attrs[i] = i
		vecs[i] = n.Data.GetVectorByAttr(i).GetLatestView()
	}
	data := batch.NewBatch(attrs, vecs)
	meta := n.Meta.Copy()
	n.RUnlock()
	return n.Flusher(n, data, meta, n.File)
}

func (n *MutableBlockNode) load() {
	n.Data = n.File.LoadBatch(n.Meta)
	// logutil.Infof("%s loaded %d", n.Meta.AsCommonID().BlockString(), n.Data.Length())
}

func (n *MutableBlockNode) unload() {
	// logutil.Infof("%s presyncing %d", n.Meta.AsCommonID().BlockString(), n.Data.Length())
	if ok := n.File.PreSync(uint32(n.Data.Length())); ok {
		if err := n.Flusher(n, n.Data, n.Meta.Copy(), n.File); err != nil {
			panic(err)
		}
	}
	n.Data.Close()
	n.Data = nil
}

func (n *MutableBlockNode) GetData() batch.IBatch {
	return n.Data
}

func (n *MutableBlockNode) GetFile() *dataio.TransientBlockFile {
	return n.File
}
