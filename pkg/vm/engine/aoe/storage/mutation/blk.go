package mutation

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
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
	Opts      *engine.Options
}

func NewMutableBlockNode(opts *engine.Options, mgr base.INodeManager, file *dataio.TransientBlockFile,
	tabledata iface.ITableData, meta *metadata.Block) *MutableBlockNode {
	n := &MutableBlockNode{
		Node:      *buffer.NewNode(mgr, *meta.AsCommonID(), 0),
		File:      file,
		Meta:      meta,
		TableData: tabledata,
		Opts:      opts,
	}
	n.UnloadFunc = n.unload
	n.LoadFunc = n.load
	return n
}

func (n *MutableBlockNode) load() {
	n.Data = n.File.LoadBatch(n.Meta)
	// logutil.S().Infof("%s loaded %d", n.Meta.AsCommonID().BlockString(), n.Data.Length())
}

func (n *MutableBlockNode) unload() {
	// logutil.S().Infof("%s presyncing %d", n.Meta.AsCommonID().BlockString(), n.Data.Length())
	ok := n.File.PreSync(uint32(n.Data.Length()))
	if !ok {
		return
	}
	meta := n.Meta.Copy()
	ctx := &sched.Context{Opts: n.Opts, Waitable: true}
	n.Ref()
	defer n.Unref()
	e := sched.NewFlushTransientBlockEvent(ctx, n, n.Data, meta, n.File)
	n.Opts.Scheduler.Schedule(e)
	if err := e.WaitDone(); err != nil {
		panic(err)
	}
	n.Data.Close()
	n.Data = nil
}
