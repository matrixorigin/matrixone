package factories

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mutation"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type mutBlockFlusher struct {
	opts *engine.Options
}

func (f mutBlockFlusher) flush(node bb.INode, data batch.IBatch, meta *metadata.Block, file *dataio.TransientBlockFile) error {
	ctx := &sched.Context{Opts: f.opts, Waitable: true}
	node.Ref()
	defer node.Unref()
	e := sched.NewFlushTransientBlockEvent(ctx, node, data, meta, file)
	f.opts.Scheduler.Schedule(e)
	return e.WaitDone()
}

type mutBlockNodeFactory struct {
	flusher *mutBlockFlusher
	mgr     bb.INodeManager
	tdata   iface.ITableData
}

func NewMutBlockNodeFactory(opts *engine.Options, mgr bb.INodeManager, tdata iface.ITableData) *mutBlockNodeFactory {
	f := &mutBlockNodeFactory{
		mgr:   mgr,
		tdata: tdata,
		flusher: &mutBlockFlusher{
			opts: opts,
		},
	}
	return f
}

func (f *mutBlockNodeFactory) GetManager() bb.INodeManager {
	return f.mgr
}

func (f *mutBlockNodeFactory) CreateNode(segfile base.ISegmentFile, meta *metadata.Block) *mutation.MutableBlockNode {
	blkfile := dataio.NewTBlockFile(segfile, *meta.AsCommonID())
	return mutation.NewMutableBlockNode(f.mgr, blkfile, f.tdata, meta, f.flusher.flush)
}
