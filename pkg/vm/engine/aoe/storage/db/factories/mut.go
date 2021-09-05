package factories

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type nodeFlusher struct {
	opts *engine.Options
}

func (f nodeFlusher) flush(node bb.INode, data batch.IBatch, meta *metadata.Block, file *dataio.TransientBlockFile) error {
	ctx := &sched.Context{Opts: f.opts, Waitable: true}
	node.Ref()
	defer node.Unref()
	e := sched.NewFlushTransientBlockEvent(ctx, node, data, meta, file)
	f.opts.Scheduler.Schedule(e)
	return e.WaitDone()
}

type mutFactory struct {
	flusher *nodeFlusher
	mgr     bb.INodeManager
}

func NewMutFactory(opts *engine.Options, mgr bb.INodeManager) *mutFactory {
	f := &mutFactory{
		mgr: mgr,
		flusher: &nodeFlusher{
			opts: opts,
		},
	}
	return f
}

func (f *mutFactory) CreateNodeFactory(tdata iface.ITableData) base.NodeFactory {
	return newMutNodeFactory(f, tdata)
}
