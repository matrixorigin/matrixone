package db

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
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
