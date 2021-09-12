package sched

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// "matrixone/pkg/logutil"
)

type flushMemblockEvent struct {
	BaseEvent
	Block iface.IMutBlock
	Meta  *metadata.Block
}

func NewFlushMemBlockEvent(ctx *Context, blk iface.IMutBlock) *flushMemblockEvent {
	e := &flushMemblockEvent{
		Block: blk,
		Meta:  blk.GetMeta(),
	}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushBlkTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushMemblockEvent) Execute() error {
	defer e.Block.Unref()
	return e.Block.WithPinedContext(func(mut mb.IMutableBlock) error {
		meta := mut.GetMeta()
		data := mut.GetData()
		var vecs []*vector.Vector
		for attri, _ := range data.GetAttrs() {
			v := data.GetVectorByAttr(attri).GetLatestView()
			vecs = append(vecs, v.CopyToVector())
		}

		bw := dataio.NewBlockWriter(vecs, meta, meta.Segment.Table.Conf.Dir)
		bw.SetPreExecutor(func() {
			logutil.Infof(" %s | Memtable | Flushing", bw.GetFileName())
		})
		bw.SetPostExecutor(func() {
			logutil.Infof(" %s | Memtable | Flushed", bw.GetFileName())
		})
		return bw.Execute()
	})
}
