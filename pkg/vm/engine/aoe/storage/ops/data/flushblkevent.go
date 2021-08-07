package data

import (
	log "github.com/sirupsen/logrus"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type FlushBlkEvent struct {
	sched.BaseEvent
	ctx *OpCtx
}

func NewFlushBlkEvent(ctx *OpCtx) *FlushBlkEvent {
	e := &FlushBlkEvent{
		ctx: ctx,
	}
	e.BaseEvent = *sched.NewBaseEvent(e, sched.DataIOBoundEvent, func() {})
	return e
}

func (e *FlushBlkEvent) Execute() error {
	if e.ctx.Collection != nil {
		defer e.ctx.Collection.Unref()
	}
	var mem imem.IMemTable
	if e.ctx.MemTable != nil {
		mem = e.ctx.MemTable
	} else if e.ctx.Collection != nil {
		mem = e.ctx.Collection.FetchImmuTable()
		if mem == nil {
			return nil
		}
	} else {
		return nil
	}
	defer mem.Unref()
	err := mem.Flush()
	if err != nil {
		log.Errorf("Flush memtable %d failed %s", mem.GetMeta().GetID(), err)
		return err
	}
	err = mem.Commit()
	if err != nil {
		log.Errorf("Commit memtable %d failed %s", mem.GetMeta().GetID(), err)
		return err
	}
	return nil
}
