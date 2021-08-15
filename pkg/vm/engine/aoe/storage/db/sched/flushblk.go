package db

import (
	log "github.com/sirupsen/logrus"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type flushMemtableEvent struct {
	BaseEvent
	Meta       *md.Block
	Collection imem.ICollection
}

func NewFlushMemtableEvent(ctx *Context, collection imem.ICollection) *flushMemtableEvent {
	e := &flushMemtableEvent{
		Collection: collection,
	}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushMemtableTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushMemtableEvent) Execute() error {
	defer e.Collection.Unref()
	mem := e.Collection.FetchImmuTable()
	if mem == nil {
		return nil
	}
	defer mem.Unref()
	e.Meta = mem.GetMeta()
	err := mem.Flush()
	if err != nil {
		log.Errorf("Flush memtable %d failed %s", e.Meta.ID, err)
		return err
	}
	return err
}
