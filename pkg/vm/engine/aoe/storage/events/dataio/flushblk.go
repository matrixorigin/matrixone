package dataio

import (
	log "github.com/sirupsen/logrus"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type flushBlkEvent struct {
	baseEvent
	MemTable   imem.IMemTable
	Collection imem.ICollection
}

func NewFlushBlkEvent(ctx *Context, memtable imem.IMemTable, collection imem.ICollection) *flushBlkEvent {
	e := &flushBlkEvent{
		MemTable:   memtable,
		Collection: collection,
	}
	e.baseEvent = baseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.IOBoundEvent, ctx.DoneCB),
	}
	return e
}

func (e *flushBlkEvent) Execute() error {
	if e.Collection != nil {
		defer e.Collection.Unref()
	}
	var mem imem.IMemTable
	if e.MemTable != nil {
		mem = e.MemTable
	} else if e.Collection != nil {
		mem = e.Collection.FetchImmuTable()
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
