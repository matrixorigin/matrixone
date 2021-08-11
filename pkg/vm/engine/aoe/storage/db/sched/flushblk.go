package db

import (
	log "github.com/sirupsen/logrus"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type flushMemtableEvent struct {
	baseEvent
	MemTable   imem.IMemTable
	Collection imem.ICollection
	Meta       *md.Block
}

func NewFlushMemtableEvent(ctx *Context, memtable imem.IMemTable, collection imem.ICollection) *flushMemtableEvent {
	e := &flushMemtableEvent{
		MemTable:   memtable,
		Collection: collection,
	}
	e.baseEvent = baseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushMemtableTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushMemtableEvent) Execute() error {
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
	e.Meta = mem.GetMeta()
	defer mem.Unref()
	err := mem.Flush()
	if err != nil {
		log.Errorf("Flush memtable %d failed %s", mem.GetMeta().GetID(), err)
		return err
	}
	return err
}
