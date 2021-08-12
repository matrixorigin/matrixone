package memdata

import (
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type createTableEvent struct {
	baseEvent
	Collection imem.ICollection
}

func NewCreateTableEvent(ctx *Context) *createTableEvent {
	e := &createTableEvent{}
	e.baseEvent = baseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx.DoneCB, ctx.Waitable),
		Ctx:       ctx,
	}
	return e
}

func (e *createTableEvent) Execute() error {
	collection := e.Ctx.MTMgr.StrongRefCollection(e.Ctx.TableMeta.ID)
	if collection != nil {
		e.Collection = collection
		return nil
	}
	meta := e.Ctx.TableMeta

	tableData, err := e.Ctx.Tables.StrongRefTable(meta.ID)
	if err != nil {
		tableData = table.NewTableData(e.Ctx.FsMgr, e.Ctx.IndexBufMgr, e.Ctx.MTBufMgr, e.Ctx.SSTBufMgr, meta)
		err = e.Ctx.Tables.CreateTable(tableData)
		if err != nil {
			return err
		}
	}
	tableData.Ref()
	collection, err = e.Ctx.MTMgr.RegisterCollection(tableData)
	if err != nil {
		return err
	}

	e.Collection = collection

	return nil
}
