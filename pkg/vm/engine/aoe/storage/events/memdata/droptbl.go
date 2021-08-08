package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type dropTableEvent struct {
	baseEvent
	TableId uint64
	Data    iface.ITableData
}

func NewDropTableEvent(ctx *Context, tableId uint64) *dropTableEvent {
	e := &dropTableEvent{TableId: tableId}
	e.baseEvent = baseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx.DoneCB, ctx.Waitable),
		Ctx:       ctx,
	}
	return e
}

func (e *dropTableEvent) Execute() error {
	tbl, err := e.Ctx.Tables.DropTable(e.TableId)
	e.Data = tbl
	return err
}
