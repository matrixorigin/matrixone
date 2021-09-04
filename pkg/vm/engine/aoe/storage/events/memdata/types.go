package memdata

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type Context struct {
	Opts                             *e.Options
	DoneCB                           ops.OpDoneCB
	Waitable                         bool
	Tables                           *table.Tables
	MTMgr                            mtif.IManager
	IndexBufMgr, MTBufMgr, SSTBufMgr bmgrif.IBufferManager
	FsMgr                            base.IManager
	TableMeta                        *md.Table
}

type BaseEvent struct {
	sched.BaseEvent
	Ctx *Context
}
