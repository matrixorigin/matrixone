package dataio

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	// bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/base"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	// mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	// md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type Context struct {
	Opts     *e.Options
	DoneCB   ops.OpDoneCB
	Waitable bool
}

type baseEvent struct {
	sched.BaseEvent
	Ctx *Context
}
