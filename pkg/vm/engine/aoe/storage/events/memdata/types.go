package memdata

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type Context struct {
	Opts   *e.Options
	Tables *table.Tables
	DoneCB func()
}

type baseEvent struct {
	sched.BaseEvent
	Ctx *Context
}
