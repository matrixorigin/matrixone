package db

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
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
