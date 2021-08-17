package meta

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// "matrixone/pkg/vm/engine/aoe"
	// md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

type Context struct {
	Opts     *e.Options
	DoneCB   ops.OpDoneCB
	Waitable bool
}

type BaseEvent struct {
	sched.BaseEvent
	Ctx *Context
}
