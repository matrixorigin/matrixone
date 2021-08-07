package meta

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// "matrixone/pkg/vm/engine/aoe"
	// md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

type Context struct {
	Opts *e.Options
}

type baseEvent struct {
	sched.BaseEvent
	Ctx *Context
}
