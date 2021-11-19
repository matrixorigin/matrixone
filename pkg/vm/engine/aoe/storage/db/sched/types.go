package sched

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type BaseEvent struct {
	sched.BaseEvent
	Ctx *iface.Context
}
