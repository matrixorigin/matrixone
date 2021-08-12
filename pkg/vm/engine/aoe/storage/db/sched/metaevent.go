package db

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type MetaEvent interface {
	sched.Event
	GetScope() (common.ID, bool)
}

type metaEvent struct {
	sched.BaseEvent
	scope    common.ID
	scopeall bool
}

func NewMetaEvent(scope common.ID, scopeall bool, t sched.EventType, waitable bool) *metaEvent {
	e := &metaEvent{
		scope:    scope,
		scopeall: scopeall,
	}
	e.BaseEvent = *sched.NewBaseEvent(e, t, nil, waitable)
	return e
}

func (e *metaEvent) GetScope() (common.ID, bool) {
	return e.scope, e.scopeall
}
