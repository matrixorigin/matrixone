package db

import (
	"errors"
	e "matrixone/pkg/vm/engine/aoe/storage"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

var (
	ErrUnexpectEventType = errors.New("aoe: unexpect event type")
)

var (
	metaEvents = map[sched.EventType]bool{
		sched.MockEvent:       true,
		sched.MetaUpdateEvent: true,
	}
)

func isMetaEvent(t sched.EventType) bool {
	_, ok := metaEvents[t]
	return ok
}

type metaResourceMgr struct {
	sched.BaseResourceMgr
	disk sched.ResourceMgr
	cpu  sched.ResourceMgr
	opts *e.Options
}

func NewMetaResourceMgr(opts *e.Options, disk, cpu sched.ResourceMgr) *metaResourceMgr {
	mgr := &metaResourceMgr{
		disk: disk,
		cpu:  cpu,
		opts: opts,
	}
	handler := sched.NewPoolHandler(4, mgr.preSubmit)
	mgr.BaseResourceMgr = *sched.NewBaseResourceMgr(handler)
	return mgr
}

func (mgr *metaResourceMgr) preSubmit(op iops.IOp) bool {
	e := op.(sched.Event)
	if !isMetaEvent(e.Type()) {
		panic(ErrUnexpectEventType)
	}
	return true
}
