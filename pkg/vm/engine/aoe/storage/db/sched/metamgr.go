package db

import (
	"errors"
	log "github.com/sirupsen/logrus"
	e "matrixone/pkg/vm/engine/aoe/storage"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
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

func (mgr *metaResourceMgr) OnExecDone(op iops.IOp) {
	log.Infof("OnExecDone %v", op)
}

func (mgr *metaResourceMgr) preSubmit(op iops.IOp) bool {
	e := op.(sched.Event)
	if !isMetaEvent(e.Type()) {
		panic(ErrUnexpectEventType)
	}
	e.AddObserver(mgr)
	return true
}
