package db

import (
	"errors"
	e "matrixone/pkg/vm/engine/aoe/storage"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	"sync"

	log "github.com/sirupsen/logrus"
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
	disk    sched.ResourceMgr
	cpu     sched.ResourceMgr
	opts    *e.Options
	mu      sync.RWMutex
	runings struct {
		events     map[uint64]MetaEvent
		tableindex map[uint64]bool
		scopeall   bool
	}
	pendings struct {
		events []MetaEvent
	}
}

func NewMetaResourceMgr(opts *e.Options, disk, cpu sched.ResourceMgr) *metaResourceMgr {
	mgr := &metaResourceMgr{
		disk: disk,
		cpu:  cpu,
		opts: opts,
	}
	mgr.runings.events = make(map[uint64]MetaEvent)
	mgr.runings.tableindex = make(map[uint64]bool)
	mgr.pendings.events = make([]MetaEvent, 0)
	handler := sched.NewPoolHandler(4, mgr.preSubmit)
	mgr.BaseResourceMgr = *sched.NewBaseResourceMgr(handler)
	return mgr
}

func (mgr *metaResourceMgr) OnExecDone(op interface{}) {
	log.Infof("OnExecDone %v", op)
	e := op.(MetaEvent)
	log.Infof("OnExecDone %d", e.ID())
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	scope, all := e.GetScope()
	delete(mgr.runings.events, e.ID())
	if all {
		mgr.runings.scopeall = false
	} else {
		delete(mgr.runings.tableindex, scope.TableID)
	}
}

func (mgr *metaResourceMgr) enqueueRunning(e MetaEvent) {
	scope, all := e.GetScope()
	if all {
		mgr.runings.scopeall = true
		mgr.runings.events[e.ID()] = e
		return
	}
	mgr.runings.events[e.ID()] = e
	mgr.runings.tableindex[scope.TableID] = true
}

func (mgr *metaResourceMgr) enqueuePending(e MetaEvent) {
	mgr.pendings.events = append(mgr.pendings.events, e)
}

func (mgr *metaResourceMgr) trySchedule(e MetaEvent) bool {
	scope, all := e.GetScope()
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if all {
		if len(mgr.runings.events) > 0 {
			mgr.enqueuePending(e)
			return false
		}
		mgr.enqueueRunning(e)
		return true
	}
	if mgr.runings.scopeall {
		mgr.enqueuePending(e)
		return false
	}
	if _, ok := mgr.runings.tableindex[scope.TableID]; !ok {
		mgr.enqueueRunning(e)
		return true
	}

	return false
}

func (mgr *metaResourceMgr) preSubmit(op iops.IOp) bool {
	e := op.(MetaEvent)
	if !isMetaEvent(e.Type()) {
		panic(ErrUnexpectEventType)
	}
	e.AddObserver(mgr)
	return mgr.trySchedule(e)
}
