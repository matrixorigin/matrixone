package db

import (
	"errors"
	"fmt"
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
	e := op.(MetaEvent)
	log.Infof("OnExecDone %d", e.ID())
	mgr.mu.Lock()
	scope, all := e.GetScope()
	log.Info(mgr.stringLocked())
	delete(mgr.runings.events, e.ID())
	if all {
		mgr.runings.scopeall = false
	} else {
		delete(mgr.runings.tableindex, scope.TableID)
	}
	if len(mgr.pendings.events) == 0 {
		mgr.mu.Unlock()
		return
	}
	var schedEvent MetaEvent
	var i int
	for i = len(mgr.pendings.events) - 1; i >= 0; i-- {
		schedEvent = mgr.pendings.events[i]
		if mgr.tryScheduleLocked(schedEvent, true) {
			mgr.pendings.events = append(mgr.pendings.events[:i], mgr.pendings.events[i+1:]...)
			break
		}
	}
	if i < 0 {
		panic("logic error")
	}
	mgr.mu.Unlock()
	mgr.ExecuteEvent(schedEvent)
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

func (mgr *metaResourceMgr) trySchedule(e MetaEvent, fromPending bool) bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.tryScheduleLocked(e, fromPending)
}

func (mgr *metaResourceMgr) tryScheduleLocked(e MetaEvent, fromPending bool) bool {
	scope, all := e.GetScope()
	if all {
		if len(mgr.runings.events) > 0 {
			if !fromPending {
				mgr.enqueuePending(e)
			}
			return false
		}
		mgr.enqueueRunning(e)
		return true
	}
	if mgr.runings.scopeall {
		if !fromPending {
			mgr.enqueuePending(e)
		}
		return false
	}
	if _, ok := mgr.runings.tableindex[scope.TableID]; !ok {
		mgr.enqueueRunning(e)
		return true
	}
	if !fromPending {
		mgr.enqueuePending(e)
	}
	return false
}

func (mgr *metaResourceMgr) preSubmit(op iops.IOp) bool {
	e := op.(MetaEvent)
	if !isMetaEvent(e.Type()) {
		panic(ErrUnexpectEventType)
	}
	e.AddObserver(mgr)
	sched := mgr.trySchedule(e, false)
	log.Info(mgr.String())
	return sched
}

func (mgr *metaResourceMgr) String() string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.stringLocked()
}

func (mgr *metaResourceMgr) stringLocked() string {
	s := fmt.Sprintf("<MetaResourceMgr>")
	s = fmt.Sprintf("%s\nRunning: (ScopeAll=%v)(Events=[", s, mgr.runings.scopeall)
	for eid, e := range mgr.runings.events {
		scope, _ := e.GetScope()
		s = fmt.Sprintf("%s(%d,%d)", s, eid, scope.TableID)
	}
	s = fmt.Sprintf("%s])", s)
	s = fmt.Sprintf("%s\nPending: (Events=[", s)
	for _, e := range mgr.pendings.events {
		s = fmt.Sprintf("%s%d,", s, e.ID())
	}
	s = fmt.Sprintf("%s])", s)
	return s
}
