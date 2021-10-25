// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sched

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
	"sync"
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

// metaResourceMgr manages all the running/pending meta events.
type metaResourceMgr struct {
	sched.BaseResourceMgr
	disk sched.ResourceMgr
	cpu  sched.ResourceMgr
	opts *storage.Options
	mu   sync.RWMutex
	// Running meta events and their related info
	runnings struct {
		events     map[uint64]MetaEvent
		tableindex map[uint64]bool
		scopeall   bool
	}
	// Pending meta events
	pendings struct {
		events []MetaEvent
	}
}

func NewMetaResourceMgr(opts *storage.Options, disk, cpu sched.ResourceMgr) *metaResourceMgr {
	mgr := &metaResourceMgr{
		disk: disk,
		cpu:  cpu,
		opts: opts,
	}
	mgr.runnings.events = make(map[uint64]MetaEvent)
	mgr.runnings.tableindex = make(map[uint64]bool)
	mgr.pendings.events = make([]MetaEvent, 0)
	handler := sched.NewPoolHandler(4, mgr.preSubmit)
	mgr.BaseResourceMgr = *sched.NewBaseResourceMgr(handler)
	return mgr
}

func (mgr *metaResourceMgr) OnExecDone(op interface{}) {
	e := op.(MetaEvent)
	mgr.mu.Lock()
	logutil.Debugf("OnExecDone %d: %s", e.ID(), mgr.stringLocked())
	scope, all := e.GetScope()
	delete(mgr.runnings.events, e.ID())
	if all {
		mgr.runnings.scopeall = false
	} else {
		delete(mgr.runnings.tableindex, scope.TableID)
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
	mgr.mu.Unlock()
	if i >= 0 {
		mgr.ExecuteEvent(schedEvent)
	}
}

func (mgr *metaResourceMgr) enqueueRunning(e MetaEvent) {
	scope, all := e.GetScope()
	if all {
		mgr.runnings.scopeall = true
		mgr.runnings.events[e.ID()] = e
		return
	}
	mgr.runnings.events[e.ID()] = e
	mgr.runnings.tableindex[scope.TableID] = true
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
		if len(mgr.runnings.events) > 0 {
			if !fromPending {
				mgr.enqueuePending(e)
			}
			return false
		}
		mgr.enqueueRunning(e)
		return true
	}
	if mgr.runnings.scopeall {
		if !fromPending {
			mgr.enqueuePending(e)
		}
		return false
	}
	if _, ok := mgr.runnings.tableindex[scope.TableID]; !ok {
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
	logutil.Debug(mgr.String())
	return sched
}

func (mgr *metaResourceMgr) String() string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.stringLocked()
}

func (mgr *metaResourceMgr) stringLocked() string {
	s := fmt.Sprintf("<MetaResourceMgr>")
	s = fmt.Sprintf("%s\nRunning: (ScopeAll=%v)(Events=[", s, mgr.runnings.scopeall)
	for eid, e := range mgr.runnings.events {
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
