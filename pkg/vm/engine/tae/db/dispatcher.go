package db

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

func ScopeConflictCheck(oldScope, newScope *common.ID) (err error) {
	if oldScope.TableID != newScope.TableID {
		return
	}
	if oldScope.SegmentID != newScope.SegmentID {
		if oldScope.SegmentID != 0 && newScope.SegmentID != 0 {
			return
		}
		return tasks.ErrScheduleScopeConflict
	}
	if oldScope.BlockID != oldScope.BlockID && oldScope.BlockID != 0 && newScope.BlockID != 0 {
		return
	}
	return tasks.ErrScheduleScopeConflict
}

type asyncJobDispatcher struct {
	sync.RWMutex
	*tasks.BaseDispatcher
	actives map[common.ID]bool
}

func newAsyncJobDispatcher() *asyncJobDispatcher {
	return &asyncJobDispatcher{
		actives:        make(map[common.ID]bool),
		BaseDispatcher: tasks.NewBaseDispatcher(),
	}
}

func (dispatcher *asyncJobDispatcher) checkConflictLocked(scopes []common.ID) (err error) {
	for active, _ := range dispatcher.actives {
		for _, scope := range scopes {
			if err = ScopeConflictCheck(&active, &scope); err != nil {
				break
			}
		}
	}
	return
}

func (dispatcher *asyncJobDispatcher) TryDispatch(task tasks.Task) (err error) {
	mscoped := task.(tasks.MScopedTask)
	scopes := mscoped.Scopes()
	if scopes == nil || len(scopes) == 0 {
		dispatcher.Dispatch(task)
		return nil
	}
	dispatcher.Lock()
	if err = dispatcher.checkConflictLocked(scopes); err != nil {
		dispatcher.Unlock()
		return
	}
	for _, scope := range scopes {
		dispatcher.actives[scope] = true
	}
	task.AddObserver(dispatcher)
	dispatcher.Unlock()
	dispatcher.Dispatch(task)
	return
}

func (dispatcher *asyncJobDispatcher) OnExecDone(v interface{}) {
	task := v.(tasks.MScopedTask)
	scopes := task.Scopes()
	dispatcher.Lock()
	for _, scope := range scopes {
		delete(dispatcher.actives, scope)
	}
	dispatcher.Unlock()
}
