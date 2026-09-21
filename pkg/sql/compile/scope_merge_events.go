// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type mergeRunEventKind uint8

const (
	mergeRunPreScopeDone mergeRunEventKind = iota
	mergeRunParentDone
)

type mergeRunEvent struct {
	kind   mergeRunEventKind
	index  int
	result scopeRunResult
}

// mergeRunEventState is a continuation state machine for one MergeRun. It
// never waits on a completion channel itself. A child, parent, or remote
// notifier publishes one short ready task, and that task advances this state
// machine. Blocking is confined to external reader/transport event sources;
// VM work always runs as a resumable pipeline continuation.
type mergeRunEventState struct {
	s    *Scope
	c    *Compile
	done func(error)

	scheduler  *scopeTaskScheduler
	rootFinish func()
	finishOnce sync.Once

	mu              sync.Mutex
	outstanding     int
	parentDone      bool
	parentStarted   bool
	firstErr        error
	terminal        bool
	analyzerStarted bool

	claimed        []bool
	completions    []*lazyBranchCompletion
	sequential     bool
	nextSequential int
}

func newMergeRunEventState(s *Scope, c *Compile, done func(error)) *mergeRunEventState {
	preCount := 0
	sequential := false
	if s != nil {
		preCount = len(s.PreScopes)
		sequential = c != nil && c.IsTpQuery() && !c.hasMergeOp && !s.ConcurrentPreScopes && !s.LazyPreScopes
	}
	state := &mergeRunEventState{
		s:              s,
		c:              c,
		done:           done,
		claimed:        make([]bool, preCount),
		sequential:     sequential,
		nextSequential: preCount - 1,
	}
	if s != nil && s.LazyPreScopes {
		state.completions = make([]*lazyBranchCompletion, preCount)
		for i := range state.completions {
			state.completions[i] = newLazyBranchCompletion()
		}
	}
	return state
}

// MergeRun drives the same event-driven state machine used by production
// execution and waits only at the API boundary for callers that need an error
// result. It does not contain a second execution implementation.
func (s *Scope) MergeRun(c *Compile) error {
	if s == nil {
		return nil
	}
	if c == nil {
		return moerr.NewInternalErrorNoCtx("nil compile for MergeRun")
	}
	future, ownsScheduler, err := newScopeExecutionFuture(c, "merge-run", func(done func(error)) error {
		return s.mergeRunAsync(c, done)
	})
	if err != nil {
		return err
	}
	_, err = future.Await(context.Background())
	if ownsScheduler {
		c.waitScopeTaskScheduler()
	}
	return err
}

// mergeRunAsync starts MergeRun and returns after its continuation has been
// admitted. done is called exactly once after all accepted child, parent, and
// remote-notify events have reached a terminal state.
func (s *Scope) mergeRunAsync(c *Compile, done func(error)) error {
	if s == nil {
		if done != nil {
			done(nil)
		}
		return nil
	}
	if c == nil {
		err := moerr.NewInternalErrorNoCtx("nil compile for MergeRun")
		if done != nil {
			done(err)
		}
		return err
	}
	if done == nil {
		return moerr.NewInternalErrorNoCtx("nil MergeRun completion")
	}

	state := newMergeRunEventState(s, c, done)
	state.scheduler = c.ensureScopeTaskScheduler(1)
	return state.scheduler.submitRootAsync("merge-events", state.start)
}

func (m *mergeRunEventState) context() context.Context {
	if m.s != nil && m.s.Proc != nil && m.s.Proc.Ctx != nil {
		return m.s.Proc.Ctx
	}
	return context.Background()
}

func (m *mergeRunEventState) addTask() {
	m.mu.Lock()
	m.outstanding++
	m.mu.Unlock()
}

func (m *mergeRunEventState) finishEvent() {
	m.mu.Lock()
	if m.outstanding > 0 {
		m.outstanding--
	}
	m.mu.Unlock()
}

func (m *mergeRunEventState) recordError(err error) {
	if err == nil {
		return
	}
	m.mu.Lock()
	if m.firstErr == nil {
		m.firstErr = err
	}
	m.mu.Unlock()
	logutil.Debugf("[scope-scheduler] merge event error=%v", err)
	m.s.cancelMergeSiblingsOnError(err)
}

func (m *mergeRunEventState) start(rootFinish func()) {
	m.mu.Lock()
	m.rootFinish = rootFinish
	m.mu.Unlock()

	defer func() {
		if recovered := recover(); recovered != nil {
			err := moerr.ConvertPanicError(m.context(), recovered)
			m.fail(err)
		}
	}()

	if m.s.ScopeAnalyzer == nil {
		m.s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	m.s.ScopeAnalyzer.Start()
	m.mu.Lock()
	m.analyzerStarted = true
	m.mu.Unlock()
	logutil.Debugf("[scope-scheduler] merge event start scheduler=%d pre=%d remote=%d",
		m.scheduler.id, len(m.s.PreScopes), len(m.s.RemoteReceivRegInfos))

	if m.s.LazyPreScopes && (len(m.s.PreScopes) < 2 || len(m.s.RemoteReceivRegInfos) != 0) {
		err := moerr.NewInternalErrorNoCtx("invalid lazy union all scope topology")
		cleanLazyScopeStartFailure(m.s, m.c, err)
		m.fail(err)
		return
	}

	if len(m.s.RemoteReceivRegInfos) != 0 {
		m.addTaskForRemoteNotifications()
	}
	if m.s.LazyPreScopes {
		m.startParent()
	} else if m.sequential {
		if m.nextSequential >= 0 {
			if err := m.activatePreScope(m.nextSequential); err != nil {
				m.fail(err)
			}
		} else {
			m.startParent()
		}
	} else {
		for i := range m.s.PreScopes {
			if err := m.activatePreScope(i); err != nil {
				m.fail(err)
				return
			}
		}
		m.startParent()
	}
	m.maybeFinish()
}

// enqueueEvent admits a short ready task. A child or parent publishes its
// terminal event and returns; the ready worker performs only state reduction.
func (m *mergeRunEventState) enqueueEvent(event mergeRunEvent) {
	if err := m.scheduler.submitRoot("merge-event", func() {
		m.handleEvent(event)
	}); err != nil {
		m.fail(err)
	}
}

func (m *mergeRunEventState) enqueueNotifyEvent(result notifyMessageResult) {
	if err := m.scheduler.submitRoot("merge-notify-event", func() {
		m.handleNotifyEvent(result)
	}); err != nil {
		// No ready task will own the sender after admission failed.
		result.clean(m.s.Proc)
		m.fail(err)
	}
}

func (m *mergeRunEventState) fail(err error) {
	if err == nil {
		return
	}
	m.recordError(err)
	m.finish(err)
}

func (m *mergeRunEventState) finish(err error) {
	m.finishOnce.Do(func() {
		m.mu.Lock()
		if err == nil {
			err = m.firstErr
		}
		m.terminal = true
		rootFinish := m.rootFinish
		analyzerStarted := m.analyzerStarted
		m.mu.Unlock()

		if analyzerStarted {
			m.s.ScopeAnalyzer.Stop()
		}
		if rootFinish != nil {
			rootFinish()
		}
		if m.done != nil {
			m.done(err)
		}
		logutil.Debugf("[scope-scheduler] merge event complete scheduler=%d err=%v", m.scheduler.id, err)
	})
}

func (m *mergeRunEventState) maybeFinish() {
	m.mu.Lock()
	complete := m.parentDone && m.outstanding == 0 && !m.terminal
	err := m.firstErr
	m.mu.Unlock()
	if complete {
		m.finish(err)
	}
}

func (m *mergeRunEventState) startParent() {
	m.mu.Lock()
	if m.parentStarted || m.terminal {
		m.mu.Unlock()
		return
	}
	m.parentStarted = true
	m.mu.Unlock()

	// Parent execution is admitted directly as a continuation. It never owns a
	// worker while waiting for a child edge or a remote response.
	m.addTask()
	if err := m.runParentTaskAsync(func(runErr error) {
		m.enqueueEvent(mergeRunEvent{
			kind:   mergeRunParentDone,
			result: newScopeRunResult(runErr, m.s),
		})
	}); err != nil {
		m.enqueueEvent(mergeRunEvent{
			kind:   mergeRunParentDone,
			result: newScopeRunResult(err, m.s),
		})
	}
}

func (m *mergeRunEventState) runParentTaskAsync(done func(error)) error {
	if !m.s.LazyPreScopes {
		return m.s.parallelRunAsync(m.c, done)
	}

	clearStarter, deferFirst, err := installSequentialBranchStarter(
		m.s.RootOp,
		m.activatePreScope,
		m.registerPreScopeWait,
	)
	if err != nil {
		return err
	}
	finish := func(runErr error) {
		clearStarter()
		cause := context.Cause(m.context())
		if cause == nil {
			cause = context.Canceled
		}
		for i := range m.s.PreScopes {
			if m.isClaimed(i) {
				continue
			}
			m.claimUnstartedBranch(i, cause)
		}
		done(runErr)
	}
	if !deferFirst {
		if err := m.activatePreScope(0); err != nil {
			finish(err)
			return nil
		}
	}
	if err := m.s.parallelRunAsync(m.c, finish); err != nil {
		finish(err)
		return nil
	}
	return nil
}

func (m *mergeRunEventState) registerPreScopeWait(i int, ready func(error)) error {
	if i < 0 || i >= len(m.completions) || !m.isClaimed(i) {
		return moerr.NewInternalErrorNoCtx("invalid lazy branch completion wait")
	}
	return m.completions[i].register(ready)
}

func (m *mergeRunEventState) isClaimed(i int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return i >= 0 && i < len(m.claimed) && m.claimed[i]
}

func (m *mergeRunEventState) claimUnstartedBranch(i int, cause error) {
	m.mu.Lock()
	if i < 0 || i >= len(m.claimed) || m.claimed[i] {
		m.mu.Unlock()
		return
	}
	m.claimed[i] = true
	m.mu.Unlock()
	cleanScopeTreeWithStartFail(m.s.PreScopes[i], cause, m.c.isPrepare)
	m.completions[i].finish(newScopeRunResult(cause, m.s.PreScopes[i]))
}

func (m *mergeRunEventState) addTaskForRemoteNotifications() {
	for range m.s.RemoteReceivRegInfos {
		m.addTask()
	}
	m.s.sendNotifyMessageWithFactoryAndCallback(
		nil,
		m.enqueueNotifyEvent,
		newMessageSenderOnClient,
		nil,
		m.scheduler,
	)
}

func (m *mergeRunEventState) activatePreScope(i int) error {
	if i < 0 || i >= len(m.s.PreScopes) {
		return moerr.NewInternalErrorNoCtx("invalid event-driven MergeRun branch activation")
	}
	m.mu.Lock()
	if m.claimed[i] {
		m.mu.Unlock()
		return moerr.NewInternalErrorNoCtx("duplicate event-driven MergeRun branch activation")
	}
	m.claimed[i] = true
	m.mu.Unlock()

	scope := m.s.PreScopes[i]
	logutil.Debugf("[scope-scheduler] merge branch admitted index=%d magic=%d", i, scope.Magic)
	m.addTask()
	if cause := context.Cause(m.context()); cause != nil {
		cleanScopeTreeWithStartFail(scope, cause, m.c.isPrepare)
		m.enqueueEvent(mergeRunEvent{
			kind:   mergeRunPreScopeDone,
			index:  i,
			result: newScopeRunResult(cause, scope),
		})
		return nil
	}

	err := m.runPreScopeAsync(scope, func(runErr error) {
		m.enqueueEvent(mergeRunEvent{
			kind:   mergeRunPreScopeDone,
			index:  i,
			result: newScopeRunResult(runErr, scope),
		})
	})
	if err != nil {
		cleanScopeTreeWithStartFail(scope, err, m.c.isPrepare)
		m.enqueueEvent(mergeRunEvent{
			kind:   mergeRunPreScopeDone,
			index:  i,
			result: newScopeRunResult(err, scope),
		})
	}
	return nil
}

func (m *mergeRunEventState) runPreScopeAsync(scope *Scope, done func(error)) (err error) {
	if m.s.LazyPreScopes {
		assignLazyRemoteGeneration(scope, m.c.addr)
		if err = m.s.initLazyPreScope(scope, m.c); err != nil {
			cleanScopeTreeWithStartFail(scope, err, m.c.isPrepare)
			return err
		}
	}
	if m.sequential {
		return scope.mergeRunAsync(m.c, done)
	}

	switch scope.Magic {
	case Normal:
		return scope.runEventAsync(m.c, done)
	case Merge, MergeInsert, MergeDelete:
		return scope.mergeRunAsync(m.c, done)
	case Remote:
		return scope.remoteRunAsync(m.c, done)
	default:
		// Control/DDL scopes have no VM root.  Route their blocking catalog
		// operation through the scheduler's event-source lane, just like a
		// top-level control scope admitted by Compile.runAsync.
		return m.c.runControlScopeAsync(scope, done)
	}
}

func (m *mergeRunEventState) handleEvent(event mergeRunEvent) {
	resolved, _ := event.result.resolveCancelCause()
	if resolved.err != nil {
		logutil.Debugf("[scope-scheduler] merge event kind=%d index=%d err=%v", event.kind, event.index, resolved.err)
	}
	switch event.kind {
	case mergeRunPreScopeDone:
		if m.completions != nil && event.index >= 0 && event.index < len(m.completions) {
			m.completions[event.index].finish(event.result)
		}
		if !m.isTerminal() {
			m.recordError(resolved.err)
			m.finishEvent()
			if m.sequential {
				if resolved.err != nil {
					m.markParentDone()
				} else {
					m.nextSequential--
					if m.nextSequential >= 0 {
						if err := m.activatePreScope(m.nextSequential); err != nil {
							m.fail(err)
						}
					} else {
						m.startParent()
					}
				}
			}
		}
	case mergeRunParentDone:
		if !m.isTerminal() {
			m.recordError(resolved.err)
			m.mu.Lock()
			m.parentDone = true
			m.mu.Unlock()
			m.finishEvent()
		}
	default:
		m.fail(moerr.NewInternalErrorNoCtx("unknown MergeRun event"))
	}
	m.maybeFinish()
}

func (m *mergeRunEventState) handleNotifyEvent(result notifyMessageResult) {
	result.clean(m.s.Proc)
	if !m.isTerminal() {
		m.recordError(result.err)
		m.finishEvent()
	}
	m.maybeFinish()
}

func (m *mergeRunEventState) isTerminal() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.terminal
}

func (m *mergeRunEventState) markParentDone() {
	m.mu.Lock()
	m.parentDone = true
	m.mu.Unlock()
}
