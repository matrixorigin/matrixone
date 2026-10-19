// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestAuthorizationEpochOwnershipAndEnd(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{TenantID: 5, UserID: 7, DefaultRoleID: 9})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := &ExecCtx{}
	ctx, e := beginAuthorizationEpoch(parent, ses, exec)
	require.Same(t, e, authorizationEpochFor(ctx, ses))
	require.Same(t, &e.prepared, rewritePolicyRequestFor(ctx, ses))
	type marker struct{}
	valueParent := context.WithValue(parent, marker{}, "request value")
	valueCtx, valueEpoch := beginAuthorizationEpoch(valueParent, ses, &ExecCtx{})
	require.Equal(t, "request value", valueCtx.Value(marker{}))
	childCancelCtx, cancelChild := context.WithCancelCause(valueCtx)
	defer cancelChild(nil)
	valueEpoch.end()
	require.ErrorIs(t, context.Cause(valueCtx), context.Canceled)
	require.ErrorIs(t, context.Cause(childCancelCtx), context.Canceled)
	other := newTestSession(t, gomock.NewController(t))
	defer other.Close()
	require.Nil(t, authorizationEpochFor(ctx, other))
	childCtx, child := beginAuthorizationEpoch(ctx, ses, &ExecCtx{})
	child.end()
	require.Error(t, childCtx.Err())
	require.NoError(t, ctx.Err(), "child completion cannot end its parent")
	changed := defines.AttachAccountId(ctx, 6)
	_, err := e.snapshot(changed, nil)
	require.ErrorContains(t, err, "context account changed")
	ses.GetTenantInfo().SetDefaultRoleID(10)
	_, err = e.snapshot(ctx, nil)
	require.ErrorContains(t, err, "principal changed")
	e.end()
	require.Error(t, ctx.Err())
	require.True(t, e.prepared.ended)
	require.NoError(t, parent.Err(), "completion cancels only the statement")
	_, err = statementSnapshotOption(ctx, ses, nil)
	require.Error(t, err)
}

func TestAuthorizationEpochEndCompletesOnce(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	parent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	ctx, e := beginAuthorizationEpoch(parent, ses, &ExecCtx{})
	entered, resume, firstDone, secondDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(release)
	priorCancel := e.cancel
	var cleanups atomic.Uint64
	e.cancel = func() {
		if cleanups.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-resume:
		case <-parent.Done():
		}
		priorCancel()
	}
	go func() { e.end(); close(firstDone) }()
	select {
	case <-entered:
	case <-parent.Done():
		t.Fatal("terminal cleanup did not start")
	}
	// A second boundary cannot observe ended and return ahead of the first
	// owner's cancellation. This check also detects an early unlocked seal.
	unlocked := e.mu.TryLock()
	if unlocked {
		e.mu.Unlock()
	}
	require.False(t, unlocked, "terminal completion remains owned until cleanup finishes")
	go func() { e.end(); close(secondDone) }()
	release()
	for _, done := range []chan struct{}{firstDone, secondDone} {
		select {
		case <-done:
		case <-parent.Done():
			t.Fatal("terminal caller did not complete")
		}
	}
	e.end()
	require.EqualValues(t, 1, cleanups.Load())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.True(t, e.prepared.ended)
	require.NoError(t, parent.Err())
}

type authorizationWaitContext struct {
	context.Context
	ready chan struct{}
	once  sync.Once
}

type authorizationDoneObserver struct {
	context.Context
	calls atomic.Uint64
}

func (c *authorizationDoneObserver) Done() <-chan struct{} {
	c.calls.Add(1)
	return c.Context.Done()
}

type authorizationUncomparableContext struct {
	context.Context
	data []byte
}

func TestAuthorizationEpochCallerCancellationBridge(t *testing.T) {
	parent, cancelParent := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancelParent)
	origin, cancel := context.WithCancel(parent)
	t.Cleanup(cancel)
	observer := &authorizationDoneObserver{Context: origin}
	e := &authorizationEpoch{ctx: observer, cancel: cancel}
	require.Nil(t, e.watchCallerCancellation(e))
	require.Zero(t, observer.calls.Load(), "the exact owner does not materialize Done for a comparison")
	// A context implementation need not be comparable. The concrete owner
	// comparison is safe, and shared cancellation still needs no bridge.
	require.Nil(t, e.watchCallerCancellation(authorizationUncomparableContext{Context: e, data: []byte{1}}))
	caller, cancelCaller := context.WithCancel(parent)
	t.Cleanup(cancelCaller)
	stop := e.watchCallerCancellation(caller)
	require.NotNil(t, stop)
	t.Cleanup(func() { stop() })
	cancelCaller()
	select {
	case <-origin.Done():
		require.ErrorIs(t, origin.Err(), context.Canceled)
	case <-parent.Done():
		t.Fatal("independent caller did not cancel the acquisition owner")
	}
	require.NoError(t, parent.Err())
}

func (c *authorizationWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.ready) })
	return c.Context.Done()
}

func TestAuthorizationEpochWaitersShareAcquisitionAndCancel(t *testing.T) {
	for _, terminal := range []string{"publish", "end", "caller cancellation"} {
		t.Run(terminal, func(t *testing.T) {
			ses := newTestSession(t, gomock.NewController(t))
			defer ses.Close()
			ctx, e := beginAuthorizationEpoch(t.Context(), ses, &ExecCtx{})
			defer e.end()
			// Hold the first acquisition at its I/O boundary without starting a
			// service. The waiters exercise the same publication and cancel edges.
			e.acquiring = true
			var results [2]chan error
			var cancels [2]context.CancelFunc
			for i := range results {
				waitCtx, cancel := context.WithCancel(ctx)
				cancels[i] = cancel
				defer cancel()
				observed := &authorizationWaitContext{Context: waitCtx, ready: make(chan struct{})}
				results[i] = make(chan error, 1)
				go func() { _, err := e.snapshot(observed, nil); results[i] <- err }()
				<-observed.ready
			}
			e.mu.Lock()
			pending := e.pending
			e.mu.Unlock()
			require.NotNil(t, pending)
			if terminal == "end" {
				e.end()
				for _, result := range results {
					require.ErrorIs(t, <-result, context.Canceled)
				}
				return
			}
			if terminal == "caller cancellation" {
				cancels[0]()
				require.ErrorIs(t, <-results[0], context.Canceled)
				results[0] = nil
			}
			failure := errors.New("catalog proof failed")
			e.mu.Lock()
			shared := pending == e.pending
			e.started, e.acquiring, e.err = true, false, failure
			close(e.pending)
			e.pending = nil
			e.mu.Unlock()
			require.True(t, shared)
			for _, result := range results {
				if result != nil {
					require.ErrorIs(t, <-result, failure)
				}
			}
		})
	}
}

func TestAuthorizationBatchParsingKeepsEagerErrorsAndRawContinuations(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	ctx := context.Background()
	for _, sql := range []string{"select id from app.t order by id;", "select ';' as x", "select 1 /* ; */;", "/*! select 1; */"} {
		first, remaining, staged, err := prepareAuthorizationStagedExecution(ctx, ses, ses.GetMySQLParser(), sql)
		require.NoError(t, err)
		require.False(t, staged)
		require.Empty(t, first)
		require.Empty(t, remaining.SQL)
	}
	_, _, _, err := prepareAuthorizationStagedExecution(ctx, ses, ses.GetMySQLParser(), "select 1; select '")
	require.Error(t, err, "bad ordinary tail must be rejected before executing statement one")
	first, rest, staged, err := prepareAuthorizationStagedExecution(ctx, ses, ses.GetMySQLParser(), "select ';'; /*! select 2; select 3 */")
	require.NoError(t, err)
	require.True(t, staged)
	require.Contains(t, first, "';'")
	next, rest, err := nextSQLModeStatementInput(ctx, ses, ses.GetMySQLParser(), &UserInput{}, rest)
	require.NoError(t, err)
	require.Contains(t, next.sql, "select 2")
	next, _, err = nextSQLModeStatementInput(ctx, ses, ses.GetMySQLParser(), &UserInput{}, rest)
	require.NoError(t, err)
	require.Contains(t, next.sql, "select 3")
}
