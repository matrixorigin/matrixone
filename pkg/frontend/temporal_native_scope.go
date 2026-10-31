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

package frontend

import (
	"context"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"math"
	"reflect"
	"sync"
	"sync/atomic"

	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type temporalCompilerScopes struct {
	mu        sync.Mutex
	contended atomic.Bool
	observed  atomic.Pointer[temporalScopeFrame]
	frame     *temporalScopeFrame
	revision  uint64
	depth     int
}
type temporalScopeBinding struct {
	execution *ExecCtx
	op        client.TxnOperator
}
type temporalScopeFrame struct {
	parent           *temporalScopeFrame // immutable, including after a normal pop
	owner            *TxnCompilerContext
	binding          atomic.Pointer[temporalScopeBinding]
	scope            *plan.TemporalPlanningScope
	active           atomic.Int64
	retired          atomic.Bool
	valid, completed bool // protected by the compiler gate
}

func (frame *temporalScopeFrame) ReleaseTemporalScope() { frame.binding.Store(nil) }

// Invalidating an immutable chain needs no lock and cannot retain an old
// transaction after a failed cleanup Try. Each executing owner still releases
// its own scope; the compiler can sweep the retired shells on its next entry.
func invalidateTemporalChain(frame *temporalScopeFrame) {
	for ; frame != nil; frame = frame.parent {
		frame.scope.Invalidate()
	}
}
func (tcc *TxnCompilerContext) invalidateTemporalFramesLocked(incoming *plan.TemporalPlanningScope) {
	incoming.Invalidate()
	for frame := tcc.temporal.frame; frame != nil; frame = frame.parent {
		frame.valid = false
		frame.scope.Invalidate()
	}
}
func (state *temporalCompilerScopes) sweepRetiredLocked() {
	for state.frame != nil && state.frame.retired.Load() && state.frame.active.Load() == 0 {
		state.frame = state.frame.parent
		state.depth--
	}
	state.observed.Store(state.frame)
	if state.frame == nil {
		state.depth = 0
		state.contended.Store(false)
	}
}

// Push shadows even nil/unclaimed nested builders. A rejected concurrent
// attempt invalidates the observed chain but does not strand a future statement.
func (tcc *TxnCompilerContext) PushTemporalPlanningScope(scope *plan.TemporalPlanningScope) func() {
	var current client.TxnOperator
	if tcc.execCtx != nil && tcc.execCtx.ses != nil {
		current = tcc.execCtx.ses.GetTxnHandler().GetTxn()
	}
	state := &tcc.temporal
	if !state.mu.TryLock() {
		state.contended.Store(true)
		invalidateTemporalChain(state.observed.Load())
		scope.Invalidate()
		return func() {}
	}
	state.sweepRetiredLocked()
	if state.contended.Load() {
		tcc.invalidateTemporalFramesLocked(scope)
		state.mu.Unlock()
		return func() {}
	}
	if state.revision == math.MaxUint64 || state.depth >= 64 || (state.frame != nil && state.frame.active.Load() != 0) {
		tcc.invalidateTemporalFramesLocked(scope)
	}
	if state.depth >= 64 || state.revision == math.MaxUint64 {
		state.mu.Unlock()
		return func() {}
	}
	state.revision++
	frame := &temporalScopeFrame{parent: state.frame, owner: tcc, scope: scope, valid: scope == nil || scope.Usable()}
	frame.binding.Store(&temporalScopeBinding{execution: tcc.execCtx, op: current})
	if scope != nil && !scope.SetNativeOwner(frame) {
		frame.valid = false
		scope.Invalidate()
	}
	state.frame = frame
	state.observed.Store(frame)
	state.depth++
	state.mu.Unlock()
	return func() {
		if !frame.retired.CompareAndSwap(false, true) {
			return
		}
		if !state.mu.TryLock() {
			state.contended.Store(true)
			invalidateTemporalChain(frame)
			frame.ReleaseTemporalScope()
			return
		}
		defer state.mu.Unlock()
		if state.frame != frame || frame.active.Load() != 0 || state.revision == math.MaxUint64 || state.contended.Load() {
			state.contended.Store(true)
			tcc.invalidateTemporalFramesLocked(nil)
			frame.ReleaseTemporalScope()
			state.sweepRetiredLocked()
			return
		}
		state.revision++
		frame.completed = frame.valid && frame.scope.Usable()
		if !frame.completed {
			frame.ReleaseTemporalScope()
		}
		state.frame = frame.parent
		state.depth--
		state.observed.Store(state.frame)
	}
}

type temporalNativeCapture struct {
	tcc        *TxnCompilerContext
	frame      *temporalScopeFrame
	binding    *temporalScopeBinding
	revision   uint64
	op         client.TxnOperator
	stamp      client.CatalogReadStamp
	visibility uint64
	captureOK  bool
	finished   atomic.Bool
	relation   engine.Relation
	ref        *plan.ObjectRef
	full       *plan.TableDef
}

func readTemporalNativeStamp(op client.TxnOperator) (client.CatalogReadStamp, uint64, bool) {
	reader, ok := op.(interface {
		TryCatalogReadStamp() (client.CatalogReadStamp, bool)
	})
	if !ok || op.IsSnapOp() {
		return client.CatalogReadStamp{}, 0, false
	}
	stamp, ok := reader.TryCatalogReadStamp()
	if !ok {
		return client.CatalogReadStamp{}, 0, false
	}
	workspace, ok := stamp.Workspace.(interface{ CatalogVisibility() (uint64, bool) })
	if !ok || reflect.TypeOf(stamp.Workspace).Kind() != reflect.Pointer {
		return client.CatalogReadStamp{}, 0, false
	}
	visibility, stable := workspace.CatalogVisibility()
	return stamp, visibility, stable
}

func (tcc *TxnCompilerContext) beginTemporalNativeResolve(excluded bool) *temporalNativeCapture {
	if !tcc.temporal.mu.TryLock() {
		tcc.temporal.contended.Store(true)
		return nil
	}
	state := &tcc.temporal
	frame := state.frame
	if frame == nil {
		state.mu.Unlock()
		return nil
	}
	if frame.active.Load() != 0 || state.contended.Load() {
		tcc.invalidateTemporalFramesLocked(nil)
	}
	frame.active.Add(1)
	binding := frame.binding.Load()
	capture := &temporalNativeCapture{tcc: tcc, frame: frame, binding: binding, revision: state.revision}
	if binding != nil {
		capture.op = binding.op
	}
	valid := !excluded && tcc.execCtx != nil && tcc.execCtx.ses != nil && frame.valid && frame.scope.Usable() && binding != nil && binding.execution == tcc.execCtx
	state.mu.Unlock()
	if valid && tcc.execCtx.ses.GetTxnHandler().GetTxn() == capture.op {
		capture.stamp, capture.visibility, capture.captureOK = readTemporalNativeStamp(capture.op)
	}
	return capture
}

func (capture *temporalNativeCapture) capture(ctx context.Context, relation engine.Relation, ref *plan.ObjectRef, full *plan.TableDef) {
	if capture == nil || !capture.captureOK || capture.tcc.execCtx == nil || capture.tcc.execCtx.ses.GetTxnHandler().GetTxn() != capture.op || ref == nil || full == nil || ref.PubInfo != nil || ref.SubscriptionName != "" || full.IsTemporary {
		return
	}
	if _, ok := relation.(engine.TemporalIndexRelation); !ok {
		return
	}
	stamp, visibility, stable := readTemporalNativeStamp(capture.op)
	if !stable || visibility != capture.visibility || stamp.TransactionID != capture.stamp.TransactionID || !stamp.Snapshot.Equal(capture.stamp.Snapshot) || stamp.Revision != capture.stamp.Revision || stamp.Workspace != capture.stamp.Workspace || context.Cause(ctx) != nil {
		return
	}
	capture.relation, capture.ref, capture.full = relation, ref, full
}

func (capture *temporalNativeCapture) finish() {
	if capture == nil || !capture.finished.CompareAndSwap(false, true) {
		return
	}
	tcc := capture.tcc
	capture.frame.active.Add(-1)
	if !tcc.temporal.mu.TryLock() {
		tcc.temporal.contended.Store(true)
		invalidateTemporalChain(capture.frame)
		return
	}
	defer tcc.temporal.mu.Unlock()
	frame := capture.frame
	if frame.active.Load() != 0 || tcc.temporal.frame != frame || tcc.temporal.revision != capture.revision || tcc.temporal.contended.Load() || !frame.valid || frame.binding.Load() != capture.binding || capture.binding == nil || capture.binding.execution != tcc.execCtx {
		tcc.invalidateTemporalFramesLocked(nil)
		frame.scope.Invalidate()
		tcc.temporal.sweepRetiredLocked()
		return
	}
	if capture.relation != nil {
		frame.scope.AddNativeRelation(capture.ref, capture.full, capture.relation, engine.TemporalRead{Op: capture.op, Stamp: capture.stamp, Visibility: capture.visibility})
	}
}

func temporalNativeSnapshotOverride(snapshot *pbplan.Snapshot) bool {
	return snapshot != nil && (plan.IsSnapshotValid(snapshot) || snapshot.Tenant != nil || snapshot.ExtraInfo != nil)
}
