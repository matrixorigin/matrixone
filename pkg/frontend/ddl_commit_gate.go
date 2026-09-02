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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const DDLCommitGateRuntimeKey = "frontend.ddl-commit-gate"

// ErrDDLFrontierPublishedByRevokedGeneration means T is durable, but the
// producer lost UUID ownership. The committed DDL must still fan out before
// this error is returned to the client.
var ErrDDLFrontierPublishedByRevokedGeneration = moerr.NewInvalidStateNoCtx(
	"DDL frontier published by revoked CN generation")

// DDLRevocationCompletion delays physical CN drain until an already-committed
// stale-generation DDL has completed its mandatory visibility fan-out.
type DDLRevocationCompletion interface {
	CompleteDDLRevocation()
}

// DDLCommitGate gives DDL producers and live protocol activation one local
// linearization point. Enter admits a commit while Block prevents new commits
// and waits for already-admitted commits to leave. A failed activation may keep
// the gate blocked across RPC attempts; Close wakes blocked sessions during CN
// shutdown.
type DDLCommitGate struct {
	mu                sync.Mutex
	changed           chan struct{}
	blocked           bool
	closed            bool
	publicDDL         bool
	active            int
	ddlFrontier       atomic.Pointer[timestamp.Timestamp]
	frontierPublisher func(context.Context, timestamp.Timestamp) error
	// enterBlockedHook is a deterministic test hook invoked after Enter observes
	// a blocked gate and before it waits. Production leaves it nil.
	enterBlockedHook func()
}

func NewDDLCommitGate() *DDLCommitGate {
	return &DDLCommitGate{changed: make(chan struct{})}
}

func (g *DDLCommitGate) RecordDDLFrontier(ts timestamp.Timestamp) {
	if ts.IsEmpty() {
		return
	}
	candidate := ts
	for {
		current := g.ddlFrontier.Load()
		if current != nil && current.GreaterEq(ts) {
			return
		}
		if g.ddlFrontier.CompareAndSwap(current, &candidate) {
			return
		}
	}
}

// SetFrontierPublisher installs the CN-owned durable publisher before public
// sessions start. Production sets it exactly once during service construction.
func (g *DDLCommitGate) SetFrontierPublisher(publisher func(context.Context, timestamp.Timestamp) error) {
	g.frontierPublisher = publisher
}

func (g *DDLCommitGate) PublishDDLFrontier(ctx context.Context, ts timestamp.Timestamp) error {
	g.RecordDDLFrontier(ts)
	if g.frontierPublisher == nil || ts.IsEmpty() {
		return nil
	}
	return g.frontierPublisher(ctx, ts)
}

func (g *DDLCommitGate) LatestDDLFrontier() timestamp.Timestamp {
	if current := g.ddlFrontier.Load(); current != nil {
		return *current
	}
	return timestamp.Timestamp{}
}

func publicBackgroundDDLBarrierEnabled(serviceID string) bool {
	value, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(DDLCommitGateRuntimeKey)
	if !ok || value == nil {
		return false
	}
	gate, ok := value.(*DDLCommitGate)
	return ok && gate.PublicDDLEnabled()
}

func publishDDLCommitFrontier(ctx context.Context, serviceID string, ts timestamp.Timestamp) error {
	value, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(DDLCommitGateRuntimeKey)
	if !ok || value == nil {
		return nil
	}
	if gate, ok := value.(*DDLCommitGate); ok {
		return gate.PublishDDLFrontier(ctx, ts)
	}
	return moerr.NewInternalError(ctx, "invalid DDL commit gate")
}

func recordDDLCommitFrontier(serviceID string, ts timestamp.Timestamp) {
	value, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(DDLCommitGateRuntimeKey)
	if !ok || value == nil {
		return
	}
	if gate, ok := value.(*DDLCommitGate); ok {
		gate.RecordDDLFrontier(ts)
	}
}

func enterDDLCommitGate(ctx context.Context, serviceID string) (func(), error) {
	value, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(DDLCommitGateRuntimeKey)
	if !ok || value == nil {
		return func() {}, nil
	}
	gate, ok := value.(*DDLCommitGate)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "invalid DDL commit gate")
	}
	return gate.Enter(ctx)
}

// EnablePublicDDL marks that public listeners are live. Background sessions
// created on behalf of a client must participate in the cross-CN barrier after
// this point; bootstrap background DDL remains local before it.
func (g *DDLCommitGate) EnablePublicDDL() {
	g.mu.Lock()
	g.publicDDL = true
	g.mu.Unlock()
}

func (g *DDLCommitGate) PublicDDLEnabled() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	// Close rejects every later Enter, but a DDL transaction admitted before
	// shutdown retains the eligibility it had when public listeners were live.
	return g.publicDDL
}

func (g *DDLCommitGate) Enter(ctx context.Context) (func(), error) {
	for {
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return nil, moerr.NewServiceUnavailableNoCtx("DDL commit gate is closed")
		}
		if !g.blocked {
			g.active++
			g.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					g.mu.Lock()
					g.active--
					g.signalLocked()
					g.mu.Unlock()
				})
			}, nil
		}
		changed := g.changed
		hook := g.enterBlockedHook
		g.mu.Unlock()
		if hook != nil {
			hook()
		}

		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-changed:
		}
	}
}

func (g *DDLCommitGate) BlockNew() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return moerr.NewServiceUnavailableNoCtx("DDL commit gate is closed")
	}
	if !g.blocked {
		g.blocked = true
		g.signalLocked()
	}
	return nil
}

func (g *DDLCommitGate) WaitDrained(ctx context.Context) error {
	g.mu.Lock()
	for g.active > 0 {
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return moerr.NewServiceUnavailableNoCtx("DDL commit gate is closed")
		}
	}
	g.mu.Unlock()
	return nil
}

func (g *DDLCommitGate) Block(ctx context.Context) error {
	if err := g.BlockNew(); err != nil {
		return err
	}
	return g.WaitDrained(ctx)
}

func (g *DDLCommitGate) Unblock() {
	g.mu.Lock()
	if !g.closed && g.blocked {
		g.blocked = false
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (g *DDLCommitGate) Close() {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		g.blocked = true
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (g *DDLCommitGate) signalLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}
