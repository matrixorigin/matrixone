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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
)

const DDLCommitGateRuntimeKey = "frontend.ddl-commit-gate"

// DDLCommitGate gives DDL producers and live protocol activation one local
// linearization point. Enter admits a commit while Block prevents new commits
// and waits for already-admitted commits to leave. A failed activation may keep
// the gate blocked across RPC attempts; Close wakes blocked sessions during CN
// shutdown.
type DDLCommitGate struct {
	mu      sync.Mutex
	changed chan struct{}
	blocked bool
	closed  bool
	active  int
	// enterBlockedHook is a deterministic test hook invoked after Enter observes
	// a blocked gate and before it waits. Production leaves it nil.
	enterBlockedHook func()
}

func NewDDLCommitGate() *DDLCommitGate {
	return &DDLCommitGate{changed: make(chan struct{})}
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

func (g *DDLCommitGate) Block(ctx context.Context) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return moerr.NewServiceUnavailableNoCtx("DDL commit gate is closed")
	}
	if !g.blocked {
		g.blocked = true
		g.signalLocked()
	}
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
