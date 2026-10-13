// Copyright 2026 Matrix Origin
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

package cnservice

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
)

const siriusBenchmarkLeaseCapacity = 128

// siriusBenchmarkProtector deliberately has no storage side effects. It is
// valid only when the TN GC manager is disabled and the launcher has verified
// that fact. The session bookkeeping still enforces the Protector lifecycle
// contract so admission cannot publish a lease after a failed registration or
// use a scope after it has been closed.
type siriusBenchmarkProtector struct {
}

type siriusBenchmarkProtection struct {
	mu     sync.Mutex
	refs   map[string]struct{}
	closed bool
}

func (p *siriusBenchmarkProtector) Begin(ctx context.Context) (
	func(context.Context, []byte, []string, time.Time) error,
	func(context.Context, []byte) error,
	func(),
	error,
) {
	if err := siriusBenchmarkContextError(ctx); err != nil {
		return nil, nil, nil, err
	}
	if p == nil {
		return nil, nil, nil, moerr.NewInternalErrorNoCtx("substrait: benchmark read protector is nil")
	}
	session := &siriusBenchmarkProtection{
		refs: make(map[string]struct{}),
	}

	register := func(ctx context.Context, readRef []byte, _ []string, expires time.Time) error {
		if err := siriusBenchmarkContextError(ctx); err != nil {
			return err
		}
		if len(readRef) == 0 {
			return moerr.NewInternalErrorNoCtx("substrait: benchmark read protector has empty reference")
		}
		if expires.IsZero() {
			return moerr.NewInternalErrorNoCtx("substrait: benchmark read protector has empty expiry")
		}
		key := string(readRef)
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.closed {
			return moerr.NewInternalErrorNoCtx("substrait: benchmark read protector session is closed")
		}
		if _, ok := session.refs[key]; ok {
			return moerr.NewInternalErrorNoCtx("substrait: duplicate benchmark read protection")
		}
		session.refs[key] = struct{}{}
		return nil
	}

	rollback := func(ctx context.Context, readRef []byte) error {
		if err := siriusBenchmarkContextError(ctx); err != nil {
			return err
		}
		if len(readRef) == 0 {
			return moerr.NewInternalErrorNoCtx("substrait: benchmark read protector has empty reference")
		}
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.closed {
			return moerr.NewInternalErrorNoCtx("substrait: benchmark read protector session is closed")
		}
		delete(session.refs, string(readRef))
		return nil
	}

	var closeOnce sync.Once
	closeProtection := func() {
		closeOnce.Do(func() {
			session.mu.Lock()
			session.closed = true
			session.refs = nil
			session.mu.Unlock()
		})
	}
	return register, rollback, closeProtection, nil
}

func (p *siriusBenchmarkProtector) Unregister(ctx context.Context, readRef []byte) error {
	if err := siriusBenchmarkContextError(ctx); err != nil {
		return err
	}
	if p == nil {
		return moerr.NewInternalErrorNoCtx("substrait: benchmark read protector is nil")
	}
	if len(readRef) == 0 {
		return moerr.NewInternalErrorNoCtx("substrait: benchmark read protector has empty reference")
	}
	// TN GC is disabled for this explicitly verified benchmark mode, so there
	// is no storage registration to revoke at terminal release.
	return nil
}

func siriusBenchmarkContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	return nil
}

// siriusBenchmarkAuditRecorder preserves the mandatory resolve-audit
// boundary without logging query text, manifests, credentials, or other
// unbounded request data on the benchmark hot path.
type siriusBenchmarkAuditRecorder struct {
	resolves atomic.Uint64
}

func (a *siriusBenchmarkAuditRecorder) RecordResolve(ctx context.Context, _ substrait.ResolveAuditEvent) error {
	if err := siriusBenchmarkContextError(ctx); err != nil {
		return err
	}
	if a == nil {
		return moerr.NewInternalErrorNoCtx("substrait: benchmark resolve audit recorder is nil")
	}
	a.resolves.Add(1)
	return nil
}

func newSiriusBenchmarkDependencies() (*substrait.LeaseManager, substrait.ResolveAuditRecorder) {
	protector := new(siriusBenchmarkProtector)
	return substrait.NewBenchmarkLeaseManager(siriusBenchmarkLeaseCapacity, protector), &siriusBenchmarkAuditRecorder{}
}
