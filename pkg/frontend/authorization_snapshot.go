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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnclient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

type authorizationEpochKey struct{}

// Preserve statement instrumentation values across an implicit commit while
// taking cancellation from its still-live request rather than the ended epoch.
type authorizationTransitionContext struct {
	context.Context
	lifetime context.Context
}

func (c authorizationTransitionContext) Deadline() (time.Time, bool) { return c.lifetime.Deadline() }
func (c authorizationTransitionContext) Done() <-chan struct{}       { return c.lifetime.Done() }
func (c authorizationTransitionContext) Err() error                  { return c.lifetime.Err() }

// An epoch owns authorization for one semantic statement, including its helper
// reads. It is independent of the user's (possibly older) data transaction.
type authorizationEpoch struct {
	owner    *Session
	exec     *ExecCtx
	identity rewriteRuleIdentity
	ctx      context.Context
	parent   context.Context
	cancel   context.CancelFunc
	prepared *rewritePolicyRequest
	mu       sync.Mutex
	entered  bool
	ended    bool
	started  bool
	pending  chan struct{}
	engine   *disttae.Engine
	admitted *txnclient.SnapshotAdmission
	err      error
	version  disttae.PrivilegeCacheVersion
	minimum  timestamp.Timestamp
	proofErr error
}

func beginAuthorizationEpoch(ctx context.Context, ses *Session, exec *ExecCtx) (context.Context, *authorizationEpoch) {
	origin, cancel := context.WithCancel(ctx)
	e := &authorizationEpoch{owner: ses, exec: exec, identity: rewriteRuleSessionIdentity(ses), ctx: origin, parent: ctx, cancel: cancel,
		prepared: &rewritePolicyRequest{owner: ses}}
	ctx = context.WithValue(origin, authorizationEpochKey{}, e)
	return context.WithValue(ctx, rewritePolicyRequestKey{}, e.prepared), e
}

func authorizationEpochFor(ctx context.Context, ses *Session) *authorizationEpoch {
	if ctx == nil {
		return nil
	}
	e, _ := ctx.Value(authorizationEpochKey{}).(*authorizationEpoch)
	if e != nil && e.owner == ses {
		return e
	}
	return nil
}

func (e *authorizationEpoch) end() {
	e.mu.Lock()
	e.ended = true
	a := e.admitted
	e.mu.Unlock()
	e.prepared.mu.Lock()
	e.prepared.valid, e.prepared.ended = false, true
	e.prepared.mu.Unlock()
	e.cancel()
	a.End()
}

func (e *authorizationEpoch) snapshot(ctx context.Context, eng *disttae.Engine) (*txnclient.SnapshotAdmission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if rewriteRuleSessionIdentity(e.owner) != e.identity {
		return nil, moerr.NewInternalError(ctx, "authorization principal changed during statement")
	}
	if account := ctx.Value(defines.TenantIDKey{}); account != nil && account != e.identity.account {
		return nil, moerr.NewInternalError(ctx, "authorization context account changed during statement")
	}
	for {
		e.mu.Lock()
		if e.ended || e.ctx.Err() != nil || rewriteRuleSessionIdentity(e.owner) != e.identity {
			e.mu.Unlock()
			if err := e.ctx.Err(); err != nil {
				return nil, err
			}
			return nil, moerr.NewInternalError(ctx, "authorization statement ended")
		}
		if e.pending != nil {
			pending := e.pending
			e.mu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-e.ctx.Done():
				return nil, e.ctx.Err()
			}
		}
		if e.started {
			a, priorEngine, err := e.admitted, e.engine, e.err
			e.mu.Unlock()
			if priorEngine != eng {
				return nil, moerr.NewInternalError(ctx, "authorization engine changed during statement")
			}
			return a, err
		}
		e.pending = make(chan struct{})
		pending := e.pending
		e.mu.Unlock()

		// No session/epoch lock covers limiter admission or logtail I/O.
		stop := func() bool { return true }
		if ctx.Done() != e.ctx.Done() {
			stop = context.AfterFunc(ctx, e.cancel)
		}
		minimum := e.owner.getLastCommitTS()
		a, _, err := eng.AcquireCatalogSnapshot(e.ctx, minimum)
		var version disttae.PrivilegeCacheVersion
		var proofErr error
		if err == nil && a != nil {
			// A failed catalog proof is not a failed snapshot admission. Keep
			// A for the established fresh-read fallback and grant-independent
			// controls; never replace it with a newer authorization snapshot.
			version, minimum, proofErr = eng.GetPrivilegeCacheVersionWithAdmission(e.ctx, e.identity.account, minimum, a)
		}
		stop()
		if callerErr := ctx.Err(); callerErr != nil {
			a.End()
			a = nil
			err = callerErr
		}
		e.mu.Lock()
		if e.ended || e.ctx.Err() != nil || rewriteRuleSessionIdentity(e.owner) != e.identity {
			a.End()
			a = nil
			err = e.ctx.Err()
			if err == nil {
				err = moerr.NewInternalError(ctx, "authorization statement ended")
			}
		}
		if err != nil {
			a.End()
			a = nil
			version = disttae.PrivilegeCacheVersion{}
		}
		e.admitted, e.err, e.engine, e.started = a, err, eng, true
		e.version, e.minimum, e.proofErr = version, minimum, proofErr
		e.pending = nil
		close(pending)
		e.mu.Unlock()
		return a, err
	}
}

func sessionCatalogEngine(ses *Session) *disttae.Engine {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil {
		return nil
	}
	storage := pu.StorageEngine
	if wrapped, ok := storage.(*engine.EntireEngine); ok {
		storage = wrapped.Engine
	}
	eng, _ := storage.(*disttae.Engine)
	return eng
}

func statementCatalogVersion(ctx context.Context, ses *Session, eng *disttae.Engine) (disttae.PrivilegeCacheVersion, timestamp.Timestamp, error) {
	minimum := ses.getLastCommitTS()
	if e := authorizationEpochFor(ctx, ses); e != nil {
		a, err := e.snapshot(ctx, eng)
		if err != nil {
			return disttae.PrivilegeCacheVersion{}, minimum, err
		}
		if a != nil {
			if _, err = eng.ValidateCatalogSnapshot(ctx, minimum, a); err != nil {
				return disttae.PrivilegeCacheVersion{}, minimum, err
			}
			e.mu.Lock()
			version, nextMinimum, proofErr := e.version, e.minimum, e.proofErr
			e.mu.Unlock()
			return version, nextMinimum, proofErr
		}
	}
	return eng.GetPrivilegeCacheVersion(ctx, ses.GetTenantInfo().GetTenantID(), minimum)
}

func authorizationReadOption(ctx context.Context, ses *Session, legacy *BackgroundExecOption) (*BackgroundExecOption, error) {
	opts := *legacy
	e := authorizationEpochFor(ctx, ses)
	eng := sessionCatalogEngine(ses)
	if e == nil || eng == nil {
		return &opts, nil
	}
	a, err := e.snapshot(ctx, eng)
	if err != nil {
		return nil, err
	}
	if a != nil {
		opts.authorizationSnapshot = a
		opts.forcePessimisticRC = false
		opts.cancelTxnCreateWithRequest = true
	}
	return &opts, nil
}

func statementSnapshotOption(ctx context.Context, ses *Session, cli txnclient.TxnClient) (txnclient.TxnOption, error) {
	e := authorizationEpochFor(ctx, ses)
	if e == nil {
		return nil, nil
	}
	if rewriteRuleSessionIdentity(ses) != e.identity {
		return nil, moerr.NewInternalError(ctx, "authorization principal changed during statement")
	}
	e.mu.Lock()
	a, ended, priorErr := e.admitted, e.ended, e.err
	e.mu.Unlock()
	if priorErr != nil {
		return nil, priorErr
	}
	if ended {
		return nil, moerr.NewInternalError(ctx, "authorization statement ended")
	}
	if a == nil {
		return nil, nil
	}
	available, err := a.Available(ctx, cli, ses.getLastCommitTS())
	if err != nil || !available {
		return nil, err
	}
	return txnclient.WithSnapshotAdmission(a), nil
}
