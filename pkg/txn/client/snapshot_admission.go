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

package client

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// SnapshotAdmission belongs to one statement, not a transaction or a cache.
// Its private owner/snapshot prevent callers from minting limiter exemptions.
// End releases no transaction: ordinary New/abort/close own that lifecycle.
type SnapshotAdmission struct {
	owner    *txnClient
	snapshot timestamp.Timestamp
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.RWMutex
	claimed  bool
	ended    bool
}

// AcquireSnapshotAdmission reports unsupported clients separately from a failed
// admission. A failed supported admission must never be treated as a fallback.
func AcquireSnapshotAdmission(ctx context.Context, cli TxnClient, minimum timestamp.Timestamp) (*SnapshotAdmission, bool, error) {
	c, ok := cli.(*txnClient)
	if !ok {
		return nil, false, nil
	}
	snapshot, err := c.GetLatestSnapshot(ctx, minimum)
	if err != nil {
		return nil, true, err
	}
	if snapshot.IsEmpty() || !snapshot.Greater(minimum) {
		return nil, true, moerr.NewInternalError(ctx, "snapshot admission has no current snapshot")
	}
	// End is represented by ended under mu. A separate cancellation child is
	// only needed if creation can wait; the direct path validates before return.
	return &SnapshotAdmission{owner: c, snapshot: snapshot, ctx: ctx}, true, nil
}

func (a *SnapshotAdmission) validate(ctx context.Context, cli TxnClient, minimum timestamp.Timestamp) error {
	if a == nil || a.owner == nil || a.ctx == nil {
		return moerr.NewInternalError(ctx, "invalid snapshot admission")
	}
	c, ok := cli.(*txnClient)
	if !ok || c != a.owner || a.ended || !a.snapshot.Greater(minimum) {
		return moerr.NewInternalError(ctx, "snapshot admission owner, lifetime or minimum changed")
	}
	if c.isClosed() {
		return moerr.NewClientClosedNoCtx()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.ctx.Err()
}

// Snapshot validates the live statement capability before exposing its exact,
// already applied snapshot. Claimed admissions still serve authorization reads.
func (a *SnapshotAdmission) Snapshot(ctx context.Context, cli TxnClient, minimum timestamp.Timestamp) (timestamp.Timestamp, error) {
	if a == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "missing snapshot admission")
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.validate(ctx, cli, minimum); err != nil {
		return timestamp.Timestamp{}, err
	}
	return a.snapshot, nil
}

// Available distinguishes an ordinary second creation/retry from invalid
// ownership. The latter must fail, rather than silently create a newer proof.
func (a *SnapshotAdmission) Available(ctx context.Context, cli TxnClient, minimum timestamp.Timestamp) (bool, error) {
	if a == nil {
		return false, moerr.NewInternalError(ctx, "missing snapshot admission")
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.validate(ctx, cli, minimum); err != nil {
		return false, err
	}
	return !a.claimed, nil
}

func (a *SnapshotAdmission) claim(ctx context.Context, cli TxnClient, minimum timestamp.Timestamp) (timestamp.Timestamp, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validate(ctx, cli, minimum); err != nil {
		return timestamp.Timestamp{}, err
	}
	if a.claimed {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "snapshot admission already claimed")
	}
	a.claimed = true
	return a.snapshot, nil
}

func (a *SnapshotAdmission) End() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.ended = true
	if a.cancel != nil {
		a.cancel()
	}
	a.mu.Unlock()
}

// WithSnapshotAdmission transfers one completed admission to a data New.
func WithSnapshotAdmission(a *SnapshotAdmission) TxnOption {
	if a == nil {
		a = &SnapshotAdmission{}
	}
	return func(op *txnOperator) { op.opts.snapshotAdmission = a }
}

// WithAuthorizationSnapshot fixes an independent read-only SI transaction at
// the statement snapshot. This transaction still pays its own normal admission.
func WithAuthorizationSnapshot(a *SnapshotAdmission) TxnOption {
	if a == nil {
		a = &SnapshotAdmission{}
	}
	return func(op *txnOperator) {
		op.opts.authorizationSnapshot = a
	}
}

// Creation observes the statement lifetime without lending its cancellation
// context to the long-lived transaction after New returns.
func (a *SnapshotAdmission) creationContext(ctx context.Context) (context.Context, func()) {
	a.mu.Lock()
	if a.cancel == nil {
		a.ctx, a.cancel = context.WithCancel(a.ctx)
		if a.ended {
			a.cancel()
		}
	}
	lifetime := a.ctx
	a.mu.Unlock()
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(lifetime, func() { cancel(lifetime.Err()) })
	return child, func() { stop(); cancel(nil) }
}
