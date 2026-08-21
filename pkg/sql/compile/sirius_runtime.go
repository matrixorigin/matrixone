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

package compile

import (
	"context"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
)

// SiriusRuntimeKey is service-scoped: separate CNs must never share ticket or
// resolver ownership through the process default runtime.
const SiriusRuntimeKey = "sql-compile-sirius-runtime"

type siriusOffloadContextKey struct{}

// WithSiriusOffload marks an explicitly hinted statement. Absence of this
// marker leaves every native compile and execution path unchanged.
func WithSiriusOffload(ctx context.Context) context.Context {
	return context.WithValue(ctx, siriusOffloadContextKey{}, true)
}

func siriusOffloadRequested(ctx context.Context) bool {
	requested, _ := ctx.Value(siriusOffloadContextKey{}).(bool)
	return requested
}

func siriusStatementEligible(stmt tree.Statement) bool {
	selectStmt, ok := stmt.(*tree.Select)
	return ok && !selectStmt.IsPerform && selectStmt.Ep == nil
}

// SiriusRuntime is initialized and closed by one CN service. Production lease
// managers are supplied by the storage/GC integration because constructing an
// unprotected CN-local substitute would violate snapshot safety. The only
// exception is the explicit local-CN benchmark mode, where TN GC is disabled,
// and each CN owns one process-local manager and one sidecar pairing.
type SiriusRuntime struct {
	Flight                   *sidecarflight.Runtime
	Leases                   *substrait.LeaseManager
	Resolver                 *substrait.ResolverServer
	AuthorizedClientSPKIHash []byte
	DataDir                  string
	LeaseTTL                 time.Duration
	CleanupTimeout           time.Duration
	// BenchmarkNoGC is set only by the CN launcher after it verifies that the
	// paired TN has disabled GC. It permits the explicitly non-durable,
	// process-local lease manager used by the local-CN benchmark profile.
	BenchmarkNoGC bool
}

func (r *SiriusRuntime) Validate() error {
	if r == nil || r.Flight == nil || r.Leases == nil ||
		r.Resolver == nil || len(r.AuthorizedClientSPKIHash) != 32 || r.DataDir == "" ||
		r.LeaseTTL <= 0 || r.LeaseTTL > substrait.MaxLeaseTTL || r.CleanupTimeout <= 0 {
		return moerr.NewInternalErrorNoCtx("substrait: incomplete CN Sirius runtime")
	}
	if r.BenchmarkNoGC {
		if !r.Leases.BenchmarkReady() {
			return moerr.NewInternalErrorNoCtx("substrait: incomplete benchmark CN Sirius runtime")
		}
	} else if !r.Leases.DurableReady() {
		return moerr.NewInternalErrorNoCtx("substrait: incomplete CN Sirius runtime")
	}
	return nil
}

// Close obeys the ownership order: stop/cancel Flight work first, then close
// the resolver that serves the leases retained by that work.
func (r *SiriusRuntime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var result error
	if r.Flight != nil {
		result = errors.Join(result, r.Flight.Close(ctx))
	}
	if r.Resolver != nil {
		result = errors.Join(result, r.Resolver.Close(ctx))
	}
	return result
}

// ReconcileReplay transfers durable leases left by a prior CN generation to
// Flight's retry owner. Cancellation by statement identity is idempotent, and
// lease release starts only after the sidecar acknowledges quiescence.
func (r *SiriusRuntime) ReconcileReplay() error {
	if err := r.Validate(); err != nil {
		return err
	}
	var result error
	for _, pending := range r.Leases.PendingExecutions() {
		readRefs := cloneReadRefs(pending.ReadRefs)
		err := r.Flight.Reconcile(pending.AccountID, pending.QueryID, func(ctx context.Context) error {
			return releaseReadRefs(ctx, r.Leases, readRefs)
		})
		result = errors.Join(result, err)
	}
	return result
}

func lookupSiriusRuntime(service string) (*SiriusRuntime, bool) {
	runtime := moruntime.ServiceRuntime(service)
	if runtime == nil {
		return nil, false
	}
	value, ok := runtime.GetGlobalVariables(SiriusRuntimeKey)
	if !ok {
		return nil, false
	}
	result, ok := value.(*SiriusRuntime)
	return result, ok && result != nil && result.Validate() == nil
}

type siriusReadOwner struct {
	execution *sidecarflight.Execution
	runtime   *SiriusRuntime
}

func newSiriusReadOwner(execution *sidecarflight.Execution, runtime *SiriusRuntime) *siriusReadOwner {
	return &siriusReadOwner{execution: execution, runtime: runtime}
}

func (o *siriusReadOwner) finish(ctx context.Context, succeeded bool) error {
	if o == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.runtime.CleanupTimeout)
	defer cancel()
	if succeeded {
		return o.execution.CleanupAfterRun(cleanupCtx, nil)
	}
	return o.execution.Cleanup(cleanupCtx)
}

func (c *Compile) tryCompileSiriusRead(ctx context.Context, queryPlan *planpb.Plan) (bool, error) {
	if c == nil || !siriusOffloadRequested(ctx) || c.isPrepare || c.isInternal || !siriusStatementEligible(c.stmt) {
		return false, nil
	}
	runtime, ok := lookupSiriusRuntime(c.proc.GetService())
	if !ok {
		return false, nil
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, nil
	}
	statementID := c.proc.GetStmtProfile().GetStmtId()
	queryID := append([]byte(nil), statementID[:]...)
	readPlan, err := c.CompileSiriusRead(
		ctx, queryPlan, uint64(accountID), queryID, runtime.AuthorizedClientSPKIHash,
		runtime.DataDir, runtime.LeaseTTL, runtime.Leases,
	)
	if err != nil {
		if readPlan != nil {
			return false, errors.Join(err, runtime.recoverAdmittedRead(ctx, uint64(accountID), queryID, readPlan))
		}
		if substrait.IsNotEligible(err) {
			return false, nil
		}
		return false, err
	}
	execution, prepareErr := runtime.Flight.Prepare(
		ctx, uint64(accountID), queryID, readPlan.Plan, readPlan.OutputTypes, readPlan.Headings,
		readPlan.LeaseExpiresAt.Add(-runtime.CleanupTimeout),
		func(releaseCtx context.Context) error {
			return readPlan.Release(releaseCtx, runtime.Leases)
		},
	)
	if prepareErr != nil {
		if sidecarflight.IsPreVisibilityFallback(prepareErr) {
			return false, nil
		}
		return false, prepareErr
	}
	c.siriusRead = newSiriusReadOwner(execution, runtime)
	return true, nil
}

// recoverAdmittedRead handles an operational failure after admission but
// before any Flight request exists. It first attempts bounded synchronous
// release. If any release fails, durable ownership transfers to Flight's
// identity-based reconciliation worker, which retries idempotent release.
func (r *SiriusRuntime) recoverAdmittedRead(ctx context.Context, accountID uint64, queryID []byte, plan *SiriusReadPlan) error {
	if r == nil || r.Flight == nil || plan == nil {
		return moerr.NewInternalErrorNoCtx("substrait: cannot recover admitted read without a runtime owner")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.CleanupTimeout)
	releaseErr := plan.Release(cleanupCtx, r.Leases)
	cancel()
	if releaseErr == nil {
		return nil
	}
	readRefs := cloneReadRefs(plan.ReadRefs)
	reconcileErr := r.Flight.Reconcile(accountID, append([]byte(nil), queryID...), func(releaseCtx context.Context) error {
		return releaseReadRefs(releaseCtx, r.Leases, readRefs)
	})
	return errors.Join(releaseErr, reconcileErr)
}

func cloneReadRefs(readRefs [][]byte) [][]byte {
	result := make([][]byte, len(readRefs))
	for i := range readRefs {
		result[i] = append([]byte(nil), readRefs[i]...)
	}
	return result
}

func releaseReadRefs(ctx context.Context, leases *substrait.LeaseManager, readRefs [][]byte) error {
	var result error
	for _, readRef := range readRefs {
		result = errors.Join(result, leases.Release(ctx, readRef))
	}
	return result
}

func (c *Compile) runSiriusRead(ctx context.Context) (err error) {
	owner := c.siriusRead
	if owner == nil {
		return moerr.NewInternalError(ctx, "substrait: missing Sirius execution owner")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = owner.finish(ctx, false)
			panic(recovered)
		}
	}()
	runErr := owner.execution.Run(ctx, c.proc.Mp(), c.counterSet, c.fill)
	return errors.Join(runErr, owner.finish(ctx, runErr == nil))
}
