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
	"fmt"
	"sync"
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

// SiriusRuntime is initialized and closed by one CN service. The lease
// manager is supplied by the storage/GC integration because constructing an
// unprotected CN-local substitute would violate snapshot safety.
type SiriusRuntime struct {
	Flight                   *sidecarflight.Runtime
	Leases                   *substrait.LeaseManager
	Resolver                 *substrait.ResolverServer
	AuthorizedClientSPKIHash []byte
	DataDir                  string
	LeaseTTL                 time.Duration
	CleanupTimeout           time.Duration
}

func (r *SiriusRuntime) Validate() error {
	if r == nil || r.Flight == nil || r.Leases == nil || !r.Leases.Ready() || !r.Leases.Protected() ||
		r.Resolver == nil || len(r.AuthorizedClientSPKIHash) != 32 || r.DataDir == "" ||
		r.LeaseTTL <= 0 || r.LeaseTTL > substrait.MaxLeaseTTL || r.CleanupTimeout <= 0 {
		return fmt.Errorf("substrait: incomplete CN Sirius runtime")
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
	readPlan  *SiriusReadPlan
	runtime   *SiriusRuntime

	once sync.Once
	done chan struct{}
	err  error
}

func newSiriusReadOwner(execution *sidecarflight.Execution, readPlan *SiriusReadPlan, runtime *SiriusRuntime) *siriusReadOwner {
	return &siriusReadOwner{execution: execution, readPlan: readPlan, runtime: runtime, done: make(chan struct{})}
}

func (o *siriusReadOwner) finish(ctx context.Context, succeeded bool) error {
	if o == nil {
		return nil
	}
	o.once.Do(func() {
		defer close(o.done)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.runtime.CleanupTimeout)
		defer cancel()
		if !succeeded {
			o.err = o.execution.CancelAndJoin(cleanupCtx)
		}
		// A failed quiescence acknowledgement deliberately retains the GC pins.
		// Releasing them would turn a cleanup timeout into use-after-GC.
		if o.err == nil {
			o.err = o.readPlan.Release(cleanupCtx, o.runtime.Leases)
		}
	})
	<-o.done
	return o.err
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
	if err != nil || accountID == 0 {
		return false, nil
	}
	statementID := c.proc.GetStmtProfile().GetStmtId()
	queryID := append([]byte(nil), statementID[:]...)
	readPlan, err := c.CompileSiriusRead(
		ctx, queryPlan, uint64(accountID), queryID, runtime.AuthorizedClientSPKIHash,
		runtime.DataDir, runtime.LeaseTTL, runtime.Leases,
	)
	if err != nil {
		if substrait.IsNotEligible(err) {
			return false, nil
		}
		return false, err
	}
	execution, prepareErr := runtime.Flight.Prepare(ctx, uint64(accountID), queryID, readPlan.Plan, readPlan.OutputTypes, readPlan.Headings)
	if prepareErr != nil {
		if sidecarflight.IsQuiescenceUnknown(prepareErr) {
			// The sidecar may still own an execution for this snapshot. Retain the
			// durable pins until an operator can establish quiescence.
			return false, prepareErr
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runtime.CleanupTimeout)
		releaseErr := readPlan.Release(releaseCtx, runtime.Leases)
		cancel()
		if sidecarflight.IsPreVisibilityFallback(prepareErr) && releaseErr == nil {
			return false, nil
		}
		return false, errors.Join(prepareErr, releaseErr)
	}
	c.siriusRead = newSiriusReadOwner(execution, readPlan, runtime)
	return true, nil
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
