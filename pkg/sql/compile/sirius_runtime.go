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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// SiriusRuntimeKey isolates native engine ownership between CN services.
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
	return ok && !selectStmt.IsPerform && selectStmt.Ep == nil && !statementHasSQLCalcFoundRows(stmt)
}

func siriusPlanEligible(queryPlan *planpb.Plan) bool {
	return queryPlan == nil || len(queryPlan.GetQuery().GetUnresolvedIndexHints()) == 0
}

// SiriusRuntime is initialized and closed by its CN service.
type SiriusRuntime struct {
	Engine         SiriusEngine
	CleanupTimeout time.Duration
	RequestTimeout time.Duration
}

func (r *SiriusRuntime) Validate() error {
	if r == nil || r.Engine == nil || r.CleanupTimeout <= 0 {
		return moerr.NewInternalErrorNoCtx("substrait: incomplete Sirius runtime")
	}
	if !r.Engine.Accepting() {
		return moerr.NewInvalidStateNoCtx("substrait: Sirius admission is sealed")
	}
	return nil
}
func (r *SiriusRuntime) Close(ctx context.Context) error {
	if r == nil || r.Engine == nil {
		return nil
	}
	return r.Engine.Close(ctx)
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
	execution SiriusExecution
	runtime   *SiriusRuntime
}

func newSiriusReadOwner(execution SiriusExecution, runtime *SiriusRuntime) *siriusReadOwner {
	return &siriusReadOwner{execution: execution, runtime: runtime}
}

func (o *siriusReadOwner) finish(ctx context.Context, succeeded bool) error {
	if o == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), o.runtime.CleanupTimeout,
		moerr.NewInternalErrorNoCtx("substrait: timed out cleaning up Sirius execution"))
	defer cancel()
	if succeeded {
		return o.execution.CleanupAfterRun(cleanupCtx, nil)
	}
	return o.execution.Cleanup(cleanupCtx)
}

func (c *Compile) tryCompileSiriusRead(ctx context.Context, queryPlan *planpb.Plan) (bool, error) {
	if c == nil || c.proc == nil || !siriusOffloadRequested(ctx) || c.isPrepare || c.isInternal || !siriusStatementEligible(c.stmt) {
		return false, nil
	}
	runtime, _ := lookupSiriusRuntime(c.proc.GetService())
	if runtime == nil {
		return false, nil
	}
	if err := runtime.Validate(); err != nil {
		return false, err
	}
	return c.compileEmbeddedSiriusRead(ctx, queryPlan, runtime)
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
