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

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// SchedulingPreviewRequest contains the runtime context needed to preview the
// query-level placement that compilation would choose. Preview does not build
// scopes, mutate the plan, record scheduler metrics, or emit scheduler logs.
type SchedulingPreviewRequest struct {
	Context    context.Context
	Query      *plan.Query
	Engine     engine.Engine
	Process    *process.Process
	Address    string
	IsInternal bool
	Tenant     string
	Username   string
	CNLabel    map[string]string
	TxnHasDDL  bool
}

// PreviewQueryScheduling returns a best-effort scheduling trace for EXPLAIN.
// Discovery failures are represented in the trace instead of changing the
// historical behavior of EXPLAIN by returning a new error.
func PreviewQueryScheduling(req SchedulingPreviewRequest) schedule.Trace {
	recorder := new(schedule.TraceRecorder)
	recorder.SetMode(schedule.TraceModePreview)
	attempt := recorder.StartAttempt()
	if req.Query == nil {
		recorder.RecordFailure(attempt, scheduleFailureInvalidQuery, schedule.Worker{})
		return recorder.Snapshot()
	}

	c := &Compile{
		e:          req.Engine,
		proc:       req.Process,
		addr:       req.Address,
		isInternal: req.IsInternal,
		tenant:     req.Tenant,
		uid:        req.Username,
		cnLabel:    req.CNLabel,
		ncpu:       system.GoMaxProcs(),
		execType:   plan2.GetExecType(req.Query, req.TxnHasDDL, false),
	}
	ctx := req.Context
	if ctx == nil {
		ctx = context.Background()
	}
	decision, failureCategory, err := c.evaluateQueryPlacement(ctx, queryCandidateModePreview)
	if err != nil {
		recorder.RecordFailure(attempt, failureCategory, schedule.Worker{})
		return recorder.Snapshot()
	}
	recorder.RecordQuery(attempt, decision)
	return recorder.Snapshot()
}
