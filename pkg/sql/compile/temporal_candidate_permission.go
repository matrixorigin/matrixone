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

package compile

import (
	"context"
	"errors"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ErrTemporalAccessDrift is internal control flow: the execution owner must
// rebuild the original plan without temporal access before retrying compile.
var ErrTemporalAccessDrift = errors.New("temporal access intent changed before compilation")

// prepareFixedZoneTemporalExecutionPlan makes an execution-owned copy whenever
// an admitted candidate is present. Fixed offsets have an explicit process
// wire representation; named or unclassified zones remain full range until
// their coordinator-local topology has separately been proved.
func prepareFixedZoneTemporalExecutionPlan(template *plan.Plan, proc *process.Process, authorized bool) *plan.Plan {
	if !HasTemporalIndexCandidates(template) {
		return template
	}
	execution := plan2.DeepCopyPlan(template)
	allow := false
	if authorized && proc != nil && proc.Base != nil {
		offset, fixed := types.FixedZoneOffset(proc.GetSessionInfo().TimeZone)
		allow = fixed && int64(offset) >= math.MinInt32 && int64(offset) <= math.MaxInt32
	}
	setTemporalCandidateNarrowing(execution.GetQuery(), allow)
	return execution
}

// SetValidatedTemporalAccessIntent binds the frontend's already checked
// execution snapshot to this compile. The copy cannot follow later wrapper
// mutations, and Reset clears it before a cached Compile can be reused.
func (c *Compile) SetValidatedTemporalAccessIntent(intent *plan2.TemporalPlanningIntent) {
	c.temporalAccessIntent = nil
	if intent != nil {
		copy := *intent
		c.temporalAccessIntent = &copy
	}
}

func (c *Compile) prepareTemporalExecutionPlan(ctx context.Context, template *plan.Plan) *plan.Plan {
	if !HasTemporalIndexCandidates(template) {
		return template
	}
	return prepareFixedZoneTemporalExecutionPlan(template, c.proc, c.temporalExecutionAllowed(ctx))
}

func (c *Compile) temporalExecutionAllowed(ctx context.Context) bool {
	intent := c.temporalAccessIntent
	if intent == nil || !intent.MatchesProcess(c.proc) || !intent.Placement.AllowsFixedTemporalCandidate() ||
		intent.Placement.Scheduling != c.querySchedulingIntent || intent.Placement.CurrentCN.ID != c.proc.GetService() ||
		intent.Placement.CurrentCN.Addr != c.addr || siriusOffloadRequested(ctx) {
		return false
	}
	offset, fixed := types.FixedZoneOffset(c.proc.GetSessionInfo().TimeZone)
	return fixed && int64(offset) >= math.MinInt32 && int64(offset) <= math.MaxInt32
}

func (c *Compile) temporalRetryNeedsRebuild() bool {
	return c.temporalCandidateTopology && (c.temporalAccessIntent == nil || !c.temporalAccessIntent.MatchesProcess(c.proc))
}

// HasTemporalIndexCandidates identifies plans that require the original
// execution intent to be checked before compiling a physical index path.
func HasTemporalIndexCandidates(p *plan.Plan) bool {
	return p != nil && queryHasTemporalCandidates(p.GetQuery())
}

func queryHasTemporalCandidates(query *plan.Query) bool {
	if query == nil {
		return false
	}
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		for _, list := range [][]*plan.Expr{node.BlockFilterList, node.FilterList} {
			for _, expr := range list {
				// Folding can replace a temporal helper with a FoldVal. The
				// owner marker survives that rewrite and remains sufficient to
				// deny cached physical-topology reuse.
				if expr.GetTemporalCandidateFilter() {
					return true
				}
			}
		}
	}
	return false
}

// setTemporalCandidateNarrowing mutates only the execution-owned query copy.
// The caller must prove placement and timezone identity before allowing it.
// Expressions lacking complete candidate provenance remain untouched.
func setTemporalCandidateNarrowing(query *plan.Query, allow bool) {
	if query == nil {
		return
	}
	var visit func(*plan.Expr, bool)
	visit = func(expr *plan.Expr, admitted bool) {
		if expr == nil {
			return
		}
		admitted = admitted || expr.TemporalCandidateFilter
		if fn := expr.GetF(); fn != nil {
			if fn.Func != nil && admitted {
				id, _ := function.DecodeOverloadID(fn.Func.Obj)
				if (id == function.TEMPORAL_DATETIME_BOUND || id == function.TEMPORAL_TIMESTAMP_BOUND) && len(fn.Args) == 5 &&
					fn.Args[4] != nil && types.T(fn.Args[4].Typ.Id) == types.T_bool && fn.Args[4].GetLit() != nil && !fn.Args[4].GetLit().Isnull {
					if _, valid := fn.Args[4].GetLit().Value.(*plan.Literal_Bval); valid {
						fn.Args[4] = plan2.MakePlan2BoolConstExprWithType(allow)
					}
				}
			}
			for _, arg := range fn.Args {
				visit(arg, admitted)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				visit(item, admitted)
			}
		}
	}
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		for _, list := range [][]*plan.Expr{node.BlockFilterList, node.FilterList} {
			for _, expr := range list {
				visit(expr, false)
			}
		}
	}
}

// Check every marked scan, including a missing/cleared local placement bit.
func (c *Compile) temporalIndexPlacementAllowed(template *plan.Plan) bool {
	if !plan2.HasTemporalPhysicalIndexScans(template.GetQuery()) {
		return true
	}
	intent := c.temporalAccessIntent
	if intent == nil || !intent.Placement.AllowsNamedTemporalCandidate() || plan2.GetForceScanOnMultiCN() {
		return false
	}
	for _, node := range template.GetQuery().Nodes {
		if plan2.TemporalPhysicalIndexScan(node) && (node.Stats == nil || !node.Stats.ForceOneCN) {
			return false
		}
	}
	return true
}
