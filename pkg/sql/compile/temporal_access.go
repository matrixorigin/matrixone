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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// ErrTemporalAccessDrift is internal control flow: the execution owner must
// rebuild the original plan without temporal access before retrying compile.
var ErrTemporalAccessDrift = moerr.NewInternalError(moerr.NoReportContext(), "temporal access intent changed before compilation")

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
		if nodeHasTemporalCandidate(node) {
			return true
		}
	}
	return false
}

func nodeHasTemporalCandidate(node *plan.Node) bool {
	if node == nil {
		return false
	}
	for _, list := range [][]*plan.Expr{node.BlockFilterList, node.FilterList} {
		for _, expr := range list {
			// The owner marker survives folding and denies physical reuse.
			if expr.GetTemporalCandidateFilter() {
				return true
			}
		}
	}
	return false
}

// Check every marked scan, including a missing/cleared local placement bit.
func (c *Compile) temporalIndexPlacementAllowed(template *plan.Plan) bool {
	if !HasTemporalIndexCandidates(template) {
		return true
	}
	intent := c.temporalAccessIntent
	if intent == nil || !intent.Placement.AllowsNamedTemporalCandidate() || plan2.GetForceScanOnMultiCN() {
		return false
	}
	for _, node := range template.GetQuery().Nodes {
		if nodeHasTemporalCandidate(node) && (!plan2.TemporalPhysicalIndexScan(node) || node.Stats == nil || !node.Stats.ForceOneCN) {
			return false
		}
	}
	return true
}
