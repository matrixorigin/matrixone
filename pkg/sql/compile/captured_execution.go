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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// ErrExecutionIntentDrift is internal control flow: the execution owner must
// rebuild the original plan without captured access before retrying compile.
var ErrExecutionIntentDrift = moerr.NewInternalError(moerr.NoReportContext(), "captured execution changed before compilation")

// SetValidatedExecutionIntent binds the frontend's already checked
// execution snapshot to this compile. The copy cannot follow later wrapper
// mutations, and Reset clears it before a cached Compile can be reused.
func (c *Compile) SetValidatedExecutionIntent(intent *plan2.ExecutionIntent) {
	c.executionIntent = nil
	if intent != nil {
		copy := *intent
		c.executionIntent = &copy
	}
}

func (c *Compile) capturedExecutionAllowed(ctx context.Context) bool {
	intent := c.executionIntent
	return intent != nil && intent.MatchesProcess(c.proc) && intent.Placement.AllowsPinnedLocalAccess() &&
		intent.Placement.Scheduling == c.querySchedulingIntent && intent.Placement.CurrentCN.ID == c.proc.GetService() &&
		intent.Placement.CurrentCN.Addr == c.addr && !siriusOffloadRequested(ctx)
}

func (c *Compile) capturedExecutionRetryNeedsRebuild() bool {
	return c.capturedExecutionTopology && (c.executionIntent == nil || !c.executionIntent.MatchesProcess(c.proc))
}

// Check every required scan, including a missing or cleared local placement bit.
func (c *Compile) capturedExecutionPlacementAllowed(template *plan.Plan) bool {
	if !plan2.HasCapturedExecutionAccess(template) {
		return true
	}
	intent := c.executionIntent
	if intent == nil || !intent.Placement.AllowsLocalIndexPlacement() || plan2.GetForceScanOnMultiCN() {
		return false
	}
	for _, node := range template.GetQuery().Nodes {
		if plan2.NodeRequiresCapturedExecution(node) && (!plan2.CapturedExecutionIndexScan(node) || node.Stats == nil || !node.Stats.ForceOneCN) {
			return false
		}
	}
	return true
}
