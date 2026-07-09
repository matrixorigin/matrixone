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
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	scheduleTraceFailureRuntimeIneligibleSelectedWorker = "runtime-ineligible-selected-worker"
	scheduleTraceFailureUnroutableSelectedWorker        = "unroutable-selected-worker"
)

type schedulingTrace struct {
	query                 querySchedulingTrace
	lastScan              scanSchedulingTrace
	selectedWorkerFailure selectedWorkerFailureTrace
}

type querySchedulingTrace struct {
	execType             string
	currentCNPolicy      string
	reason               string
	satisfied            bool
	fallback             bool
	unsatisfied          bool
	candidateCount       int
	candidateWorkerCount int
	selectedWorkerCount  int
	droppedWorkerCount   int
	droppedUnroutable    int
	droppedDraining      int
	droppedDrained       int
	droppedDuplicate     int
}

type scanSchedulingTrace struct {
	reason               string
	queryPlacementReason string
	localOnly            bool
	hasStats             bool
	forceSingle          bool
	queryWorkerCount     int
	scanWorkerCount      int
	blockNum             int32
	dop                  int32
	forceOneCN           bool
}

type selectedWorkerFailureTrace struct {
	reason         string
	workerState    string
	workerHasRoute bool
}

func (c *Compile) recordQuerySchedulingTrace(
	placement schedule.QueryDecision,
	rawCandidateCount int,
	candidateWorkerCount int,
) {
	counts := droppedWorkerReasonCounts(placement.Dropped)
	c.scheduleTrace.query = querySchedulingTrace{
		execType:             queryExecTypeString(c.execType),
		currentCNPolicy:      placement.CurrentCNPolicy.String(),
		reason:               placement.Reason,
		satisfied:            placement.Satisfied,
		fallback:             placement.Reason == schedule.ReasonNoCandidateCN,
		unsatisfied:          !placement.Satisfied,
		candidateCount:       rawCandidateCount,
		candidateWorkerCount: candidateWorkerCount,
		selectedWorkerCount:  len(placement.Workers),
		droppedWorkerCount:   len(placement.Dropped),
		droppedUnroutable:    counts[schedule.ReasonDroppedUnroutableCN],
		droppedDraining:      counts[schedule.ReasonDroppedDrainingCN],
		droppedDrained:       counts[schedule.ReasonDroppedDrainedCN],
		droppedDuplicate:     counts[schedule.ReasonDroppedDuplicateCN],
	}
}

func (c *Compile) recordScanSchedulingTrace(
	decision schedule.ScanDecision,
	stats *schedule.ScanStats,
	forceSingle bool,
) {
	trace := scanSchedulingTrace{
		reason:               decision.Reason,
		queryPlacementReason: c.queryPlacement.Reason,
		localOnly:            decision.LocalOnly,
		forceSingle:          forceSingle,
		queryWorkerCount:     len(c.cnList),
		scanWorkerCount:      len(decision.Workers),
	}
	if stats != nil {
		trace.hasStats = true
		trace.blockNum = stats.BlockNum
		trace.dop = stats.Dop
		trace.forceOneCN = stats.ForceOneCN
	}
	c.scheduleTrace.lastScan = trace
}

func (c *Compile) recordSelectedWorkerFailureTrace(reason string, node engine.Node) {
	c.scheduleTrace.selectedWorkerFailure = selectedWorkerFailureTrace{
		reason:         reason,
		workerState:    node.WorkState.String(),
		workerHasRoute: node.Addr != "",
	}
}
