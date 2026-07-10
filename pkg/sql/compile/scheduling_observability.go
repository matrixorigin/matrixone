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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	scheduleFailureRuntimeIneligibleSelectedWorker = "runtime-ineligible-selected-worker"
	scheduleFailureUnroutableSelectedWorker        = "unroutable-selected-worker"
	scheduleFailureCandidateProvider               = "candidate-provider"
	scheduleFailureCandidateDiscovery              = "candidate-discovery"
	scheduleFailurePoolResolution                  = "pool-resolution"
	scheduleFailureInvalidQuery                    = "invalid-query"
)

func (c *Compile) recordQuerySchedulingMetrics(
	placement schedule.QueryDecision,
) {
	result := "satisfied"
	if !placement.Satisfied {
		result = "unsatisfied"
	}
	metricv2.QueryScheduleDecisionCounter.WithLabelValues(
		queryExecTypeString(c.execType),
		placement.CurrentCNPolicy.String(),
		placement.Reason,
		result,
	).Inc()

	metricv2.QueryScheduleWorkerHistogram.WithLabelValues("discovered").Observe(float64(placement.CandidateResolution.DiscoveredCount))
	metricv2.QueryScheduleWorkerHistogram.WithLabelValues("candidate").Observe(float64(placement.ResolvedCandidateCount))
	metricv2.QueryScheduleWorkerHistogram.WithLabelValues("selected").Observe(float64(len(placement.Workers)))

	for reason, count := range droppedWorkerReasonCounts(placement.Dropped) {
		metricv2.QueryScheduleDroppedWorkerCounter.WithLabelValues(reason).Add(float64(count))
	}
}

func (c *Compile) recordScanSchedulingMetrics(
	decision schedule.ScanDecision,
	stats *schedule.ScanStats,
	forceSingle bool,
) {
	hasStats := stats != nil
	forceOneCN := hasStats && stats.ForceOneCN
	metricv2.ScanScheduleDecisionCounter.WithLabelValues(
		decision.Reason,
		c.queryPlacement.Reason,
		strconv.FormatBool(decision.LocalOnly),
		strconv.FormatBool(hasStats),
		strconv.FormatBool(forceSingle),
		strconv.FormatBool(forceOneCN),
	).Inc()
	metricv2.ScanScheduleInputHistogram.WithLabelValues("query-workers").Observe(float64(len(c.cnList)))
	metricv2.ScanScheduleInputHistogram.WithLabelValues("selected-workers").Observe(float64(len(decision.Workers)))
	if hasStats {
		metricv2.ScanScheduleInputHistogram.WithLabelValues("blocks").Observe(float64(stats.BlockNum))
		metricv2.ScanScheduleInputHistogram.WithLabelValues("dop").Observe(float64(stats.Dop))
	}
}

func recordSelectedWorkerFailureMetric(reason string) {
	metricv2.QueryScheduleSelectedWorkerFailureCounter.WithLabelValues(reason).Inc()
}
