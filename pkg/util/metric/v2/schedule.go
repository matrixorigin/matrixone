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

package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	QueryScheduleDecisionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "sql",
			Name:      "query_schedule_decision_total",
			Help:      "Total number of query scheduling decisions.",
		}, []string{"exec_type", "current_cn_policy", "reason", "result"})

	QueryScheduleWorkerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "sql",
			Name:      "query_schedule_workers",
			Help:      "Worker counts observed at each query scheduling stage.",
			Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256},
		}, []string{"kind"})

	QueryScheduleDroppedWorkerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "sql",
			Name:      "query_schedule_dropped_workers_total",
			Help:      "Total number of query workers dropped by scheduling reason.",
		}, []string{"reason"})

	QueryScheduleSelectedWorkerFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "sql",
			Name:      "query_schedule_selected_worker_failures_total",
			Help:      "Total number of selected workers rejected by final route validation.",
		}, []string{"reason"})

	QueryWorkloadPolicyDecisionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "sql",
			Name:      "query_workload_policy_decision_total",
			Help:      "Total number of workload policy decisions.",
		}, []string{"workload_class", "policy_source", "routing", "result"})

	ScanScheduleDecisionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "sql",
			Name:      "scan_schedule_decision_total",
			Help:      "Total number of scan scheduling decisions.",
		}, []string{"reason", "query_reason", "local_only", "has_stats", "force_single", "force_one_cn"})

	ScanScheduleInputHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "sql",
			Name:      "scan_schedule_input",
			Help:      "Inputs and worker counts observed for scan scheduling decisions.",
			Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384},
		}, []string{"kind"})
)

func initScheduleMetrics() {
	registry.MustRegister(QueryScheduleDecisionCounter)
	registry.MustRegister(QueryScheduleWorkerHistogram)
	registry.MustRegister(QueryScheduleDroppedWorkerCounter)
	registry.MustRegister(QueryScheduleSelectedWorkerFailureCounter)
	registry.MustRegister(QueryWorkloadPolicyDecisionCounter)
	registry.MustRegister(ScanScheduleDecisionCounter)
	registry.MustRegister(ScanScheduleInputHistogram)
}
