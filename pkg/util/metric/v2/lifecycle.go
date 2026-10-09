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
	LifecycleJobCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "jobs_total",
			Help:      "Lifecycle child jobs by bounded mode and result.",
		},
		[]string{"mode", "result"},
	)
	LifecycleObjectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "objects_total",
			Help:      "Lifecycle Objects discovered, retired, or blocked.",
		},
		[]string{"operation"},
	)
	LifecycleBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "bytes_total",
			Help:      "Lifecycle byte volume by bounded operation.",
		},
		[]string{"operation"},
	)
	LifecycleRootTransitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "root_transitions_total",
			Help:      "Cleanup Root CAS transitions.",
		},
		[]string{"from", "to"},
	)
	LifecycleFinalTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "final_transactions_total",
			Help:      "Lifecycle final transaction outcomes.",
		},
		[]string{"mode", "result"},
	)
	LifecycleRestoreCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "restore_total",
			Help:      "Lifecycle Restore and Purge outcomes.",
		},
		[]string{"operation", "result"},
	)
	LifecycleResourceRejectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "resource_rejections_total",
			Help:      "Lifecycle-only resource admission rejections.",
		},
		[]string{"resource"},
	)
	LifecycleActiveJobGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "active_jobs",
			Help:      "Currently running Lifecycle child jobs by bounded mode.",
		},
		[]string{"mode"},
	)
	LifecycleActiveRestoreGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "active_restores",
			Help:      "Currently running Lifecycle Restore commands on this CN process.",
		},
	)
	LifecycleFullScanAgeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "observed_full_scan_age_seconds",
			Help:      "Maximum full-scan age observed in the current bounded scheduling page.",
		},
	)
	LifecycleActiveCleanupRootGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "active_cleanup_roots",
			Help:      "Latest active Cleanup Root count observed by admission.",
		},
	)
	LifecycleReservedCleanupBytesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "reserved_cleanup_bytes",
			Help:      "Latest Cleanup Root reserved-byte total observed by admission.",
		},
	)
	LifecycleProviderErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lifecycle",
			Name:      "provider_errors_total",
			Help:      "Lifecycle Archive provider errors by bounded operation.",
		},
		[]string{"operation"},
	)
)

func initLifecycleMetrics() {
	registry.MustRegister(LifecycleJobCounter)
	registry.MustRegister(LifecycleObjectCounter)
	registry.MustRegister(LifecycleBytesCounter)
	registry.MustRegister(LifecycleRootTransitionCounter)
	registry.MustRegister(LifecycleFinalTxnCounter)
	registry.MustRegister(LifecycleRestoreCounter)
	registry.MustRegister(LifecycleResourceRejectionCounter)
	registry.MustRegister(LifecycleActiveJobGauge)
	registry.MustRegister(LifecycleActiveRestoreGauge)
	registry.MustRegister(LifecycleFullScanAgeGauge)
	registry.MustRegister(LifecycleActiveCleanupRootGauge)
	registry.MustRegister(LifecycleReservedCleanupBytesGauge)
	registry.MustRegister(LifecycleProviderErrorCounter)
}
