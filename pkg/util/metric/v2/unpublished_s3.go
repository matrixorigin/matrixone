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

// Service is a fixed CN identity. Object names and transaction IDs are never
// metric labels.
var (
	UnpublishedS3TicketsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "mo", Subsystem: "unpublished_s3", Name: "tickets",
		Help: "Current and peak CN cleanup tickets since process start.",
	}, []string{"service", "state"})
	UnpublishedS3AdmissionFailuresCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "unpublished_s3", Name: "admission_failures_total",
		Help: "S3 uploads or remote receipts rejected before ownership handoff because CN cleanup capacity was full.",
	}, []string{"service"})
	UnpublishedS3PendingTasksGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "mo", Subsystem: "unpublished_s3", Name: "pending_cleanup_tasks",
		Help: "CN retry callbacks still responsible for unpublished S3 objects.",
	}, []string{"service"})
	UnpublishedS3OldestTaskAgeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "mo", Subsystem: "unpublished_s3", Name: "oldest_cleanup_age_seconds",
		Help: "Age of the oldest pending unpublished S3 cleanup callback.",
	}, []string{"service"})
	UnpublishedS3DeleteFailuresCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "unpublished_s3", Name: "delete_failures_total",
		Help: "Failed deletion attempts for unpublished S3 objects in this process.",
	})
)

func initUnpublishedS3Metrics() {
	registry.MustRegister(UnpublishedS3TicketsGauge)
	registry.MustRegister(UnpublishedS3AdmissionFailuresCounter)
	registry.MustRegister(UnpublishedS3PendingTasksGauge)
	registry.MustRegister(UnpublishedS3OldestTaskAgeGauge)
	registry.MustRegister(UnpublishedS3DeleteFailuresCounter)
}
