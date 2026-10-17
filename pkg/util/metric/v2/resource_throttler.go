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

package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	// MemoryThrottlerAdmissionCounter records every memory admission decision.
	// The reason label is deliberately bounded so a pressure profile can use the
	// counter as a stable, machine-readable source of denied requests.
	MemoryThrottlerAdmissionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "memory_throttler",
			Name:      "admission_total",
			Help:      "Total memory throttler admission decisions.",
		}, []string{"result", "reason"})

	MemoryThrottlerAdmissionBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "memory_throttler",
			Name:      "admission_bytes_total",
			Help:      "Total bytes requested by memory throttler admission decisions.",
		}, []string{"result", "reason"})

	MemoryThrottlerReservationReleasedBytesCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "memory_throttler",
			Name:      "reservation_released_bytes_total",
			Help:      "Total reservation bytes released from the memory throttler.",
		})

	MemoryThrottlerReclamationCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "memory_throttler",
			Name:      "reclamation_total",
			Help:      "Total memory reclamation actions completed by the memory throttler.",
		}, []string{"action"})

	MemoryThrottlerReclamationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "memory_throttler",
			Name:      "reclamation_duration_seconds",
			Help:      "Duration of memory reclamation actions.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"action"})

	MemoryThrottlerReclamationHeapReleasedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "memory_throttler",
			Name:      "reclamation_heap_released_bytes_total",
			Help:      "Go heap bytes returned to the runtime allocator during reclamation.",
		}, []string{"action"})
)

func initResourceThrottlerMetricLabels() {
	// Seed the bounded label sets so a profile can distinguish a real zero from
	// an endpoint that never exposed the memory-throttler metric families.
	for _, labels := range [][2]string{
		{"granted", "accepted"},
		{"denied", "out_of_available"},
		{"denied", "invalid_request"},
	} {
		MemoryThrottlerAdmissionCounter.WithLabelValues(labels[0], labels[1])
		MemoryThrottlerAdmissionBytesCounter.WithLabelValues(labels[0], labels[1])
	}
	for _, action := range []string{
		"cache-eviction",
		"cache-eviction-free-os-memory",
		"free-os-memory",
	} {
		MemoryThrottlerReclamationCounter.WithLabelValues(action)
		MemoryThrottlerReclamationDuration.WithLabelValues(action)
		MemoryThrottlerReclamationHeapReleasedBytes.WithLabelValues(action)
	}
}
