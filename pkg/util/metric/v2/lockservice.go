// Copyright 2023 Matrix Origin
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
	lockServiceStaleBindPurgedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lockservice",
			Name:      "stale_bind_purged_total",
			Help:      "Total number of stale lock table binds purged by lockservice allocator epoch fencing.",
		}, []string{"source"})

	lockServiceAllocatorEpochChangedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lockservice",
			Name:      "allocator_epoch_changed_total",
			Help:      "Total number of allocator epoch changes observed by lockservice.",
		}, []string{"source"})

	lockServiceAllocatorEpochRegressionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lockservice",
			Name:      "allocator_epoch_regression_total",
			Help:      "Total number of allocator epoch regressions observed by lockservice.",
		}, []string{"source"})

	LockServiceAllocatorEpochObservedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "lockservice",
			Name:      "allocator_epoch_observed",
			Help:      "Latest allocator epoch observed by lockservice.",
		})
)

func GetLockServiceStaleBindPurgedCounter(source string) prometheus.Counter {
	return lockServiceStaleBindPurgedCounter.WithLabelValues(source)
}

func GetLockServiceAllocatorEpochChangedCounter(source string) prometheus.Counter {
	return lockServiceAllocatorEpochChangedCounter.WithLabelValues(source)
}

func GetLockServiceAllocatorEpochRegressionCounter(source string) prometheus.Counter {
	return lockServiceAllocatorEpochRegressionCounter.WithLabelValues(source)
}

func initLockServiceMetrics() {
	registry.MustRegister(lockServiceStaleBindPurgedCounter)
	registry.MustRegister(lockServiceAllocatorEpochChangedCounter)
	registry.MustRegister(lockServiceAllocatorEpochRegressionCounter)
	registry.MustRegister(LockServiceAllocatorEpochObservedGauge)
}
