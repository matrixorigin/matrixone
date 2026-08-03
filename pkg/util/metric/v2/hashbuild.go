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

// HashBuild budget metrics intentionally have only fixed-cardinality labels.
// Statement, SQL, key, generation, and tenant identities must never be labels.
var (
	HashBuildBudgetEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "hashbuild",
			Name:      "budget_events_total",
			Help:      "HashBuild memory and spill budget lifecycle transitions.",
		},
		[]string{"component", "event", "scope"},
	)
	HashBuildBudgetBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "hashbuild",
			Name:      "budget_bytes_total",
			Help:      "Bytes requested, reconciled, released, rejected, or spilled by HashBuild.",
		},
		[]string{"component", "event", "scope"},
	)
	HashBuildSpillDepthCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "hashbuild",
			Name:      "spill_depth_total",
			Help:      "Count of admitted HashBuild spill and re-spill transitions by bounded depth.",
		},
		[]string{"action", "depth"},
	)
)

func initHashBuildMetrics() {
	registry.MustRegister(HashBuildBudgetEventCounter)
	registry.MustRegister(HashBuildBudgetBytesCounter)
	registry.MustRegister(HashBuildSpillDepthCounter)
}
