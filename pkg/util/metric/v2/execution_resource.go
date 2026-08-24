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

// Execution-resource metrics intentionally have only fixed-cardinality
// labels. Statement, SQL, key, generation, tenant, and operator identities
// must never be labels.
var (
	ExecutionResourceBudgetEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "execution_resource",
			Name:      "budget_events_total",
			Help:      "Execution memory and spill budget lifecycle transitions.",
		},
		[]string{"component", "event", "scope"},
	)
	ExecutionResourceBudgetAmountCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "execution_resource",
			Name:      "budget_amount_total",
			Help:      "Resource amount requested, reconciled, released, or rejected; component determines byte or descriptor units.",
		},
		[]string{"component", "event", "scope"},
	)
)

func initExecutionResourceMetrics() {
	registry.MustRegister(ExecutionResourceBudgetEventCounter)
	registry.MustRegister(ExecutionResourceBudgetAmountCounter)
}
