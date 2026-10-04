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

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestExecutionResourceBudgetMetricsAreGeneric(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(ExecutionResourceBudgetEventCounter))
	require.NoError(t, registry.Register(ExecutionResourceBudgetAmountCounter))
	require.NoError(t, registry.Register(HashBuildSpillDepthCounter))

	ExecutionResourceBudgetEventCounter.WithLabelValues("memory", "reserve", "query").Add(0)
	ExecutionResourceBudgetAmountCounter.WithLabelValues("memory", "reserve", "query").Add(0)
	HashBuildSpillDepthCounter.WithLabelValues("spill", "0").Add(0)

	families, err := registry.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	require.Contains(t, names, "mo_execution_resource_budget_events_total")
	require.Contains(t, names, "mo_execution_resource_budget_amount_total")
	require.Contains(t, names, "mo_hashbuild_spill_depth_total")
	require.NotContains(t, names, "mo_hashbuild_budget_events_total")
	require.NotContains(t, names, "mo_hashbuild_budget_bytes_total")
}
