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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryStatementRejectionReasonsAreExposedBeforeFailures(t *testing.T) {
	families, err := GetPrometheusGatherer().Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "mo_system_memory_statement_rejected_total" {
			continue
		}
		var reasons []string
		for _, metric := range family.Metric {
			require.Len(t, metric.Label, 1)
			require.Equal(t, "reason", metric.Label[0].GetName())
			require.NotNil(t, metric.Counter)
			reasons = append(reasons, metric.Label[0].GetValue())
		}
		require.ElementsMatch(t, []string{
			"domain-unmeasured", "no-headroom", "queue-order", "pressure-concurrency",
			"context-canceled", "deadline-exceeded",
		}, reasons)
		return
	}
	t.Fatal("statement rejection counter must be registered and seeded even on a healthy service")
}
