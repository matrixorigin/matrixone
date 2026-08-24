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
	"sort"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRPCRequestLifecycleMetricsAreRegisteredWithBoundedLabels(t *testing.T) {
	const name = "request-lifecycle-registration-test"
	outcomes := []string{"abandoned", "backend_error", "canceled", "send_error", "success", "timeout"}

	NewRPCClientRequestStartedCounterByName(name).Inc()
	for _, outcome := range outcomes {
		NewRPCClientRequestCompletedCounterByNameAndOutcome(name, outcome).Inc()
	}
	NewRPCClientRequestDurationHistogramByName(name).Observe(0.001)

	families, err := GetPrometheusGatherer().Gather()
	require.NoError(t, err)
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}

	started := byName["mo_rpc_client_request_started_total"]
	require.NotNil(t, started)
	require.Equal(t, dto.MetricType_COUNTER, started.GetType())
	require.Equal(t, []string{name}, metricLabelValues(started, "name", name))

	completed := byName["mo_rpc_client_request_completed_total"]
	require.NotNil(t, completed)
	require.Equal(t, dto.MetricType_COUNTER, completed.GetType())
	require.Equal(t, outcomes, metricLabelValues(completed, "outcome", name))

	duration := byName["mo_rpc_client_request_duration_seconds"]
	require.NotNil(t, duration)
	require.Equal(t, dto.MetricType_HISTOGRAM, duration.GetType())
	require.Equal(t, []string{name}, metricLabelValues(duration, "name", name))
	for _, metric := range duration.Metric {
		if metricLabel(metric, "name") != name {
			continue
		}
		require.LessOrEqual(t, len(metric.GetHistogram().Bucket), 50,
			"request duration must not multiply every client name by the generic 147 buckets")
		require.LessOrEqual(t, metric.GetHistogram().Bucket[0].GetUpperBound(),
			float64(time.Microsecond)/float64(time.Second))
		require.GreaterOrEqual(t,
			metric.GetHistogram().Bucket[len(metric.GetHistogram().Bucket)-1].GetUpperBound(),
			float64(10*time.Hour)/float64(time.Second))
	}
}

func metricLabelValues(family *dto.MetricFamily, labelName, metricName string) []string {
	values := make([]string, 0, len(family.Metric))
	for _, metric := range family.Metric {
		labels := make(map[string]string, len(metric.Label))
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		if labels["name"] == metricName {
			values = append(values, labels[labelName])
		}
	}
	sort.Strings(values)
	return values
}

func metricLabel(metric *dto.Metric, name string) string {
	for _, label := range metric.Label {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}
