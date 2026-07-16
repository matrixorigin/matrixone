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

func TestPipelineRemoteReceiverWaitHistogramStaysCompact(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(PipelineRemoteReceiverWaitDurationHistogram))

	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "mo_pipeline_remote_receiver_wait_duration_seconds" {
			continue
		}
		require.Len(t, family.Metric, 6)
		for _, metric := range family.Metric {
			require.Len(t, metric.GetHistogram().Bucket, 15)
		}
		return
	}
	t.Fatal("remote receiver wait histogram was not gathered")
}
