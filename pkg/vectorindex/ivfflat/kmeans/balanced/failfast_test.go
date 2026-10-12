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

package balanced

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// Clustering must fail rather than continue from a distance that left the element domain: an
// unreported +Inf makes every bound in the algorithm meaningless and silently mis-assigns rows.
func TestClusterFailsFastOnOutOfDomainDistance(t *testing.T) {
	ctx := context.Background()

	km, err := NewKMeans([][]float32{{0, 0}, {2e19, 2e19}, {1, 1}, {3e19, 0}},
		2, 5, 0.01, metric.Metric_L2Distance, false, 1)
	require.NoError(t, err)
	defer km.Close()
	_, err = km.Cluster(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows the element domain")

	// Ordinary data still clusters.
	km2, err := NewKMeans([][]float32{{0, 0}, {1, 1}, {5, 5}, {6, 6}},
		2, 5, 0.01, metric.Metric_L2Distance, false, 1)
	require.NoError(t, err)
	defer km2.Close()
	centroids, err := km2.Cluster(ctx)
	require.NoError(t, err)
	require.NotNil(t, centroids)
}
