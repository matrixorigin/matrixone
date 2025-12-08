//go:build gpu

package brute_force

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestGpuBruteForce(t *testing.T) {

	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	query := [][]float32{{1, 2, 3}, {3, 4, 5}}
	dimension := uint(3)
	ncpu := uint(1)
	limit := uint(2)
	elemsz := uint(4) // float32

	idx, err := NewBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)

	err = idx.Load(nil)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: ncpu}

	keys, distances, err := idx.Search(nil, query, rt)
	require.NoError(t, err)
	fmt.Printf("keys %v, dist %v\n", keys, distances)

}
