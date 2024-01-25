package faiss

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestFaissClustering_ComputeCenters(t *testing.T) {
	rowCnt := 3000
	dims := 5
	data := make([][]float64, rowCnt)
	loadData(rowCnt, dims, data)

	clusterCnt := 10
	cluster, _ := NewFaiss(data, clusterCnt)
	centers, err := cluster.Cluster()
	require.Nil(t, err)

	require.Equal(t, 10, len(centers))
}

func loadData(nb int, d int, xb [][]float64) {
	for r := 0; r < nb; r++ {
		xb[r] = make([]float64, d)
		for c := 0; c < d; c++ {
			xb[r][c] = rand.Float64() * 1000
		}
	}
}
