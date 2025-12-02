//go:build gpu
// +build gpu

package device

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	cuvs "github.com/rapidsai/cuvs/go"
)

type GpuClusterer struct {
}

func (c *GpuClusterer) InitCentroids() error {

	return nil
}

func (c *GpuClusterer) Cluster() (any, error) {

	return nil, nil
}

func (c *GpuClusterer) SSE() (float64, error) {
	return 0, nil
}

func NewKMeans[T types.RealNumbers](vectors [][]T, clusterCnt,
	maxIterations int, deltaThreshold float64,
	distanceType metric.MetricType, initType kmeans.InitType,
	spherical bool,
	nworker int) (kmeans.Clusterer, error) {
	resources, err := cuvs.NewResource(nil)
	if err != nil {
		return nil, err
	}
	defer resources.Close()
	return &GpuClusterer{}, nil
}
