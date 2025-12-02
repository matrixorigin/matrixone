//go:build gpu
// +build gpu

package device

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/rapidsai/cuvs/go/ivf_flat"
)

type GpuClusterer[T cuvs.TensorNumberType] struct {
	resource    *cuvs.Resource
	index       *ivf_flat.IvfFlatIndex
	indexParams *ivf_flat.IndexParams
	dataset     *cuvs.Tensor[T]
}

func (c *GpuClusterer[T]) InitCentroids() error {

	return nil
}

func (c *GpuClusterer[T]) Cluster() (any, error) {

	return nil, nil
}

func (c *GpuClusterer[T]) SSE() (float64, error) {
	return 0, nil
}

func (c *GpuClusterer[T]) Close() error {

	if c.indexParams != nil {
		c.indexParams.Close()
	}
	if c.dataset != nil {
		c.dataset.Close()

	}
	if c.resource != nil {
		c.resource.Close()
	}
	if c.index != nil {
		c.index.Close()
	}
	return nil
}

func NewKMeans[T types.RealNumbers](vectors [][]T, clusterCnt,
	maxIterations int, deltaThreshold float64,
	distanceType metric.MetricType, initType kmeans.InitType,
	spherical bool,
	nworker int) (kmeans.Clusterer, error) {

	switch vecs := any(vectors).(type) {
	case [][]float32:

		c := &GpuClusterer[float32]{}
		resources, err := cuvs.NewResource(nil)
		if err != nil {
			return nil, err
		}
		c.resource = &resources

		indexParams, err := ivf_flat.CreateIndexParams()
		if err != nil {
			return nil, err
		}
		indexParams.SetNLists(uint32(clusterCnt))
		//indexParams.SetMetric()
		indexParams.SetKMeansNIters(uint32(maxIterations))
		indexParams.SetKMeansTrainsetFraction(1) // train all sample
		c.indexParams = indexParams

		dataset, err := cuvs.NewTensor(vecs)
		if err != nil {
			return nil, err
		}
		c.dataset = &dataset

		return c, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("cuvs type not supported")

	}
}
