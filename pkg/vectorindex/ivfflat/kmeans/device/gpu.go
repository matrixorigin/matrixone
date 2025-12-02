//go:build gpu
// +build gpu

// Copyright 2023 Matrix Origin
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
	centroids   *cuvs.Tensor[T]
}

func (c *GpuClusterer[T]) InitCentroids() error {

	return nil
}

func (c *GpuClusterer[T]) Cluster() (any, error) {

	if _, err := c.dataset.ToDevice(c.resource); err != nil {
		return nil, err
	}

	if err := ivf_flat.BuildIndex(*c.resource, c.indexParams, c.dataset, c.index); err != nil {
		return nil, err
	}

	if err := c.resource.Sync(); err != nil {
		return nil, err
	}

	nlist, err := ivf_flat.GetNLists(c.index)
	if err != nil {
		return nil, err
	}

	dim, err := ivf_flat.GetDim(c.index)
	if err != nil {
		return nil, err
	}

	centers, err := cuvs.NewTensorOnDevice[T](c.resource, []int64{int64(nlist), int64(dim)})
	if err != nil {
		return nil, err
	}
	c.centroids = &centers

	if _, err := centers.ToDevice(c.resource); err != nil {
		return nil, err
	}

	if err := ivf_flat.GetCenters(c.index, &centers); err != nil {
		return nil, err
	}

	if _, err := centers.ToHost(c.resource); err != nil {
		return nil, err
	}

	result, err := centers.Slice()
	if err != nil {
		return nil, err
	}

	return result, nil
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
	if c.centroids != nil {
		c.centroids.Close()
	}
	return nil
}

func resolveCuvsDistanceForDense(distance metric.MetricType) cuvs.Distance {
	switch distance {
	case metric.Metric_L2sqDistance:
		return cuvs.DistanceL2
	case metric.Metric_L2Distance:
		return cuvs.DistanceL2
	case metric.Metric_InnerProduct:
		return cuvs.DistanceL2
	case metric.Metric_CosineDistance:
		return cuvs.DistanceL2
	case metric.Metric_L1Distance:
		return cuvs.DistanceL2
	default:
		return cuvs.DistanceL2
	}
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
		indexParams.SetMetric(resolveCuvsDistanceForDense(distanceType))
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
