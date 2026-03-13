//go:build gpu

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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans/elkans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type GpuClusterer[T cuvs.VectorType] struct {
	kmeans  *cuvs.GpuKMeans[T]
	nlist   int
	dim     int
	vectors []T
}

func (c *GpuClusterer[T]) InitCentroids(ctx context.Context) error {
	return nil
}

func (c *GpuClusterer[T]) Cluster(ctx context.Context) (any, error) {
	if c.kmeans == nil {
		return nil, moerr.NewInternalErrorNoCtx("GpuKMeans not initialized")
	}

	nSamples := uint64(len(c.vectors) / c.dim)
	_, _, err := c.kmeans.Fit(c.vectors, nSamples)
	if err != nil {
		return nil, err
	}

	centroids, err := c.kmeans.GetCentroids()
	if err != nil {
		return nil, err
	}

	// Reshape centroids back to [][]T
	result := make([][]T, c.nlist)
	for i := 0; i < c.nlist; i++ {
		result[i] = make([]T, c.dim)
		copy(result[i], centroids[i*c.dim:(i+1)*c.dim])
	}

	return result, nil
}

func (c *GpuClusterer[T]) SSE() (float64, error) {
	return 0, nil
}

func (c *GpuClusterer[T]) Close() error {
	if c.kmeans != nil {
		return c.kmeans.Destroy()
	}
	return nil
}

func resolveCuvsDistanceForDense(distance metric.MetricType) cuvs.DistanceType {
	switch distance {
	case metric.Metric_L2sqDistance:
		return cuvs.L2Expanded
	case metric.Metric_L2Distance:
		return cuvs.L2Expanded
	case metric.Metric_InnerProduct:
		return cuvs.InnerProduct
	case metric.Metric_CosineDistance:
		return cuvs.CosineSimilarity
	case metric.Metric_L1Distance:
		return cuvs.L1
	default:
		return cuvs.L2Expanded
	}
}

func NewKMeans[T types.RealNumbers](vectors [][]T, clusterCnt,
	maxIterations int, deltaThreshold float64,
	distanceType metric.MetricType, initType kmeans.InitType,
	spherical bool,
	nworker int) (kmeans.Clusterer, error) {

	switch vecs := any(vectors).(type) {
	case [][]float32:
		if len(vecs) == 0 {
			return nil, moerr.NewInternalErrorNoCtx("empty dataset")
		}

		dim := len(vecs[0])
		// Flatten vectors for pkg/cuvs
		flattened := make([]float32, len(vecs)*dim)
		for i, v := range vecs {
			copy(flattened[i*dim:(i+1)*dim], v)
		}

		// cuVS K-Means is currently single-GPU focused in our wrapper
		deviceID := 0
		nthread := uint32(1)

		km, err := cuvs.NewGpuKMeans[float32](uint32(clusterCnt), uint32(dim), resolveCuvsDistanceForDense(distanceType), maxIterations, deviceID, nthread)
		if err != nil {
			return nil, err
		}
		km.Start()

		c := &GpuClusterer[float32]{
			kmeans:  km,
			nlist:   clusterCnt,
			dim:     dim,
			vectors: flattened,
		}
		return c, nil

	default:
		return elkans.NewKMeans(vectors, clusterCnt, maxIterations, deltaThreshold, distanceType, initType, spherical, nworker)
	}
}
