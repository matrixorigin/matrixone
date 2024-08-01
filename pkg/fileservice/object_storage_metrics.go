// Copyright 2024 Matrix Origin
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

package fileservice

import (
	"context"
	"io"
	"time"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// objectStorageMetrics tracks operations and expose to prometheus
type objectStorageMetrics struct {
	upstream ObjectStorage

	numDelete     prometheus.Gauge
	numExists     prometheus.Gauge
	numList       prometheus.Gauge
	numRead       prometheus.Gauge
	numActiveRead prometheus.Gauge
	numStat       prometheus.Gauge
	numWrite      prometheus.Gauge
}

func newObjectStorageMetrics(
	upstream ObjectStorage,
	name string,
) *objectStorageMetrics {
	return &objectStorageMetrics{
		upstream: upstream,

		numRead:       metric.FSObjectStorageOperations.WithLabelValues(name, "read"),
		numActiveRead: metric.FSObjectStorageOperations.WithLabelValues(name, "active-read"),
		numWrite:      metric.FSObjectStorageOperations.WithLabelValues(name, "write"),
		numDelete:     metric.FSObjectStorageOperations.WithLabelValues(name, "delete"),
		numList:       metric.FSObjectStorageOperations.WithLabelValues(name, "list"),
		numExists:     metric.FSObjectStorageOperations.WithLabelValues(name, "exists"),
		numStat:       metric.FSObjectStorageOperations.WithLabelValues(name, "stat"),
	}
}

var _ ObjectStorage = new(objectStorageMetrics)

func (o *objectStorageMetrics) Delete(ctx context.Context, keys ...string) (err error) {
	o.numDelete.Inc()
	return o.upstream.Delete(ctx, keys...)
}

func (o *objectStorageMetrics) Exists(ctx context.Context, key string) (bool, error) {
	o.numExists.Inc()
	return o.upstream.Exists(ctx, key)
}

func (o *objectStorageMetrics) List(ctx context.Context, prefix string, fn func(isPrefix bool, key string, size int64) (bool, error)) (err error) {
	o.numList.Inc()
	return o.upstream.List(ctx, prefix, fn)
}

func (o *objectStorageMetrics) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	o.numRead.Inc()
	o.numActiveRead.Inc()
	r, err := o.upstream.Read(ctx, key, min, max)
	if err != nil {
		return nil, err
	}
	return &readCloser{
		r: r,
		closeFunc: func() error {
			o.numActiveRead.Dec()
			return r.Close()
		},
	}, nil
}

func (o *objectStorageMetrics) Stat(ctx context.Context, key string) (size int64, err error) {
	o.numStat.Inc()
	return o.upstream.Stat(ctx, key)
}

func (o *objectStorageMetrics) Write(ctx context.Context, key string, r io.Reader, size int64, expire *time.Time) (err error) {
	o.numWrite.Inc()
	return o.upstream.Write(ctx, key, r, size, expire)
}
