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
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// Keep the original layout independent of production constants for compatibility
// assertions and allocation benchmarks.
func legacyDurationBuckets() []float64 {
	return append(prometheus.ExponentialBucketsRange(1e-7, 36000, 146), math.MaxFloat64)
}

func TestDurationBuckets(t *testing.T) {
	buckets := getDurationBuckets()
	legacy := legacyDurationBuckets()
	require.Len(t, buckets, 51)
	require.Subset(t, legacy, buckets, "retained le values must not change")
	require.Equal(t, legacy[0], buckets[0])
	require.Equal(t, legacy[len(legacy)-2:], buckets[len(buckets)-2:])
	for i, bound := range buckets {
		require.Greater(t, bound, float64(0))
		require.False(t, math.IsInf(bound, 0))
		if i > 0 {
			require.Greater(t, bound, buckets[i-1])
			if i < len(buckets)-1 {
				require.Less(t, bound/buckets[i-1], 1.8, "avoid gaps in the duration range")
			}
		}
	}

	other := getDurationBuckets()
	other[0] = -1
	require.Equal(t, legacy[0], buckets[0], "callers must not share mutable buckets")
	require.Equal(t, legacy[0], getDurationBuckets()[0])
}

func TestDurationHistogramGather(t *testing.T) {
	buckets := getDurationBuckets()
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "test_duration_seconds",
		Help:    "Test duration.",
		Buckets: buckets,
	})
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(histogram))
	gather := func() *dto.Histogram {
		families, err := registry.Gather()
		require.NoError(t, err)
		require.Len(t, families, 1)
		require.Equal(t, dto.MetricType_HISTOGRAM, families[0].GetType())
		require.Len(t, families[0].Metric, 1)
		return families[0].Metric[0].GetHistogram()
	}

	empty := gather()
	assertDurationHistogramSamples(t, empty, buckets, nil)
	values := []float64{
		-1, 0,
		math.Nextafter(buckets[0], 0), buckets[0], math.Nextafter(buckets[0], math.Inf(1)),
		math.Nextafter(buckets[20], 0), buckets[20], math.Nextafter(buckets[20], math.Inf(1)),
		buckets[len(buckets)-2], math.Nextafter(buckets[len(buckets)-2], math.Inf(1)),
		math.MaxFloat64, math.Inf(1),
	}
	boundaryValues := values[:len(values)-2]
	for _, value := range boundaryValues {
		histogram.Observe(value)
	}
	first := gather()
	assertDurationHistogramSamples(t, first, buckets, boundaryValues)

	for _, value := range values[len(boundaryValues):] {
		histogram.Observe(value)
	}
	assertDurationHistogramSamples(t, gather(), buckets, values)
	// A later Observe/Gather must not mutate any previously published snapshot.
	assertDurationHistogramSamples(t, empty, buckets, nil)
	assertDurationHistogramSamples(t, first, buckets, boundaryValues)
}

func assertDurationHistogramSamples(t *testing.T, histogram *dto.Histogram, buckets, values []float64) {
	t.Helper()
	require.NotNil(t, histogram)
	require.Equal(t, uint64(len(values)), histogram.GetSampleCount())
	var sum float64
	for _, value := range values {
		sum += value
	}
	require.Equal(t, sum, histogram.GetSampleSum())
	require.Len(t, histogram.Bucket, len(buckets))
	for i, bound := range buckets {
		var count uint64
		for _, value := range values {
			if value <= bound {
				count++
			}
		}
		require.Equal(t, bound, histogram.Bucket[i].GetUpperBound())
		require.Equal(t, count, histogram.Bucket[i].GetCumulativeCount(), "bucket %g", bound)
	}
}

func TestDurationHistogramHTTP(t *testing.T) {
	buckets := getDurationBuckets()
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "test_duration_seconds",
		Help:        "Test duration.",
		Buckets:     buckets,
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	values := []float64{0.001, 36001}
	for _, value := range values {
		histogram.Observe(value)
	}
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(histogram))
	recorder := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(
		recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(recorder.Body)
	require.NoError(t, err)
	require.Len(t, families, 1)
	family := families["test_duration_seconds"]
	require.NotNil(t, family)
	require.Equal(t, "Test duration.", family.GetHelp())
	require.Equal(t, dto.MetricType_HISTOGRAM, family.GetType())
	require.Len(t, family.Metric, 1)
	metric := family.Metric[0]
	require.Len(t, metric.Label, 1)
	require.Equal(t, "operation", metric.Label[0].GetName())
	require.Equal(t, "read", metric.Label[0].GetValue())
	// Text exposition adds the implicit +Inf bucket to the finite layout.
	assertDurationHistogramSamples(t, metric.GetHistogram(), append(buckets, math.Inf(1)), values)
}

func BenchmarkDurationHistogram(b *testing.B) {
	for _, layout := range []struct {
		name    string
		buckets []float64
	}{
		{name: "legacy", buckets: legacyDurationBuckets()},
		{name: "current", buckets: getDurationBuckets()},
	} {
		b.Run(layout.name, func(b *testing.B) {
			newHistogram := func() prometheus.Histogram {
				histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
					Name:    "benchmark_duration_seconds",
					Help:    "Benchmark duration.",
					Buckets: layout.buckets,
				})
				histogram.Observe(0.001)
				return histogram
			}
			b.Run("Write", func(b *testing.B) {
				histogram := newHistogram()
				var out dto.Metric
				b.ReportAllocs()
				for b.Loop() {
					if err := histogram.Write(&out); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Gather", func(b *testing.B) {
				registry := prometheus.NewRegistry()
				registry.MustRegister(newHistogram())
				b.ReportAllocs()
				for b.Loop() {
					if _, err := registry.Gather(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Observe", func(b *testing.B) {
				histogram := newHistogram()
				values := [...]float64{1e-8, 1e-6, 0.001, 1, 36001}
				i := 0
				b.ReportAllocs()
				for b.Loop() {
					histogram.Observe(values[i%len(values)])
					i++
				}
			})
		})
	}
}
