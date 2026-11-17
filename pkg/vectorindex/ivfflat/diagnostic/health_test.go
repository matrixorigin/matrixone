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

package diagnostic

import (
	"context"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildHealth(t *testing.T) {
	b := BuildInfo{Format: 1, IndexVersion: 7, BuildTS: "12345-8", Rows: 100, TrainingSamples: 60, SourceTableID: 42, Historical: true}
	raw, err := json.Marshal(b)
	require.NoError(t, err)
	decoded, status := DecodeBuild(string(raw), 7)
	require.Empty(t, status)
	require.Equal(t, &b, decoded)
	for _, tc := range []struct {
		raw     string
		version int64
		status  string
	}{
		{"", 7, "baseline_missing"}, {"{", 7, "baseline_invalid"}, {string(raw), 8, "baseline_stale"},
		{`{"format":2}`, 0, "baseline_format_unsupported"}, {`{"format":1,"build_ts":"bad"}`, 0, "baseline_invalid"},
	} {
		_, status := DecodeBuild(tc.raw, tc.version)
		require.Equal(t, tc.status, status)
	}
	m, a := BuildHealth(400, 10, &b)
	require.Equal(t, 3., m["growth_ratio"])
	require.Equal(t, .2, m["training_sample_ratio"])
	require.Len(t, a, 1)
	require.Equal(t, "training_sample_insufficient", a[0].Reason)
	_, a = BuildHealth(100, 1, &b)
	require.Len(t, a, 1)
	require.Equal(t, int64(10), a[0].Params["lists"])
	for _, tc := range [][2]int64{{0, 1}, {1, 1}, {100, 5}, {100, 20}} {
		_, a := BuildHealth(tc[0], tc[1], nil)
		require.Empty(t, a)
	}
	_, a = BuildHealth(100, 21, nil)
	require.Len(t, a, 1)
	require.Equal(t, "lists_above_sqrt_n", a[0].Reason)
}

func TestSampledDrift(t *testing.T) {
	r := NewReservoir(10)
	scratch := []float64{1}
	for i := 0; i < 1000; i++ {
		scratch[0] = float64(i)
		r.Observe(strconv.Itoa(i), 0, scratch)
	}
	require.Len(t, r.Samples, 10)
	scratch[0] = -1
	for _, s := range r.Samples {
		v, err := strconv.Atoi(s.Key)
		require.NoError(t, err)
		require.Equal(t, float64(v), s.Vector[0])
	}
	r.capacity = 1001
	r.bytes = MaxAnalysisBytes
	r.Observe("large", 0, make([]float64, 1024))
	require.Equal(t, "sample_resource_limit", r.Status)
	require.Empty(t, r.Samples)
	b0, b1 := NewBucket(0, []float64{0}), NewBucket(1, []float64{10})
	b0.Count = 100
	b1.Count = 1
	samples := []Sample{{"old", 1, []float64{11}}, {"new1", 0, []float64{2}}, {"new2", 1, []float64{12}}}
	m := Drift(samples, map[string]bool{"old": true}, []*Bucket{b0, b1}, true)
	require.Equal(t, 4., m["new_old_residual_ratio"])
	require.Equal(t, .5, m["new_rows_in_largest_5pct_buckets_ratio"])
	require.Equal(t, int64(2), m["new_sample_count"])
	require.Equal(t, true, m["approximate"])
	// Symmetric remote clusters preserve each bucket mean. Center mismatch
	// stays zero, while new/old residuals expose the much larger new-data loss.
	symmetric := []*Bucket{NewBucket(0, []float64{-1, 0}), NewBucket(1, []float64{1, 0})}
	var remote []Sample
	oldKeys := map[string]bool{}
	for i := 0; i < 8; i++ {
		x := float64(2*(i%2) - 1)
		y := float64(2*((i/2)%2) - 1)
		if i >= 4 {
			y *= 100
		}
		key := strconv.Itoa(i)
		v := []float64{x, y}
		cid := i % 2
		require.NoError(t, symmetric[cid].Add(v))
		remote = append(remote, Sample{key, int64(cid), v})
		if i < 4 {
			oldKeys[key] = true
		}
	}
	for _, b := range symmetric {
		g, ok := b.Geometry()
		require.True(t, ok)
		require.Zero(t, g.Mismatch)
	}
	require.InDelta(t, 10000., Drift(remote, oldKeys, symmetric, false)["new_old_residual_ratio"], 1e-8)
	m = Drift(samples, map[string]bool{}, []*Bucket{b0, b1}, false)
	require.Equal(t, "cohort_unavailable", m["status"])
	require.NotContains(t, m, "new_old_residual_ratio")
}

func TestExactNeighborCoverage(t *testing.T) {
	ctx := context.Background()
	queries := []Sample{{Key: "q", Vector: []float64{0}}}
	exact := NewExactNeighbors(queries)
	membership := map[string]int64{}
	for i := 0; i < 21; i++ {
		key := strconv.Itoa(i)
		require.NoError(t, exact.Observe(ctx, key, []float64{float64(i + 1)}))
		membership[key] = int64(i % 2)
	}
	require.NoError(t, exact.Observe(ctx, "q", []float64{0}))
	keys := exact.Keys()
	slices.Sort(keys)
	want := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	require.Equal(t, want, keys, "independent ordered scalar-distance oracle, excluding self")
	buckets := []*Bucket{NewBucket(0, []float64{0}), NewBucket(1, []float64{100})}
	m, advice, err := exact.Coverage(ctx, buckets, membership, nil, .95, 1)
	require.NoError(t, err)
	curve := m["curve"].([]map[string]any)
	require.Equal(t, .5, curve[0]["recall"])
	require.Equal(t, 1., curve[1]["recall"])
	require.Equal(t, 2, m["recommended_probe_limit"])
	require.Len(t, advice, 2)
	delete(membership, "0")
	m, _, err = exact.Coverage(ctx, buckets, membership, nil, .95, 1)
	require.NoError(t, err)
	require.Equal(t, "index_coverage_unavailable", m["status"])
	require.NotContains(t, m, "curve")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, exact.Observe(cancelled, "x", []float64{1}), context.Canceled)
	limited := NewExactNeighbors(queries)
	limited.bytes = MaxAnalysisBytes
	require.ErrorIs(t, limited.Observe(ctx, "x", []float64{1}), ErrAnalysisLimit)
	require.Error(t, exact.Observe(ctx, "overflow", []float64{math.MaxFloat64}))
	require.Equal(t, []int{1, 2, 4, 8, 16, 32, 64, 128, 256}, ProbeCounts(256))
}

func TestSampleRetrain(t *testing.T) {
	samples := []Sample{{Vector: []float64{99}}, {Vector: []float64{100}}, {Vector: []float64{101}}}
	buckets := []*Bucket{NewBucket(0, []float64{0})}
	m, gain, err := Retrain(context.Background(), samples, buckets)
	require.NoError(t, err)
	require.True(t, gain)
	require.Greater(t, m["sse_ratio"].(float64), 1000.)
	require.Equal(t, []float64{0}, buckets[0].Center)
	m, _, err = Retrain(context.Background(), nil, buckets)
	require.NoError(t, err)
	require.Equal(t, "sample_limit", m["status"])
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = Retrain(cancelled, samples, buckets)
	require.ErrorIs(t, err, context.Canceled)
}
