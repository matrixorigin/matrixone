//go:build gpu

// Copyright 2021 - 2022 Matrix Origin
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

package cuvs

import "testing"

// Verifies the distance metrics from pkg/vectorindex/metric (L2 / L2sq -> L2Expanded,
// inner_product -> InnerProduct, cosine -> CosineExpanded) are actually supported by
// the CAGRA and IVF-PQ GPU indexes: each builds, searches, returns the correct nearest
// neighbor, AND returns a score with the right sign. In particular InnerProduct must be
// NEGATED on the C++ side (transform_distance) so it matches MatrixOne's inner_product
// distance convention (-dot, smaller = nearer). L1 is intentionally excluded — it is
// rejected by the CREATE INDEX validator (not in OpTypeToUsearchMetric).
//
// Data: row 0 is a dominant, unique-direction vector; querying with it makes row 0 the
// unique nearest under L2, cosine AND inner-product (largest dot / zero L2 / zero cosine),
// so the expected top-1 is deterministic across all three metrics.

func metricTestData() (ds []float32, ids []int64, dim uint32, count uint64, query []float32) {
	rows := [][]float32{
		{16, 15, 14, 13, 12, 11, 10, 9}, // id 100: dominant, unique direction
		{1, 0, 0, 0, 0, 0, 0, 0},
		{2, 0, 0, 0, 0, 0, 0, 0},
		{3, 0, 0, 0, 0, 0, 0, 0},
		{4, 0, 0, 0, 0, 0, 0, 0},
		{0, 1, 0, 0, 0, 0, 0, 0},
		{0, 2, 0, 0, 0, 0, 0, 0},
		{0, 0, 3, 0, 0, 0, 0, 0},
		{0, 0, 0, 4, 0, 0, 0, 0},
		{1, 1, 0, 0, 0, 0, 0, 0},
		{2, 2, 0, 0, 0, 0, 0, 0},
		{0, 0, 1, 1, 0, 0, 0, 0},
		{3, 0, 3, 0, 0, 0, 0, 0},
		{1, 2, 3, 0, 0, 0, 0, 0},
		{0, 4, 0, 2, 0, 0, 0, 0},
		{5, 1, 0, 0, 0, 0, 0, 0},
		// extra distinct rows so count (21) > CAGRA intermediate_graph_degree (16);
		// all small, so row 0 stays the unique nearest under every metric.
		{6, 0, 0, 0, 0, 0, 0, 0},
		{0, 5, 0, 0, 0, 0, 0, 0},
		{0, 0, 4, 4, 0, 0, 0, 0},
		{2, 3, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 5, 0, 0, 0},
	}
	dim = 8
	count = uint64(len(rows))
	ds = make([]float32, 0, count*uint64(dim))
	ids = make([]int64, count)
	for i, r := range rows {
		ds = append(ds, r...)
		ids[i] = int64(100 + i)
	}
	query = append([]float32(nil), rows[0]...) // = id 100
	return
}

// metricCases enumerates the cuvs metrics that pkg/vectorindex/metric maps to for
// CAGRA / IVF-PQ. ipNegative marks the metric whose score must come back negated.
var metricCases = []struct {
	name       string
	metric     DistanceType
	ipNegative bool
}{
	{"L2Expanded", L2Expanded, false},
	{"InnerProduct", InnerProduct, true},
	{"CosineExpanded", CosineExpanded, false},
}

// checkScoreSign enforces the score convention: inner_product is negated (< 0 for a
// non-orthogonal match), L2 / cosine are non-negative (≈ 0 for the self-match).
func checkScoreSign(t *testing.T, name string, ipNegative bool, dist float32) {
	t.Helper()
	if ipNegative {
		if dist >= 0 {
			t.Fatalf("%s: inner_product score must be negated (< 0), got %v", name, dist)
		}
	} else {
		if dist < -1e-3 {
			t.Fatalf("%s: %s score must be non-negative, got %v", name, name, dist)
		}
	}
}

func TestCagraMetricSupport(t *testing.T) {
	simSkipNoGPUMetric(t)
	ds, ids, dim, count, query := metricTestData()

	for _, mc := range metricCases {
		t.Run(mc.name, func(t *testing.T) {
			bp := DefaultCagraBuildParams()
			bp.IntermediateGraphDegree = 16
			bp.GraphDegree = 8
			idx, err := NewGpuCagra[float32](ds, count, dim, mc.metric, bp, []int{0}, 1, SingleGpu, ids)
			if err != nil {
				t.Fatalf("build CAGRA(%s): %v", mc.name, err)
			}
			defer idx.Destroy()
			if err := idx.Start(); err != nil {
				t.Fatalf("start: %v", err)
			}
			if err := idx.Build(); err != nil {
				t.Fatalf("build CAGRA(%s): %v", mc.name, err)
			}
			sp := DefaultCagraSearchParams()
			sp.ItopkSize = 16
			res, err := idx.Search(query, 1, dim, 1, sp)
			if err != nil {
				t.Fatalf("search CAGRA(%s): %v", mc.name, err)
			}
			if len(res.Neighbors) != 1 || res.Neighbors[0] != 100 {
				t.Fatalf("CAGRA(%s): expected nearest id 100, got %v", mc.name, res.Neighbors)
			}
			checkScoreSign(t, "CAGRA/"+mc.name, mc.ipNegative, res.Distances[0])
		})
	}
}

func TestIvfPqMetricSupport(t *testing.T) {
	simSkipNoGPUMetric(t)
	ds, ids, dim, count, query := metricTestData()

	for _, mc := range metricCases {
		t.Run(mc.name, func(t *testing.T) {
			bp := DefaultIvfPqBuildParams()
			bp.NLists = 2
			bp.M = 8
			bp.BitsPerCode = 8
			bp.KmeansTrainsetFraction = 1.0
			idx, err := NewGpuIvfPq[float32](ds, count, dim, mc.metric, bp, []int{0}, 1, SingleGpu, ids)
			if err != nil {
				t.Fatalf("build IVF-PQ(%s): %v", mc.name, err)
			}
			defer idx.Destroy()
			if err := idx.Start(); err != nil {
				t.Fatalf("start: %v", err)
			}
			if err := idx.Build(); err != nil {
				t.Fatalf("build IVF-PQ(%s): %v", mc.name, err)
			}
			sp := DefaultIvfPqSearchParams()
			sp.NProbes = 2
			res, err := idx.Search(query, 1, dim, 1, sp)
			if err != nil {
				t.Fatalf("search IVF-PQ(%s): %v", mc.name, err)
			}
			if len(res.Neighbors) != 1 || res.Neighbors[0] != 100 {
				t.Fatalf("IVF-PQ(%s): expected nearest id 100, got %v", mc.name, res.Neighbors)
			}
			checkScoreSign(t, "IVFPQ/"+mc.name, mc.ipNegative, res.Distances[0])
		})
	}
}

func simSkipNoGPUMetric(t *testing.T) {
	t.Helper()
	if c, err := GetGpuDeviceCount(); err != nil || c < 1 {
		t.Skip("requires >= 1 GPU")
	}
}
