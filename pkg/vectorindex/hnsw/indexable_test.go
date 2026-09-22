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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// usearch computes in float32 and reports nothing when a stored vector's squared norm leaves that
// domain -- for L2sq two distinct vectors come back at distance 0 (subnormal) or +Inf (overflow),
// with the ranking inverted in both cases. The rows are rejected on the way in instead. This is
// not cosine-specific, so the check is not gated on the metric.
func TestAddRejectsUnindexableVector(t *testing.T) {
	for _, m := range []usearch.Metric{usearch.L2sq, usearch.Cosine, usearch.InnerProduct} {
		cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
		cfg.Usearch.Metric = m

		idx, err := NewHnswModelForBuild[float32]("indexable-test", cfg, 1, 16, "")
		require.NoError(t, err)

		for _, tc := range []struct {
			name string
			vec  []float32
		}{
			{"subnormal squared norm", []float32{1e-30, 0, 0}},
			{"overflowing squared norm", []float32{1e20, 0, 0}},
		} {
			err = idx.Add(1, tc.vec)
			require.Error(t, err, "%v %s", m, tc.name)
			require.Contains(t, err.Error(), "leaves the float32 domain")

			err = idx.AddWithoutIncr(2, tc.vec)
			require.Error(t, err, "%v %s AddWithoutIncr", m, tc.name)
		}

		// Ordinary rows, and an all-zero row, still insert: their distances are well defined.
		require.NoError(t, idx.Add(10, []float32{1, 2, 3}))
		require.NoError(t, idx.Add(11, []float32{0, 0, 0}))
		idx.Destroy()
	}
}
