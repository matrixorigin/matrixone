// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// TestRemoveAddKeepsGraphReachable is the #30660 regression. A CDC DELETE then INSERT
// calls Remove then Add; usearch reuses the removed node's slot for the new vector. The
// removed node's old neighbors must stay connected, or every key reachable only through
// it disappears from search. Points on a line link each node only to its two line
// neighbors, so one remove plus a far add would cut off every key below the removed one.
func TestRemoveAddKeepsGraphReachable(t *testing.T) {
	const total = 1000
	for _, tc := range []struct {
		name    string
		removed int64
		reload  bool
	}{
		{"remove 100", 100, false},
		{"remove 500", 500, false},
		{"remove 100 after save and load", 100, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
			cfg.Usearch.Metric = usearch.L2sq
			cfg.Usearch.Quantization = usearch.F32

			idx, err := NewHnswModelForBuild[float32]("remove-add", cfg, 1, 2*total, "")
			require.NoError(t, err)
			defer func() { idx.Destroy() }()
			for i := int64(1); i <= total; i++ {
				require.NoError(t, idx.Add(i, []float32{float32(i), 1, 2}))
			}

			if tc.reload {
				path := filepath.Join(t.TempDir(), "model")
				require.NoError(t, idx.Index.Save(path))
				idx.Index.Destroy()
				idx.Index, err = usearch.NewIndex(cfg.Usearch)
				require.NoError(t, err)
				require.NoError(t, idx.Index.Load(path))
				require.NoError(t, idx.Index.Reserve(2*total))
			}

			const far = int64(2001)
			require.NoError(t, idx.Remove(tc.removed))
			require.NoError(t, idx.Add(far, []float32{777.2, 1, 2}))

			keys, _, err := idx.Search([]float32{777.2, 1, 2}, total)
			require.NoError(t, err)
			got := make(map[int64]bool, len(keys))
			for _, k := range keys {
				got[int64(k)] = true
			}
			var missing []int64
			for i := int64(1); i <= total; i++ {
				if i != tc.removed && !got[i] {
					missing = append(missing, i)
				}
			}
			require.True(t, got[far])
			require.Empty(t, missing)
			require.Len(t, keys, total)
		})
	}
}
