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

package table_function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

// TestVectorSearchAttrPos: the search table functions must locate their output columns by
// name, because the planner prunes the ones the query does not read. Writing to a
// hard-coded slot put an int64 pk into the float64 score vector and panicked the CN.
func TestVectorSearchAttrPos(t *testing.T) {
	full := []string{"pkid", "score", catalog.SystemSI_IVFFLAT_IncludeColPrefix + "c"}
	require.Equal(t, 0, vectorSearchAttrPos(full, "pkid"))
	require.Equal(t, 1, vectorSearchAttrPos(full, "score"))
	require.Equal(t, 2, vectorSearchAttrPos(full, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"c"))

	// pkid pruned (`select l2_distance(v,q) from t order by … limit k`): score moves to 0
	// and pkid must report absent rather than aliasing onto it.
	pruned := []string{"score"}
	require.Equal(t, -1, vectorSearchAttrPos(pruned, "pkid"))
	require.Equal(t, 0, vectorSearchAttrPos(pruned, "score"))

	// score pruned ahead of an INCLUDE column shifts it too.
	shifted := []string{"pkid", catalog.SystemSI_IVFFLAT_IncludeColPrefix + "c"}
	require.Equal(t, -1, vectorSearchAttrPos(shifted, "score"))
	require.Equal(t, 1, vectorSearchAttrPos(shifted, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"c"))

	require.Equal(t, -1, vectorSearchAttrPos(nil, "pkid"))
}

// TestResolveVectorSearchSlots: the per-layout resolution the emit loops rely on. Positions
// are looked up ONCE here rather than per emitted row -- with an 8192-row batch, and one
// extra lookup per INCLUDE column per row for IVF-FLAT, the name scan was repeated tens of
// thousands of times per batch in the vector-search output path.
func TestResolveVectorSearchSlots(t *testing.T) {
	inc := catalog.SystemSI_IVFFLAT_IncludeColPrefix

	t.Run("full layout", func(t *testing.T) {
		attrs := []string{"pkid", "score", inc + "a", inc + "b"}
		got := resolveVectorSearchSlots(attrs, []string{"a", "b"}, inc)
		require.Equal(t, 0, got.pk)
		require.Equal(t, 1, got.score)
		require.Equal(t, []int{2, 3}, got.include)
	})

	t.Run("planner pruned pkid, positions shift", func(t *testing.T) {
		attrs := []string{"score", inc + "a"}
		got := resolveVectorSearchSlots(attrs, []string{"a"}, inc)
		require.Equal(t, -1, got.pk, "a pruned column is -1, never a wrong slot")
		require.Equal(t, 0, got.score)
		require.Equal(t, []int{1}, got.include)
	})

	t.Run("include column absent from the projection", func(t *testing.T) {
		got := resolveVectorSearchSlots([]string{"pkid", "score"}, []string{"a"}, inc)
		require.Equal(t, []int{-1}, got.include,
			"include stays parallel to includeColumns so the emit loop can index it positionally")
	})

	t.Run("no include columns", func(t *testing.T) {
		got := resolveVectorSearchSlots([]string{"pkid", "score"}, nil, "")
		require.Equal(t, 0, got.pk)
		require.Equal(t, 1, got.score)
		require.Nil(t, got.include)
	})

	t.Run("empty layout", func(t *testing.T) {
		got := resolveVectorSearchSlots(nil, nil, "")
		require.Equal(t, -1, got.pk)
		require.Equal(t, -1, got.score)
	})
}
