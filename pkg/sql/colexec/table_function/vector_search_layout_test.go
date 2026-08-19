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

func TestVectorSearchAttrPos(t *testing.T) {
	full := []string{"pkid", "score", catalog.SystemSI_IVFFLAT_IncludeColPrefix + "c"}
	require.Equal(t, 0, vectorSearchAttrPos(full, "pkid"))
	require.Equal(t, 1, vectorSearchAttrPos(full, "score"))
	require.Equal(t, 2, vectorSearchAttrPos(full, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"c"))

	pruned := []string{"score"}
	require.Equal(t, -1, vectorSearchAttrPos(pruned, "pkid"))
	require.Equal(t, 0, vectorSearchAttrPos(pruned, "score"))

	shifted := []string{"pkid", catalog.SystemSI_IVFFLAT_IncludeColPrefix + "c"}
	require.Equal(t, -1, vectorSearchAttrPos(shifted, "score"))
	require.Equal(t, 1, vectorSearchAttrPos(shifted, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"c"))
	require.Equal(t, -1, vectorSearchAttrPos(nil, "pkid"))
}

func TestRequestedIvfIncludeColumnsPositionIndependent(t *testing.T) {
	prefix := catalog.SystemSI_IVFFLAT_IncludeColPrefix
	require.Equal(t, []string{"a", "b"}, requestedIvfIncludeColumns(
		[]string{"pkid", "score", prefix + "a", prefix + "b"}))
	require.Equal(t, []string{"a"}, requestedIvfIncludeColumns([]string{"score", prefix + "a"}))
	require.Equal(t, []string{"a"}, requestedIvfIncludeColumns([]string{prefix + "a"}))
	require.Nil(t, requestedIvfIncludeColumns([]string{"pkid", "score"}))
	require.Nil(t, requestedIvfIncludeColumns(nil))
}

func TestResolveVectorSearchSlots(t *testing.T) {
	prefix := catalog.SystemSI_IVFFLAT_IncludeColPrefix

	t.Run("full layout", func(t *testing.T) {
		got := resolveVectorSearchSlots(
			[]string{"pkid", "score", prefix + "a", prefix + "b"}, []string{"a", "b"}, prefix)
		require.Equal(t, 0, got.pk)
		require.Equal(t, 1, got.score)
		require.Equal(t, []int{2, 3}, got.include)
	})

	t.Run("planner pruned pkid", func(t *testing.T) {
		got := resolveVectorSearchSlots([]string{"score", prefix + "a"}, []string{"a"}, prefix)
		require.Equal(t, -1, got.pk)
		require.Equal(t, 0, got.score)
		require.Equal(t, []int{1}, got.include)
	})

	t.Run("include column pruned", func(t *testing.T) {
		got := resolveVectorSearchSlots([]string{"pkid", "score"}, []string{"a"}, prefix)
		require.Equal(t, []int{-1}, got.include)
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
