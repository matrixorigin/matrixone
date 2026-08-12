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

// TestRequestedIvfIncludeColumns_PositionIndependent: INCLUDE columns were discovered at
// attrs[2:], which silently drops them once pkid or score is pruned ahead of them.
func TestRequestedIvfIncludeColumns_PositionIndependent(t *testing.T) {
	p := catalog.SystemSI_IVFFLAT_IncludeColPrefix
	require.Equal(t, []string{"a", "b"}, requestedIvfIncludeColumns([]string{"pkid", "score", p + "a", p + "b"}))
	require.Equal(t, []string{"a"}, requestedIvfIncludeColumns([]string{"score", p + "a"}))
	require.Equal(t, []string{"a"}, requestedIvfIncludeColumns([]string{p + "a"}))
	require.Nil(t, requestedIvfIncludeColumns([]string{"pkid", "score"}))
	require.Nil(t, requestedIvfIncludeColumns(nil))
}
