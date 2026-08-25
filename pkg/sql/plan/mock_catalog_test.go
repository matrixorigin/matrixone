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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

func TestMockCompilerContextTableIDs(t *testing.T) {
	first := NewMockCompilerContext(true)
	second := NewMockCompilerContext(true)
	require.Equal(t, len(first.tables), len(second.tables))

	require.Equal(t, len(first.tables), len(first.id2name), "every current table must have exactly one reverse ID mapping")
	seen := make(map[uint64]string, len(first.tables))
	for name, tableDef := range first.tables {
		require.Greater(t, tableDef.TblId, uint64(catalog.MO_RESERVED_MAX),
			"ordinary mock table %s must not use a reserved catalog ID", name)
		if previous, exists := seen[tableDef.TblId]; exists {
			require.Failf(t, "duplicate mock table ID", "tables %s and %s use ID %d", previous, name, tableDef.TblId)
		}
		seen[tableDef.TblId] = name

		require.Equal(t, tableDef.TblId, second.tables[name].TblId, "table IDs must be deterministic")
		objRef, resolved, err := first.ResolveById(tableDef.TblId, nil)
		require.NoError(t, err)
		require.NotNil(t, objRef)
		require.NotNil(t, resolved)
		require.Equal(t, tableDef.TblId, uint64(objRef.Obj))
		require.Equal(t, tableDef.TblId, resolved.TblId)
		require.Equal(t, name, resolved.Name)
	}

	for tableID, name := range first.id2name {
		tableDef := first.tables[name]
		require.NotNil(t, tableDef)
		require.Equal(t, tableID, tableDef.TblId, "reverse ID mapping must not outlive an overwritten table")

		objRef, resolved, err := first.ResolveById(tableID, nil)
		require.NoError(t, err)
		require.NotNil(t, objRef)
		require.NotNil(t, resolved)
		require.Equal(t, tableID, uint64(objRef.Obj))
		require.Equal(t, tableID, resolved.TblId)
		require.Equal(t, name, resolved.Name)
	}
}
