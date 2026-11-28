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

package plan

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func stampViewGeneration(t *testing.T, def *TableDef, generation int64) {
	t.Helper()
	var data ViewData
	require.NoError(t, json.Unmarshal([]byte(def.ViewSql.View), &data))
	if generation > 0 {
		data.CTEScopeProtocol = &generation
	} else {
		data.CTEScopeProtocol = nil
	}
	encoded, err := json.Marshal(data)
	require.NoError(t, err)
	def.ViewSql.View = string(encoded)
}

// Each persisted view definition selects its own binding generation from its
// own persisted marker: an enclosing release-authored (marker-free) definition
// must never force a nested definition recorded with the current generation
// back into the legacy masking, and a marker-free nested definition inside a
// corrected outer keeps the released physical fallback.
func TestViewSchemaRequestRespectsEachDefinitionsOwnGeneration(t *testing.T) {
	innerBody := "with nation as (select 9 as n_nationkey) select n_nationkey from (with a as (select n_nationkey from nation), nation as (select 1 as n_nationkey) select n_nationkey from a) as d"
	for _, tc := range []struct {
		name              string
		marked            bool
		wantTyp           int32
		wantPhysicalDep   bool
		markOuterRecorded bool
	}{
		{"recorded inner inside legacy outer stays corrected", true, int32(types.T_int64), false, false},
		{"marker-free inner inside legacy outer keeps the physical fallback", false, int32(types.T_int32), true, false},
		{"recorded inner inside recorded outer stays corrected", true, int32(types.T_int64), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			inner := f.addView(t, "inner_g", innerBody)
			if tc.marked {
				stampViewGeneration(t, inner, 1)
			}
			outer := f.addView(t, "outer_g", "select n_nationkey from inner_g")
			if tc.markOuterRecorded {
				stampViewGeneration(t, outer, 1)
			}
			result := viewSchemaTestResult(t, f.request(t), "outer_g")
			columns, err := result.Columns()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			require.Equal(t, tc.wantTyp, columns[0].Typ.Id)
			dependencies, err := result.Dependencies()
			require.NoError(t, err)
			names := make([]string, len(dependencies))
			for i, dependency := range dependencies {
				names[i] = dependency.RelationName
			}
			if tc.wantPhysicalDep {
				require.Contains(t, names, "nation")
			} else {
				require.NotContains(t, names, "nation")
			}
		})
	}
}
