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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestCollectIrregularIndexUpdateColsDoesNotBlockPrimaryKey(t *testing.T) {
	tableDef := &TableDef{
		Pkey: &PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		Indexes: []*IndexDef{
			{
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				Parts:           []string{"vec"},
				IncludedColumns: []string{"title"},
			},
		},
	}

	hasIrregularIndex, blockedCols := collectIrregularIndexUpdateCols(tableDef)
	require.True(t, hasIrregularIndex)
	require.True(t, blockedCols["vec"])
	require.True(t, blockedCols["title"])
	require.False(t, blockedCols["id"])
}

func TestPrimaryKeyUpdatedDetectsSingleAndCompositeKeys(t *testing.T) {
	tests := []struct {
		name       string
		tableDef   *TableDef
		updateCols map[string]tree.Expr
		want       bool
	}{
		{
			name: "single primary key updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"id"},
					PkeyColName: "id",
				},
			},
			updateCols: map[string]tree.Expr{"id": nil},
			want:       true,
		},
		{
			name: "composite primary key part updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"tenant_id", "id"},
					PkeyColName: catalog.CPrimaryKeyColName,
				},
			},
			updateCols: map[string]tree.Expr{"id": nil},
			want:       true,
		},
		{
			name: "unrelated column updated",
			tableDef: &TableDef{
				Pkey: &PrimaryKeyDef{
					Names:       []string{"id"},
					PkeyColName: "id",
				},
			},
			updateCols: map[string]tree.Expr{"title": nil},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, primaryKeyUpdated(tt.tableDef, tt.updateCols))
		})
	}
}
