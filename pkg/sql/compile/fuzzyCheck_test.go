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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestNewFuzzyCheckAttrUsesParentUniqueColName verifies that when a hidden
// unique-index table dispatches fuzzy check via Fuzzymessage.ParentUniqueCols,
// the duplicate-entry error reports the original user-visible column name
// (e.g. "a") instead of the hidden index column ("__mo_index_idx_col").
func TestNewFuzzyCheckAttrUsesParentUniqueColName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_a_index",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName, // "__mo_index_idx_col"
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{}},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "decimal15",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{}},
			},
		},
	}

	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()

	require.Equal(t, "a", f.attr, "attr should be the user-visible column name, not the hidden index column")
	require.NotNil(t, f.col)
	require.Equal(t, "a", f.col.Name)
}

// TestNewFuzzyCheckAttrFallsBackToPkeyColName verifies the non-hidden-index
// insertion path still uses the primary-key column name for the error.
func TestNewFuzzyCheckAttrFallsBackToPkeyColName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{}},
			},
		},
	}

	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()

	require.Equal(t, "id", f.attr)
}
