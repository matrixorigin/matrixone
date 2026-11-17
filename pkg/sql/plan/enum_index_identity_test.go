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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEnumIndexIdentityDependencies(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	index.Parts = []string{"other"}
	uses, err := EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.False(t, uses, "an unrelated legacy index must stay on its old path")
	index.Parts = []string{"e"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses)
	index.Parts = []string{"other"}
	index.IncludedColumns = []string{"e"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "INCLUDE is an index dependency even if the key is not ENUM")
	index.IncludedColumns = nil
	index.IndexAlgoParams = `{"included_columns":"e"}`
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "catalog-loaded INCLUDE metadata must also be checked")
	index.IndexAlgoParams = ""
	index.Parts = []string{"generated"}
	table.Cols = append(table.Cols, &planpb.ColDef{
		Name: "generated", Typ: planpb.Type{Id: int32(types.T_int64)},
		GeneratedCol: &planpb.GeneratedCol{Expr: &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}}}},
	})
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "a generated index key inherits its source dependency")
	table.Cols[len(table.Cols)-1].GeneratedCol.Expr.GetCol().ColPos = 99
	_, err = EnumIndexUsesNewIdentity(table, index)
	require.Error(t, err)
	table.Cols[len(table.Cols)-1].GeneratedCol.Expr.GetCol().ColPos = int32(len(table.Cols) - 1)
	_, err = EnumIndexUsesNewIdentity(table, index)
	require.Error(t, err, "a cyclic dependency cannot be classified")
	index.Parts = []string{"other"}
	table.Pkey.PkeyColName = "e"
	table.Pkey.Names = []string{"e"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "the hidden primary-key payload also fixes the index format")
}

func TestEnumIndexIdentityInGeneratedIntermediate(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	table.Cols[1].Typ.EnumCollation = 0
	table.Cols = append(table.Cols, &planpb.ColDef{
		Name: "generated", Typ: planpb.Type{Id: int32(types.T_int64)},
		GeneratedCol: &planpb.GeneratedCol{Expr: &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: 2},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{}},
		}},
	})
	index.Parts = []string{"generated"}
	uses, err := EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses)
	index.Parts = []string{"other"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.False(t, uses, "an unrelated legacy index retains its format")
}

func TestNewEnumIndexRejectsMixedLegacyPrefix(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	index.Parts = []string{"other", "e"}
	index.IndexAlgoParams = `{"prefix_lengths":"other:1"}`
	require.ErrorContains(t, validateNewEnumIndexPrefixes(table, []*planpb.IndexDef{index}), "ENUM prefix index")
	index.Unique = false
	require.Error(t, validateNewEnumIndexPrefixes(table, []*planpb.IndexDef{index}))

	// A legacy-only prefix index remains valid on a table that also has an
	// unrelated new-identity ENUM column.
	index.Parts = []string{"other"}
	require.NoError(t, validateNewEnumIndexPrefixes(table, []*planpb.IndexDef{index}))
	// A prefix of a regular text column with a new ENUM remains supported.
	table.Cols = append(table.Cols, &planpb.ColDef{Name: "txt", Typ: planpb.Type{Id: int32(types.T_varchar)}})
	index.Parts = []string{"txt", "e"}
	index.IndexAlgoParams = `{"prefix_lengths":"txt:1"}`
	require.NoError(t, validateNewEnumIndexPrefixes(table, []*planpb.IndexDef{index}))
	// Malformed prefix metadata is not treated as absence of a prefix.
	index.IndexAlgoParams = `{"prefix_lengths":"other:0"}`
	require.Error(t, validateNewEnumIndexPrefixes(table, []*planpb.IndexDef{index}))
}
