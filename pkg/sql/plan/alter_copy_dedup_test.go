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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyDedupRequiresUnchangedValuesAndSources(t *testing.T) {
	for _, tc := range []struct {
		name               string
		mutate             func(*TableDef, map[string]selectExpr)
		skipPK, skipUnique bool
	}{
		{"unchanged", func(*TableDef, map[string]selectExpr) {}, true, true},
		{"primary type changes", func(d *TableDef, _ map[string]selectExpr) { d.Cols[0].Typ.Scale++ }, false, true},
		{"unique type changes", func(d *TableDef, _ map[string]selectExpr) { d.Cols[1].Typ.Width-- }, true, false},
		{"primary replaced", func(_ *TableDef, m map[string]selectExpr) {
			m["a"] = selectExpr{sexprType: exprConstValue, sexprStr: "0"}
		}, false, true},
		{"unique remapped", func(_ *TableDef, m map[string]selectExpr) {
			m["b"] = selectExpr{sexprType: exprColumnName, sexprStr: "a"}
		}, true, false},
		{"generated unique", func(d *TableDef, _ map[string]selectExpr) { d.Cols[1].GeneratedCol = &planpb.GeneratedCol{} }, true, false},
		{"unique algorithm changed", func(d *TableDef, _ map[string]selectExpr) { d.Indexes[0].IndexAlgoParams = "changed" }, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := &TableDef{Cols: []*ColDef{{Name: "a", Typ: Type{Id: 1, Width: 10, Scale: 2}}, {Name: "b", Typ: Type{Id: 1, Width: 10, Scale: 2}}}, Pkey: &PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}, Indexes: []*planpb.IndexDef{{IndexName: "u", Unique: true, Parts: []string{"b"}}}}
			newDef := DeepCopyTableDef(old, true)
			mapping := map[string]selectExpr{"a": {sexprType: exprColumnName, sexprStr: "a"}, "b": {sexprType: exprColumnName, sexprStr: "b"}}
			tc.mutate(newDef, mapping)
			require.Equal(t, tc.skipPK, skipPkDedup(old, newDef, mapping, nil))
			require.Equal(t, tc.skipUnique, skipUniqueIdxDedup(old, newDef, mapping, nil)["u"])
		})
	}
	fake := &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName}}
	require.True(t, skipPkDedup(&TableDef{}, fake, nil, nil))
	require.False(t, skipPkDedup(fake, &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}}, nil, nil))
}

func TestAlterCopyDedupLegacySinglePrimaryKey(t *testing.T) {
	old := &TableDef{
		Cols: []*ColDef{{Name: "a", Typ: Type{Id: 1}}, {Name: "b", Typ: Type{Id: 1}}},
		Pkey: &PrimaryKeyDef{PkeyColName: "a"},
	}
	updated := DeepCopyTableDef(old, true)
	sources := map[string]selectExpr{
		"a": {sexprType: exprColumnName, sexprStr: "a"},
		"b": {sexprType: exprColumnName, sexprStr: "b"},
	}
	require.True(t, skipPkDedup(old, updated, sources, nil))
	updated.Pkey.PkeyColName = "b"
	require.False(t, skipPkDedup(old, updated, sources, nil))
}
