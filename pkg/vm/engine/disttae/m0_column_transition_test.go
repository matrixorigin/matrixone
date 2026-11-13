//go:build mo_m0

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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestM0CNColumnTransitionChecksSemanticType(t *testing.T) {
	before := m0TestAppendTableDef()
	before.Cols[0].ColId = 17
	before.Cols[0].NotNull = true
	before.Cols[0].Typ = plan.Type{Id: 20, Width: 24, Scale: 2, Charset: 1, CollationVersion: 1}
	before.Cols[0].Default = &plan.Default{NullAbility: false, Expr: &plan.Expr{}}
	before.Cols[0].GeneratedCol = &plan.GeneratedCol{Expr: &plan.Expr{}, IsStored: true}
	before.Cols[0].OnUpdate = &plan.OnUpdate{Expr: &plan.Expr{}}
	makeAfter := func() *plan.TableDef {
		after := plan2.DeepCopyTableDef(before, true)
		rowid := after.Cols[len(after.Cols)-1]
		after.Cols[len(after.Cols)-1] = &plan.ColDef{Name: "added", Seqnum: 4, Default: &plan.Default{NullAbility: true}}
		after.Cols = append(after.Cols, rowid)
		return after
	}
	require.NoError(t, validateM0CNColumnTransition(before, makeAfter()))
	for _, tc := range []struct {
		name string
		edit func(*plan.TableDef)
	}{
		{"same oid different width", func(d *plan.TableDef) { d.Cols[0].Typ.Width++ }},
		{"same oid different scale", func(d *plan.TableDef) { d.Cols[0].Typ.Scale++ }},
		{"string padding semantics", func(d *plan.TableDef) { d.Cols[0].Typ.PadSpace = true }},
		{"collation", func(d *plan.TableDef) { d.Cols[0].Typ.CollationVersion++ }},
		{"collation coercibility", func(d *plan.TableDef) { d.Cols[0].Typ.CollationCoercibility++ }},
		{"collation coercibility set", func(d *plan.TableDef) { d.Cols[0].Typ.CollationCoercibilitySet = true }},
		{"collation conflict", func(d *plan.TableDef) { d.Cols[0].Typ.CollationMergeConflict = true }},
		{"column identity", func(d *plan.TableDef) { d.Cols[0].ColId++ }},
		{"column sequence", func(d *plan.TableDef) { d.Cols[0].Seqnum++ }},
		{"nullability", func(d *plan.TableDef) { d.Cols[0].Default.NullAbility = true }},
		{"default expression", func(d *plan.TableDef) { d.Cols[0].Default.Expr = nil }},
		{"default source", func(d *plan.TableDef) { d.Cols[0].Default.OriginString = "different default" }},
		{"generated expression", func(d *plan.TableDef) { d.Cols[0].GeneratedCol.Expr = nil }},
		{"generated source", func(d *plan.TableDef) { d.Cols[0].GeneratedCol.OriginString = "a + 2" }},
		{"generated storage", func(d *plan.TableDef) { d.Cols[0].GeneratedCol.IsStored = false }},
		{"on update", func(d *plan.TableDef) { d.Cols[0].OnUpdate = nil }},
		{"on update source", func(d *plan.TableDef) { d.Cols[0].OnUpdate.OriginString = "different update" }},
		{"key", func(d *plan.TableDef) { d.Pkey.Names = []string{"other"} }},
		{"rowid", func(d *plan.TableDef) { d.Cols[len(d.Cols)-1].Name = "wrong" }},
		{"duplicate name case insensitive", func(d *plan.TableDef) { d.Cols[len(d.Cols)-2].Name = "A" }},
		{"added not null", func(d *plan.TableDef) { d.Cols[len(d.Cols)-2].NotNull = true }},
		{"added unique", func(d *plan.TableDef) { d.Cols[len(d.Cols)-2].Unique = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := makeAfter()
			tc.edit(after)
			require.Error(t, validateM0CNColumnTransition(before, after))
		})
	}
	t.Run("transient provenance", func(t *testing.T) {
		after := makeAfter()
		after.Cols[0].Typ.Table = "other alias"
		require.NoError(t, validateM0CNColumnTransition(before, after))
	})
}

func TestM0AppendPositionRequiresTail(t *testing.T) {
	require.True(t, m0AppendPositionAllowed(-1, 5))
	require.True(t, m0AppendPositionAllowed(4, 5))
	require.False(t, m0AppendPositionAllowed(0, 5))
	require.False(t, m0AppendPositionAllowed(3, 5))
	require.False(t, m0AppendPositionAllowed(5, 5))
	require.False(t, m0AppendPositionAllowed(-2, 5))
	require.False(t, m0AppendPositionAllowed(-1, 0))
}
