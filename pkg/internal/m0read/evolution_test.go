// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package m0read

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEvolutionBindingAndLayout(t *testing.T) {
	owner := EvolutionBinding{Account: 42, Database: 7, Relation: 101}
	c := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), AccountId: owner.Account, DatabaseId: owner.Database, RelationId: owner.Relation,
		RootColumns: 1, LayoutGeneration: 1, Columns: []*api.M0EvolutionColumn{
			{ColumnId: 1, Seqnum: 0, Semantics: []byte("root")},
			{ColumnId: 2, Seqnum: 1, IntroducedLayout: 1, Semantics: []byte("nullable-tail")},
		}}
	c.SchemaDigest = EvolutionDigest(c.Columns)
	for g := uint32(0); g <= 1; g++ {
		width, err := EvolutionWidth(c, owner, g)
		require.NoError(t, err)
		require.Equal(t, 1+g, width)
	}
	_, err := EvolutionWidth(c, owner, 2)
	require.Error(t, err)
	require.Error(t, ValidateEvolution(nil, owner))
	for _, change := range []func(*api.M0EvolutionContract){
		func(x *api.M0EvolutionContract) { x.FormatVersion++ },
		func(x *api.M0EvolutionContract) { x.RootId = make([]byte, 16) },
		func(x *api.M0EvolutionContract) { x.RootId = x.RootId[:15] },
		func(x *api.M0EvolutionContract) { x.Columns[0] = nil },
		func(x *api.M0EvolutionContract) { x.Columns[0].Semantics = nil },
		func(x *api.M0EvolutionContract) { x.SchemaDigest = nil },
		func(x *api.M0EvolutionContract) { x.AccountId++ },
		func(x *api.M0EvolutionContract) { x.DatabaseId++ },
		func(x *api.M0EvolutionContract) { x.RelationId++ },
		func(x *api.M0EvolutionContract) { x.RootColumns++ },
		func(x *api.M0EvolutionContract) { x.Columns[1].IntroducedLayout = 0 },
		func(x *api.M0EvolutionContract) { x.Columns[1].Seqnum = 0 },
		func(x *api.M0EvolutionContract) { x.Columns[1].ColumnId = 1 },
		func(x *api.M0EvolutionContract) { x.Columns[0].Semantics[0] ^= 1 },
	} {
		bad := proto.Clone(c).(*api.M0EvolutionContract)
		change(bad)
		require.Error(t, ValidateEvolution(bad, owner))
	}
}

func TestPlanEvolutionSchemaBinding(t *testing.T) {
	def := &plan.TableDef{TblId: 42, DbId: 7, Cols: []*plan.ColDef{{ColId: 11, Seqnum: 0, Name: "a", Typ: plan.Type{Id: 24, Width: 12, Scale: 2}}, {ColId: 29, Seqnum: 1, Name: "b", Typ: plan.Type{Id: 24, Width: 12, Scale: 2}}}}
	c := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), AccountId: 3, DatabaseId: 7, RelationId: 42, RootColumns: 1, LayoutGeneration: 1}
	for i, col := range def.Cols {
		sem, err := PlanEvolutionColumnSemantic(def, col)
		require.NoError(t, err)
		c.Columns = append(c.Columns, &api.M0EvolutionColumn{ColumnId: col.ColId, Seqnum: col.Seqnum, IntroducedLayout: uint32(i), Semantics: sem})
	}
	c.SchemaDigest = EvolutionDigest(c.Columns)
	def.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: c}))}}}}}}
	_, err := ValidatePlanEvolution(def, 3)
	require.NoError(t, err)
	_, err = ValidatePlanEvolution(def, 4)
	require.Error(t, err)
	def.Cols[0].Typ.Width++
	_, err = ValidatePlanEvolution(def, 3)
	require.Error(t, err, "same OID with changed SQL width must not match")
	def.Cols[0].Typ.Width--
	for _, change := range []struct {
		name   string
		mutate func(*plan.TableDef)
	}{
		{"nil column", func(d *plan.TableDef) { d.Cols[0] = nil }},
		{"extra column", func(d *plan.TableDef) { d.Cols = append(d.Cols, &plan.ColDef{ColId: 30, Name: "extra"}) }},
		{"missing column", func(d *plan.TableDef) { d.Cols = d.Cols[:1] }},
		{"unsupported generated semantic", func(d *plan.TableDef) { d.Cols[0].GeneratedCol = &plan.GeneratedCol{} }},
	} {
		t.Run(change.name, func(t *testing.T) {
			changed := proto.Clone(def).(*plan.TableDef)
			change.mutate(changed)
			_, err := ValidatePlanEvolution(changed, 3)
			require.Error(t, err)
		})
	}
	def.Cols = append(def.Cols, &plan.ColDef{Name: "__mo_rowid"})
	_, err = ValidatePlanEvolution(def, 3)
	require.NoError(t, err, "physical rowid is outside persistent user-column semantics")
	def.Defs = nil
	ordinary, err := ValidatePlanEvolution(def, 3)
	require.NoError(t, err)
	require.Nil(t, ordinary, "no contract is not authority")
}

func TestEvolutionCarrierAndCloneOwnership(t *testing.T) {
	_, err := PlanEvolution(nil)
	require.Error(t, err)
	carrier := func(value string) *plan.TableDef_DefType {
		return &plan.TableDef_DefType{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: value}}}}}
	}
	value := string(api.MustMarshalTblExtra(&api.SchemaExtra{}))
	_, err = PlanEvolution(&plan.TableDef{Defs: []*plan.TableDef_DefType{carrier(value), carrier(value)}})
	require.Error(t, err, "even duplicate empty carriers are contradictory metadata")
	_, err = PlanEvolution(&plan.TableDef{Defs: []*plan.TableDef_DefType{carrier(string([]byte{0xff}))}})
	require.Error(t, err, "malformed metadata must not become absent authority")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.False(t, IsCloneEvolution(ctx))
	require.Nil(t, CloneEvolution(ctx))
	original := &api.M0EvolutionContract{RootId: []byte("0123456789abcdef"), Columns: []*api.M0EvolutionColumn{{ColumnId: 11, Semantics: []byte("semantic")}}}
	bound := WithCloneEvolution(ctx, original)
	original.RootId[0] ^= 1
	original.Columns[0].Semantics[0] ^= 1
	require.True(t, IsCloneEvolution(bound))
	require.Equal(t, byte('0'), CloneEvolution(bound).RootId[0])
	require.Equal(t, byte('s'), CloneEvolution(bound).Columns[0].Semantics[0])
	child := WithCloneEvolution(bound, nil)
	require.True(t, IsCloneEvolution(child))
	require.Nil(t, CloneEvolution(child), "an ordinary source must mask its enclosing clone authority")
	require.NotNil(t, CloneEvolution(bound))
	cancel()
	require.ErrorIs(t, child.Err(), context.Canceled)
}
