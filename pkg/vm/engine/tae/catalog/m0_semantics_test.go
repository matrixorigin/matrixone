// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestEvolutionSemanticAdapters(t *testing.T) {
	stored := &ColDef{Name: "a", SeqNum: 0, Type: types.NewWithCharset(types.T_varchar, 32, 0, 3), NullAbility: true}
	cn := &plan.ColDef{Name: "a", ColId: 17, Seqnum: 0, Typ: vector.TypeToProtoType(stored.Type)}
	encode := func(c *plan.ColDef) []byte {
		s, err := m0read.PlanColumnSemantic(c, false)
		require.NoError(t, err)
		b, err := m0read.EncodeColumnSemantic(s)
		require.NoError(t, err)
		return b
	}
	base := encode(cn)
	tn, err := EvolutionColumnSemantic(stored, 17)
	require.NoError(t, err)
	require.Equal(t, base, tn)
	for _, mutate := range []func(*plan.ColDef){
		func(c *plan.ColDef) { c.Typ.Width++ },
		func(c *plan.ColDef) { c.Typ.Scale++ },
		func(c *plan.ColDef) { c.Typ.CollationVersion++ },
		func(c *plan.ColDef) { c.ColId++ },
	} {
		changed := *cn
		mutate(&changed)
		require.NotEqual(t, base, encode(&changed))
	}
	cn.Typ.Table = "temporary_binding"
	cn.Typ.CollationCoercibility = 2
	require.Equal(t, base, encode(cn))
	cn.Default = &plan.Default{NullAbility: true, OriginString: "NULL", Expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}}
	require.Equal(t, base, encode(cn))
	cn.GeneratedCol = &plan.GeneratedCol{}
	s, err := m0read.PlanColumnSemantic(cn, false)
	require.NoError(t, err)
	_, err = m0read.EncodeColumnSemantic(s)
	require.Error(t, err)
	_, err = EvolutionColumnSemantic(stored, 0)
	require.Error(t, err)
	cn.GeneratedCol = nil
	cn.Default = &plan.Default{NullAbility: false}
	stored.NullAbility = false
	stored.Default, err = types.Encode(cn.Default)
	require.NoError(t, err)
	tn, err = EvolutionColumnSemantic(stored, 17)
	require.NoError(t, err)
	require.Equal(t, encode(cn), tn)
}

func TestEvolutionCompositeKeySemantics(t *testing.T) {
	schema := NewEmptySchema("key_semantics")
	require.NoError(t, schema.AppendCol("a", types.T_int32.ToType()))
	require.NoError(t, schema.AppendCol("b", types.T_int32.ToType()))
	require.NoError(t, schema.AppendPKCol("__mo_cpkey_col", types.T_varchar.ToType(), 0))
	require.NoError(t, schema.Finalize(false))
	stored := schema.ColDefs[2]
	stored.Hidden = true
	schema.Extra.M0Evolution = &api.M0EvolutionContract{Columns: []*api.M0EvolutionColumn{{ColumnId: 11}, {ColumnId: 29}, {ColumnId: 31}}}
	cn := &plan.TableDef{Cols: []*plan.ColDef{{Name: "a", ColId: 11}, {Name: "b", ColId: 29}}}
	key := &plan.ColDef{Name: stored.Name, ColId: 31, Seqnum: 2, Typ: vector.TypeToProtoType(stored.Type), Hidden: true, Primary: true, Default: &plan.Default{NullAbility: stored.NullAbility}}
	cn.Cols = append(cn.Cols, key)
	cn.Pkey = &plan.PrimaryKeyDef{PkeyColName: key.Name, CompPkeyCol: key, Names: []string{"a", "b"}}
	encodeTN := func(names []string) ([]byte, error) {
		var err error
		schema.Constraint, err = (&engine.ConstraintDef{Cts: []engine.Constraint{&engine.PrimaryKeyDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: key.Name, Names: names}}}}).MarshalBinary()
		require.NoError(t, err)
		return EvolutionSchemaColumnSemantic(schema, stored, 31)
	}
	base, err := m0read.PlanEvolutionColumnSemantic(cn, key)
	require.NoError(t, err)
	tn, err := encodeTN(cn.Pkey.Names)
	require.NoError(t, err)
	require.Equal(t, base, tn)
	cn.Pkey.Names = []string{"b", "a"}
	swapped, err := m0read.PlanEvolutionColumnSemantic(cn, key)
	require.NoError(t, err)
	require.NotEqual(t, base, swapped)
	tn, err = encodeTN(cn.Pkey.Names)
	require.NoError(t, err)
	require.Equal(t, swapped, tn)
	cn.Pkey.Names = []string{"a", "missing"}
	_, err = m0read.PlanEvolutionColumnSemantic(cn, key)
	require.Error(t, err)
	_, err = encodeTN(cn.Pkey.Names)
	require.Error(t, err)
	cn.Pkey.Names = []string{"a", "a"}
	_, err = m0read.PlanEvolutionColumnSemantic(cn, key)
	require.Error(t, err)
	_, err = encodeTN(cn.Pkey.Names)
	require.Error(t, err)
}
