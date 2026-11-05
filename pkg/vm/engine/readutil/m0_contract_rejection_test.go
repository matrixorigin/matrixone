//go:build mo_m0

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

package readutil

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestM0ContractLocationCannotFallBackOnInvalidLayout(t *testing.T) {
	// This is an invalid-schema rejection fixture, not production authority.
	def := &plan.TableDef{TblId: 9, DbId: 7, Version: 1, Cols: []*plan.ColDef{
		{Name: "a", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: "b", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: catalog.CPrimaryKeyColName, ColId: 3, Seqnum: 2, Hidden: true, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: "tail", ColId: 4, Seqnum: 3, Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: catalog.Row_ID},
	}, Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"a", "b"}}}
	// Missing account context must not affect ordinary no-contract dispatch.
	proven, _, err := buildM0ReadLayout(context.Background(), def, nil, nil)
	require.NoError(t, err)
	require.False(t, proven)
	c := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), AccountId: 42, DatabaseId: 7, RelationId: 9, RootColumns: 3, LayoutGeneration: 1}
	for i, col := range def.Cols[:4] {
		sem, err := m0read.PlanEvolutionColumnSemantic(def, col)
		require.NoError(t, err)
		g := uint32(0)
		if i == 3 {
			g = 1
		}
		c.Columns = append(c.Columns, &api.M0EvolutionColumn{ColumnId: col.ColId, Seqnum: col.Seqnum, IntroducedLayout: g, Semantics: sem})
	}
	c.SchemaDigest = m0read.EvolutionDigest(c.Columns)
	def.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: c}))}}}}}}
	// The stored semantic projection matches, but its physical key anchor is
	// missing. Contract presence makes this an error, never an ordinary fallback.
	_, err = m0read.ValidatePlanEvolution(def, 42)
	require.NoError(t, err)
	proven, _, err = buildM0ReadLayout(defines.AttachAccountId(context.Background(), 42), def, nil, nil)
	require.True(t, proven)
	require.ErrorContains(t, err, "incomplete CN layout")
}
