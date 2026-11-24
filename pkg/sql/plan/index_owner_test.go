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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestClassifyIndexPKPart(t *testing.T) {
	for _, tc := range []struct {
		name     string
		parts    []string
		pk       string
		prefixes map[string]int
		want     indexPKPartKind
	}{
		{"prefix primary key", []string{"id", "e"}, "id", map[string]int{"id": 1}, indexPKPartPrefix},
		{"prefix alias", []string{catalog.CreateAlias("id"), "e"}, "id", map[string]int{"id": 1}, indexPKPartPrefix},
		{"other prefix", []string{"e", catalog.CreateAlias("id")}, "id", map[string]int{"e": 1}, indexPKPartFull},
		{"composite key", []string{"id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, catalog.CPrimaryKeyColName, map[string]int{"id": 1}, indexPKPartFull},
		{"absent", []string{"e"}, "id", nil, indexPKPartAbsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyIndexPKPart(tc.parts, tc.pk, tc.prefixes))
		})
	}
}

func TestIndexOwnerProbeKeySlot(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_varchar)}
	scan := &planpb.Node{
		BindingTags: []int32{7},
		TableDef: &planpb.TableDef{
			Pkey: &planpb.PrimaryKeyDef{PkeyColName: catalog.IndexTableIndexColName},
			Name2ColIndex: map[string]int32{
				catalog.IndexTableIndexColName:   0,
				catalog.IndexTablePrimaryColName: 1,
			},
		},
	}
	key := makeRuntimeFilterTestEq(typ, 7, 8, 0, 0)
	owner := makeRuntimeFilterTestEq(typ, 7, 8, 1, 1)
	builder := &QueryBuilder{indexOwnerProbes: map[int32]indexOwnerProbe{7: {keyCol: 0, ownerCol: 1}}}
	join := &planpb.Node{JoinType: planpb.Node_INNER, OnList: []*planpb.Expr{key, owner}}
	slot, ok := builder.indexOwnerProbeKeySlot(join, scan)
	require.True(t, ok)
	require.Equal(t, 0, slot)
	join.OnList[0], join.OnList[1] = join.OnList[1], join.OnList[0]
	slot, ok = builder.indexOwnerProbeKeySlot(join, scan)
	require.True(t, ok)
	require.Equal(t, 1, slot)
	join.JoinType = planpb.Node_LEFT
	_, ok = builder.indexOwnerProbeKeySlot(join, scan)
	require.False(t, ok)
	join.JoinType = planpb.Node_INNER
	join.OnList = append(join.OnList, key)
	_, ok = builder.indexOwnerProbeKeySlot(join, scan)
	require.False(t, ok)
	join.OnList = join.OnList[:2]
	delete(builder.indexOwnerProbes, 7)
	_, ok = builder.indexOwnerProbeKeySlot(join, scan)
	require.False(t, ok)
}

func TestIndexOwnerRuntimeFilterUsesPhysicalKey(t *testing.T) {
	builder := newRuntimeFilterSingleTestBuilder(false)
	probe := builder.qry.Nodes[0]
	typ := planpb.Type{Id: int32(types.T_varchar), Width: 128}
	probe.TableDef.Cols = []*planpb.ColDef{
		{Name: catalog.IndexTableIndexColName, Typ: typ},
		{Name: catalog.IndexTablePrimaryColName, Typ: typ},
	}
	probe.TableDef.Name2ColIndex = map[string]int32{
		catalog.IndexTableIndexColName:   0,
		catalog.IndexTablePrimaryColName: 1,
	}
	probe.TableDef.Pkey = &planpb.PrimaryKeyDef{
		PkeyColName: catalog.IndexTableIndexColName,
		Names:       []string{catalog.IndexTableIndexColName},
	}
	join := builder.qry.Nodes[2]
	join.JoinType = planpb.Node_INNER
	// The same types deliberately make an incorrectly selected slot hard to
	// detect without checking both the plan and the resulting RF specification.
	join.OnList = []*planpb.Expr{
		makeRuntimeFilterTestEq(typ, 1, 2, 1, 1),
		makeRuntimeFilterTestEq(typ, 1, 2, 0, 0),
	}
	builder.indexOwnerProbes = map[int32]indexOwnerProbe{1: {keyCol: 0, ownerCol: 1}}
	builder.generateRuntimeFilters(2)

	require.Len(t, join.OnList, 2)
	require.EqualValues(t, 0, join.OnList[0].GetF().Args[0].GetCol().ColPos)
	require.EqualValues(t, 1, join.OnList[1].GetF().Args[0].GetCol().ColPos)
	require.Len(t, join.RuntimeFilterBuildList, 1)
	require.Len(t, probe.RuntimeFilterProbeList, 1)
	require.EqualValues(t, 0, join.RuntimeFilterBuildList[0].BuildExpr.GetCol().ColPos)
	require.EqualValues(t, 0, probe.RuntimeFilterProbeList[0].Expr.GetCol().ColPos)
}
