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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestSetIndexBackfillMarksOnlySpecialKeyReferences(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	spec := enumIndexTestSpec()
	spec.Index.Unique = false
	spec.Index.Parts = []string{"e", "s", "u"}
	spec.Source.Cols = append(spec.Source.Cols,
		&planpb.ColDef{Name: "s", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}},
		&planpb.ColDef{Name: "u", Typ: planpb.Type{Id: int32(types.T_uint64)}},
		&planpb.ColDef{Name: "other_set", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: "x,y"}},
	)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "insert into db.idx select serial_full(e,s,u), id from db.src", 1)
	require.NoError(t, err)
	defer stmt.Free()
	marked, err := WithEnumIndexBackfill(ctx, stmt, spec)
	require.NoError(t, err)
	state := marked.Value(enumIndexBackfillKey{}).(*enumIndexBackfillBinding)
	require.Len(t, state.refs, 2)
	require.Contains(t, state.columns, "e")
	require.Contains(t, state.columns, "s")
	require.NotContains(t, state.columns, "u")
	require.NotContains(t, state.columns, "other_set")
	for ref, typ := range state.refs {
		ok, err := enumIndexPhysicalRef(marked, ref, &Binding{db: "db", table: "src", tableID: 10}, &typ, 0)
		require.NoError(t, err)
		require.True(t, ok)
		if ref.ColName() == "s" {
			changed := typ
			changed.Enumvalues = "x,y"
			_, err = enumIndexPhysicalRef(marked, ref, &Binding{db: "db", table: "src", tableID: 10}, &changed, 0)
			require.Error(t, err)
		}
	}
	clean := WithoutEnumIndexBackfill(marked)
	require.False(t, isEnumIndexBackfill(clean))

	spec.Index.Parts = []string{"s"}
	pure, err := parsers.ParseOne(ctx, dialect.MYSQL, "insert into db.idx select s,id from db.src where s is not null", 1)
	require.NoError(t, err)
	defer pure.Free()
	pureCtx, err := WithEnumIndexBackfill(ctx, pure, spec)
	require.NoError(t, err)
	require.Len(t, pureCtx.Value(enumIndexBackfillKey{}).(*enumIndexBackfillBinding).refs, 2)
}

func TestSetIndexSerialExtractGeneralBindingRemainsNumeric(t *testing.T) {
	source := GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 0, 0)
	setType := planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}
	got, err := MakeSerialExtractExpr(t.Context(), source, setType, 0)
	require.NoError(t, err)
	require.Empty(t, got.Typ.Enumvalues)
	ordinary, err := MakeSerialExtractExpr(t.Context(), source, planpb.Type{Id: int32(types.T_uint64)}, 0)
	require.NoError(t, err)
	require.Empty(t, ordinary.Typ.Enumvalues)
	// The general binder is also used by public SERIAL_EXTRACT SQL. Its
	// historical numeric result must not acquire the internal source domain.
	public, err := BindFuncExprImplByPlanExpr(t.Context(), "serial_extract", []*planpb.Expr{
		source, makePlan2Int64ConstExprWithType(0),
		{Typ: setType, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
	})
	require.NoError(t, err)
	require.Empty(t, public.Typ.Enumvalues)
}

func TestSetIndexProjectionFallsBackOnlyForNewEnumIdentity(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	table.Cols = append(table.Cols, &planpb.ColDef{
		Name: "s", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"},
	})
	table.Name2ColIndex = map[string]int32{"id": 0, "e": 1, "other": 2, "s": 3}
	index.Unique = false
	index.Parts = []string{"e", "s", "id"}
	base := &planpb.Node{TableDef: table, BindingTags: []int32{5}}
	hidden := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "__mo_index_idx_col", Typ: planpb.Type{Id: int32(types.T_varchar)}},
		{Name: "__mo_index_pri_col", Typ: table.Cols[0].Typ},
	}}
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	mapping, err := builder.regularIndexColumnMap(index, base, 6, hidden)
	require.NoError(t, err)
	require.Contains(t, mapping, [2]int32{5, 1}, "ENUM remains index-coverable")
	require.NotContains(t, mapping, [2]int32{5, 3}, "SET must come from the base row")

	// Merely adding a new ENUM elsewhere on a legacy SET index must not
	// change that index's existing projection path.
	index.Parts = []string{"s", "id"}
	mapping, err = builder.regularIndexColumnMap(index, base, 6, hidden)
	require.NoError(t, err)
	require.Contains(t, mapping, [2]int32{5, 3})
	require.Empty(t, mapping[[2]int32{5, 3}].Typ.Enumvalues)
}

func TestSetIndexForcedReadUsesBaseRow(t *testing.T) {
	for _, suffix := range []string{"", " order by id"} {
		t.Run(suffix, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Cols[1].Typ = planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: 2}
			table.Cols[2].Typ = planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}
			logical, err := runOneStmt(mock, t, "select id,a+0,b+0 from index_hint_t force index(idx_ab) where a='a'"+suffix)
			require.NoError(t, err)
			scan := findFirstIndexScanNode(logical)
			require.NotNil(t, scan, "FORCE INDEX must not silently become a table scan")
			require.Equal(t, "idx_ab", scan.IndexScanInfo.IndexName)
			hasIndexJoin := false
			for _, node := range logical.GetQuery().Nodes {
				if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_INDEX {
					hasIndexJoin = true
				}
			}
			require.True(t, hasIndexJoin, "SET value must be recovered from the base row")
		})
	}
}
