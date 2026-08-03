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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func newMySQLSpecialOrderMock() *MockOptimizer {
	mock := NewMockOptimizer(false)
	mock.ctxt.objects["enum_order_t"] = &planpb.ObjectRef{Obj: 9001, ObjName: "enum_order_t"}
	mock.ctxt.tables["enum_order_t"] = &planpb.TableDef{
		TblId: 9001,
		Name:  "enum_order_t",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "e", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "low,mid,high"}},
			{Name: "s", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: "red,green,blue"}},
			{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar)}},
		},
	}
	mock.ctxt.objects["enum_duplicate_t"] = &planpb.ObjectRef{Obj: 9002, ObjName: "enum_duplicate_t"}
	mock.ctxt.tables["enum_duplicate_t"] = &planpb.TableDef{
		TblId: 9002,
		Name:  "enum_duplicate_t",
		Cols: []*planpb.ColDef{
			{Name: "e", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "same,same,other"}},
		},
	}
	mock.ctxt.objects["enum_equal_fold_t"] = &planpb.ObjectRef{Obj: 9003, ObjName: "enum_equal_fold_t"}
	mock.ctxt.tables["enum_equal_fold_t"] = &planpb.TableDef{
		TblId: 9003,
		Name:  "enum_equal_fold_t",
		Cols: []*planpb.ColDef{
			{Name: "e", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "low,LOW,high"}},
		},
	}
	mock.ctxt.objects["enum_other_order_t"] = &planpb.ObjectRef{Obj: 9004, ObjName: "enum_other_order_t"}
	mock.ctxt.tables["enum_other_order_t"] = &planpb.TableDef{
		TblId: 9004,
		Name:  "enum_other_order_t",
		Cols: []*planpb.ColDef{
			{Name: "e", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "low,high,mid"}},
		},
	}
	mock.ctxt.objects["set_empty_member_t"] = &planpb.ObjectRef{Obj: 9005, ObjName: "set_empty_member_t"}
	mock.ctxt.tables["set_empty_member_t"] = &planpb.TableDef{
		TblId: 9005,
		Name:  "set_empty_member_t",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "s", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}},
		},
	}
	return mock
}

func singleSortSpec(t *testing.T, logicPlan *planpb.Plan) *planpb.OrderBySpec {
	t.Helper()
	var found []*planpb.OrderBySpec
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_SORT {
			require.Len(t, node.OrderBy, 1)
			found = append(found, node.OrderBy[0])
		}
	}
	require.Len(t, found, 1)
	return found[0]
}

func requireSingleSortKeyType(t *testing.T, logicPlan *planpb.Plan, typ types.T) {
	t.Helper()
	require.Equal(t, int32(typ), singleSortSpec(t, logicPlan).Expr.Typ.Id)
}

func requireSingleWindowOrderKeyType(t *testing.T, logicPlan *planpb.Plan, typ types.T) {
	t.Helper()
	found := make([]*planpb.Expr, 0, len(logicPlan.GetQuery().Nodes))
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_WINDOW {
			continue
		}
		require.Len(t, node.WinSpecList, 1)
		window := node.WinSpecList[0].GetW()
		require.NotNil(t, window)
		require.Len(t, window.OrderBy, 1)
		found = append(found, window.OrderBy[0].Expr)
	}
	require.Len(t, found, 1)
	require.Equal(t, int32(typ), found[0].Typ.Id)
}

func requireSingleGroupConcatOrderKeyType(t *testing.T, logicPlan *planpb.Plan, typ types.T) {
	t.Helper()
	var found []*planpb.Expr
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, aggregate := range node.AggList {
			fn := aggregate.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != NameGroupConcat {
				continue
			}
			require.Equal(t, planpb.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER, fn.AggConfigType)
			require.GreaterOrEqual(t, len(fn.AggConfig), 14)
			require.Equal(t, groupConcatOrderConfigVersion, fn.AggConfig[0])
			orderCount := int(binary.BigEndian.Uint32(fn.AggConfig[5:9]))
			require.Equal(t, 1, orderCount)
			indexOffset := 9 + orderCount
			argIndex := int(binary.BigEndian.Uint32(fn.AggConfig[indexOffset : indexOffset+4]))
			require.Less(t, argIndex, len(fn.Args))
			found = append(found, fn.Args[argIndex])
		}
	}
	require.Len(t, found, 1)
	require.Equal(t, int32(typ), found[0].Typ.Id)
}

func TestMySQLSpecialOrderProvenanceThroughQueryBoundaries(t *testing.T) {
	orderCases := []struct {
		name string
		sql  string
		typ  types.T
	}{
		{name: "direct enum", sql: "select id, e from enum_order_t order by e", typ: types.T_enum},
		{name: "cte enum", sql: "with c as (select id, e from enum_order_t) select id, e from c order by e", typ: types.T_enum},
		{name: "recursive cte enum", sql: "with recursive r(e, n) as (select e, 1 from enum_order_t union all select e, n + 1 from r where n < 2) select e from r order by e", typ: types.T_enum},
		{name: "recursive cte null member", sql: "with recursive r(e, n) as (select e, 1 from enum_order_t union all select null, n + 1 from r where n < 2) select e from r order by e", typ: types.T_enum},
		{name: "recursive cte literal clears provenance", sql: "with recursive r(e, n) as (select e, 1 from enum_order_t union all select 'bogus', n + 1 from r where n < 2) select e from r order by e", typ: types.T_varchar},
		{name: "recursive cte cast clears provenance", sql: "with recursive r(e, n) as (select e, 1 from enum_order_t union all select cast(e as char), n + 1 from r where n < 2) select e from r order by e", typ: types.T_varchar},
		{name: "recursive cte different enum clears provenance", sql: "with recursive r(e, n) as (select e, 1 from enum_order_t union all select o.e, r.n + 1 from r, enum_other_order_t o where r.n < 2) select e from r order by e", typ: types.T_varchar},
		{name: "recursive cte set", sql: "with recursive r(s, n) as (select s, 1 from enum_order_t union all select s, n + 1 from r where n < 2) select s from r order by s", typ: types.T_uint64},
		{name: "recursive cte set literal clears provenance", sql: "with recursive r(s, n) as (select s, 1 from enum_order_t union all select 'bogus', n + 1 from r where n < 2) select s from r order by s", typ: types.T_varchar},
		{name: "derived enum", sql: "select id, e from (select id, e from enum_order_t) d order by e", typ: types.T_enum},
		{name: "nested derived enum", sql: "select e from (select e from (select e from enum_order_t) d1) d2 order by e", typ: types.T_enum},
		{name: "cte alias desc nullable", sql: "with c(x, severity) as (select id, e from enum_order_t) select severity as level from c order by level desc", typ: types.T_enum},
		{name: "unselected enum key", sql: "select id from (select id, e from enum_order_t) d order by e", typ: types.T_enum},
		{name: "positional enum key", sql: "select e from (select e from enum_order_t) d order by 1 desc", typ: types.T_enum},
		{name: "union all enum", sql: "select e from enum_order_t union all select e from enum_order_t order by e", typ: types.T_enum},
		{name: "union distinct enum", sql: "select e from enum_order_t union select e from enum_order_t order by e", typ: types.T_enum},
		{name: "derived union enum", sql: "select e from (select e from enum_order_t union all select e from enum_order_t) u order by e", typ: types.T_enum},
		{name: "union enum then null", sql: "select e from enum_order_t union all select null order by e", typ: types.T_enum},
		{name: "union null then enum", sql: "select null as e union all select e from enum_order_t order by e", typ: types.T_enum},
		{name: "derived union enum and null", sql: "select e from (select e from enum_order_t union all select null) u order by e", typ: types.T_enum},
		{name: "distinct derived enum", sql: "select distinct e from (select e from enum_order_t) d order by e desc", typ: types.T_enum},
		{name: "grouped enum", sql: "select e from enum_order_t group by e order by e", typ: types.T_enum},
		{name: "derived grouped enum", sql: "select e from (select e from enum_order_t group by e) d order by e", typ: types.T_enum},
		{name: "cte grouped enum", sql: "with c as (select e from enum_order_t group by e) select e from c order by e", typ: types.T_enum},
		{name: "grouped set", sql: "select s from enum_order_t group by s order by s", typ: types.T_uint64},
		{name: "derived grouped set", sql: "select s from (select s from enum_order_t group by s) d order by s", typ: types.T_uint64},
		{name: "grouped enum expression stays lexical", sql: "select concat(e, '') as e from enum_order_t group by concat(e, '') order by e", typ: types.T_varchar},
		{name: "mixed union cast clears provenance", sql: "select e from enum_order_t union all select cast(e as char) from enum_order_t order by e", typ: types.T_varchar},
		{name: "explicit cast null union clears provenance", sql: "select e from enum_order_t union all select cast(null as char) order by e", typ: types.T_varchar},
		{name: "different union definitions clear provenance", sql: "select e from enum_order_t union all select e from enum_other_order_t order by e", typ: types.T_varchar},
		{name: "direct set", sql: "select s from enum_order_t order by s", typ: types.T_uint64},
		{name: "cte set", sql: "with c as (select s from enum_order_t) select s from c order by s", typ: types.T_uint64},
		{name: "derived set", sql: "select s from (select s from enum_order_t) d order by s", typ: types.T_uint64},
		{name: "nested set desc nullable", sql: "select s from (select s from (select s from enum_order_t) d1) d2 order by s desc", typ: types.T_uint64},
		{name: "union set", sql: "select s from enum_order_t union all select s from enum_order_t order by s", typ: types.T_uint64},
		{name: "derived union set", sql: "select s from (select s from enum_order_t union all select s from enum_order_t) u order by s", typ: types.T_uint64},
		{name: "explicit enum cast stays lexical", sql: "select cast(e as char) as value from (select e from enum_order_t) d order by value", typ: types.T_varchar},
		{name: "explicit set cast stays lexical", sql: "select cast(s as char) as value from (select s from enum_order_t) d order by value", typ: types.T_varchar},
		{name: "inner enum cast stays lexical", sql: "select e from (select cast(e as char) as e from enum_order_t) d order by e", typ: types.T_varchar},
		{name: "enum string expression stays lexical", sql: "select concat(e, '') as value from (select e from enum_order_t) d order by value", typ: types.T_varchar},
		{name: "ordinary string stays lexical", sql: "select v from (select v from enum_order_t) d order by v", typ: types.T_varchar},
	}
	for _, tc := range orderCases {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.sql)
			require.NoError(t, err)
			requireSingleSortKeyType(t, logicPlan, tc.typ)
		})
	}

	windowCases := []struct {
		name string
		sql  string
		typ  types.T
	}{
		{name: "direct enum window", sql: "select row_number() over (order by e desc) from enum_order_t", typ: types.T_enum},
		{name: "derived enum window", sql: "select row_number() over (order by e) from (select e from enum_order_t) d", typ: types.T_enum},
		{name: "cte enum window", sql: "with c as (select e from enum_order_t) select row_number() over (order by e desc) from c", typ: types.T_enum},
		{name: "derived union enum window", sql: "select row_number() over (order by e) from (select e from enum_order_t union all select e from enum_order_t) u", typ: types.T_enum},
		{name: "recursive cte enum window", sql: "with recursive r(e, n) as (select e, 1 from enum_order_t union all select e, n + 1 from r where n < 2) select row_number() over (order by e) from r", typ: types.T_enum},
		{name: "recursive cte literal window stays lexical", sql: "with recursive r(e, n) as (select e, 1 from enum_order_t union all select 'bogus', n + 1 from r where n < 2) select row_number() over (order by e) from r", typ: types.T_varchar},
		{name: "grouped enum window", sql: "select row_number() over (order by e) from enum_order_t group by e", typ: types.T_enum},
		{name: "derived grouped enum window", sql: "select row_number() over (order by e) from (select e from enum_order_t group by e) d", typ: types.T_enum},
		{name: "direct set window", sql: "select row_number() over (order by s) from enum_order_t", typ: types.T_uint64},
		{name: "cte set window", sql: "with c as (select s from enum_order_t) select row_number() over (order by s desc) from c", typ: types.T_uint64},
		{name: "derived set window", sql: "select row_number() over (order by s) from (select s from enum_order_t) d", typ: types.T_uint64},
		{name: "derived union set window", sql: "select row_number() over (order by s) from (select s from enum_order_t union all select s from enum_order_t) u", typ: types.T_uint64},
		{name: "explicit cast window stays lexical", sql: "select row_number() over (order by cast(e as char)) from (select e from enum_order_t) d", typ: types.T_varchar},
	}
	for _, tc := range windowCases {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.sql)
			require.NoError(t, err)
			requireSingleWindowOrderKeyType(t, logicPlan, tc.typ)
		})
	}
}

func TestMySQLSpecialOrderProvenanceRejectsNonReversibleEnum(t *testing.T) {
	t.Run("DDL currently accepts duplicate and EqualFold-equivalent labels", func(t *testing.T) {
		for _, sql := range []string{
			"create table enum_duplicate_ddl (e enum('same', 'same', 'other'))",
			"create table enum_equal_fold_ddl (e enum('low', 'LOW', 'high'))",
		} {
			_, err := runOneStmt(newMySQLSpecialOrderMock(), t, sql)
			require.NoError(t, err)
		}
	})

	for _, table := range []string{"enum_duplicate_t", "enum_equal_fold_t"} {
		t.Run(table+" direct keeps raw ordinal", func(t *testing.T) {
			logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
				"select e from "+table+" order by e")
			require.NoError(t, err)
			requireSingleSortKeyType(t, logicPlan, types.T_enum)
		})

		for _, sql := range []string{
			"select distinct e from " + table + " order by e",
			"select e from (select e from " + table + ") d order by e",
			"with c as (select e from " + table + ") select row_number() over (order by e) from c",
			"select e from " + table + " union all select e from " + table + " order by e",
		} {
			t.Run(sql, func(t *testing.T) {
				_, err := runOneStmt(newMySQLSpecialOrderMock(), t, sql)
				require.Error(t, err)
				require.Contains(t, err.Error(), "non-unique display labels")
			})
		}
	}
}

func TestMySQLSpecialOrderProvenanceRejectsSetWithEmptyMember(t *testing.T) {
	logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select id, s from set_empty_member_t order by s")
	require.NoError(t, err)
	requireSingleSortKeyType(t, logicPlan, types.T_uint64)

	_, err = runOneStmt(newMySQLSpecialOrderMock(), t,
		"select id, s from (select id, s from set_empty_member_t) d order by s, id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous SET display values")
}

func TestMySQLSpecialOrderProvenanceInGroupConcat(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		typ  types.T
	}{
		{name: "direct enum", sql: "select group_concat(e order by e) from enum_order_t", typ: types.T_enum},
		{name: "direct non-reversible enum keeps raw ordinal", sql: "select group_concat(e order by e) from enum_duplicate_t", typ: types.T_enum},
		{name: "derived enum", sql: "select group_concat(e order by e) from (select e from enum_order_t) d", typ: types.T_enum},
		{name: "derived enum ordinal", sql: "select group_concat(e order by 1) from (select e from enum_order_t) d", typ: types.T_enum},
		{name: "cte set", sql: "with c as (select s from enum_order_t) select group_concat(s order by s) from c", typ: types.T_uint64},
		{name: "derived explicit cast stays lexical", sql: "select group_concat(e order by cast(e as char)) from (select e from enum_order_t) d", typ: types.T_varchar},
		{name: "derived expression stays lexical", sql: "select group_concat(e order by concat(e, '')) from (select e from enum_order_t) d", typ: types.T_varchar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.sql)
			require.NoError(t, err)
			requireSingleGroupConcatOrderKeyType(t, logicPlan, tc.typ)
		})
	}

	_, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select group_concat(e order by e) from (select e from enum_duplicate_t) d")
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-unique display labels")
}
