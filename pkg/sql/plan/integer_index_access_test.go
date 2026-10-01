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
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestExactDecimalIntegerDomain(t *testing.T) {
	for _, tc := range []struct {
		value  string
		target types.T
		fits   bool
	}{
		{"0.0", types.T_uint64, true}, {"127.0", types.T_int8, true}, {"128.0", types.T_int8, false},
		{"-128.0", types.T_int8, true}, {"-129.0", types.T_int8, false}, {"-1.0", types.T_uint64, false},
		{"2147483647.0", types.T_int32, true}, {"2147483648.0", types.T_int32, false},
		{"-2147483648.0", types.T_int32, true}, {"-2147483649.0", types.T_int32, false},
		{"9223372036854775807.0", types.T_int64, true}, {"9223372036854775808.0", types.T_int64, false},
		{"-9223372036854775808.0", types.T_int64, true}, {"-9223372036854775809.0", types.T_int64, false},
		{"18446744073709551615.0", types.T_uint64, true}, {"18446744073709551616.0", types.T_uint64, false},
		{"9007199254740993.0", types.T_int64, true}, {"9.000002", types.T_int32, false},
		{"0.00000000000000000000000000000000000001", types.T_int64, false},
		{"9007199254740993.0000000000000000000000000000000000000000", types.T_int64, true},
		{"18446744073709551616.0000000000000000000000000000000000000000", types.T_uint64, false},
	} {
		t.Run(tc.value+tc.target.String(), func(t *testing.T) {
			expr, err := makePlan2DecimalExprWithType(context.Background(), tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.fits, exactDecimalIntegerFits(expr, tc.target))
			col := GetColExpr(pb.Type{Id: int32(tc.target)}, 0, 0)
			bound, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*pb.Expr{col, expr})
			require.NoError(t, err)
			require.Equal(t, tc.fits, bound.GetF().Args[0].GetCol() != nil)
		})
	}
	for _, tc := range []struct {
		oid   types.T
		value string
	}{{types.T_decimal64, "127.0"}, {types.T_decimal128, "-128.0"}} {
		value, scale, err := types.Parse128(tc.value)
		require.NoError(t, err)
		var expr *pb.Expr
		if tc.oid == types.T_decimal64 {
			expr = MakePlan2Decimal64ExprWithType(types.Decimal64(value.B0_63), &pb.Type{Id: int32(tc.oid), Width: 18, Scale: scale})
		} else {
			expr = MakePlan2Decimal128ExprWithType(value, &pb.Type{Id: int32(tc.oid), Width: 38, Scale: scale})
		}
		require.True(t, exactDecimalIntegerFits(expr, types.T_int8))
		expr.Typ.Scale = -1
		require.False(t, exactDecimalIntegerFits(expr, types.T_int8))
	}
}

func TestExactDecimalNativeIntegerFilters(t *testing.T) {
	for _, predicate := range []string{"id=3.0", "3.0=id", "id>=3.0", "3.0<id", "id in (1.0,3.0)", "id between 1.0 and 3.0"} {
		t.Run(predicate, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			query, err := runOneStmt(mock, t, "select id from index_hint_t use index() where "+predicate)
			require.NoError(t, err)
			var scan *pb.Node
			for _, n := range query.GetQuery().Nodes {
				if n.NodeType == pb.Node_TABLE_SCAN && !n.IndexScanInfo.IsIndexScan {
					scan = n
				}
			}
			require.NotNil(t, scan)
			require.NotEmpty(t, scan.FilterList)
			for _, filter := range scan.FilterList {
				require.Zero(t, countExprFunctionCalls([]*pb.Expr{filter}, "cast"), predicate)
			}
		})
	}
}

func TestNullableUniqueHintCompleteness(t *testing.T) {
	for _, tc := range []struct {
		sql       string
		usesIndex bool
	}{
		{"select id,a,b from index_hint_t force index(uk_ab) order by id", false},
		{"select a,b from index_hint_t force index(uk_ab) order by a", false},
		{"select id from index_hint_t force index(uk_ab) where a is null order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a=1 order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a is not null order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a=1 or b=2 order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a is not null and b is not null order by id", true},
		{"select id from index_hint_t force index(uk_ab) where (a=1 and b=2) or (a=3 and b=4) order by id", true},
		{"select a,count(*) from index_hint_t force index for group by(uk_ab) group by a order by a", false},
		{"select id from index_hint_t force index for join(uk_ab) join nation on id=n_nationkey", false},
		{"select id,a,b from index_hint_t force index(idx_ab) order by id", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			query, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.usesIndex, findFirstIndexScanName(query) != "")
		})
	}
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	for _, c := range mock.ctxt.tables["index_hint_t"].Cols {
		c.Typ.NotNullable = true
		c.Default = nil
	}
	query, err := runOneStmt(mock, t, "select a,b from index_hint_t force index(uk_ab) order by a")
	require.NoError(t, err)
	require.Equal(t, "uk_ab", findFirstIndexScanName(query))
}

func TestSparseUniqueRejectionIsAtomic(t *testing.T) {
	builder := NewQueryBuilder(pb.Query_SELECT, NewMockCompilerContext(true), false, true)
	tag := builder.genNewBindTag()
	node := &pb.Node{BindingTags: []int32{tag}, TableDef: &pb.TableDef{Cols: []*pb.ColDef{{Name: "k", Typ: pb.Type{Id: int32(types.T_int32)}}}, Name2ColIndex: map[string]int32{"k": 0}, Pkey: &pb.PrimaryKeyDef{PkeyColName: "k"}}}
	index := &pb.IndexDef{Unique: true, Parts: []string{"k"}, IndexName: "uq", IndexTableName: "missing", TableExist: true}
	mapping := make(map[[2]int32]*pb.Expr)
	before := builder.nextBindTag
	id, err := builder.tryHintedCoveringIndexScan(index, node, map[[2]int32]int{{tag, 0}: 1}, mapping)
	require.NoError(t, err)
	require.EqualValues(t, -1, id)
	id, _, err = builder.buildHintedIndexBackfillJoin(index, node)
	require.NoError(t, err)
	require.EqualValues(t, -1, id)
	require.Empty(t, builder.qry.Nodes)
	require.Empty(t, mapping)
	require.Equal(t, before, builder.nextBindTag)
}

func TestExactDecimalPreservesOtherDomains(t *testing.T) {
	for _, predicate := range []string{"id=3.1", "id=2147483648.0", "id=3e0", "cast(id as decimal(12,1))=3.0"} {
		t.Run(predicate, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			query, err := runOneStmt(mock, t, "select id from index_hint_t use index() where "+predicate)
			require.NoError(t, err)
			casts := 0
			for _, n := range query.GetQuery().Nodes {
				if n.NodeType == pb.Node_TABLE_SCAN {
					casts += countExprFunctionCalls(n.FilterList, "cast")
				}
			}
			require.Positive(t, casts, "unproven and explicitly declared domains retain their column cast")
		})
	}
}
