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
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

type zoneMapStatsTestCompilerContext struct {
	*MockCompilerContext
	stats *statsinfo.StatsInfo
	err   error
}

func (ctx *zoneMapStatsTestCompilerContext) Stats(*ObjectRef, *Snapshot) (*statsinfo.StatsInfo, error) {
	return ctx.stats, ctx.err
}

func TestCalcScanStatsSimpleBlockFilters(t *testing.T) {
	ctx := context.Background()
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	col := GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 1, 1)
	bound := MakePlan2Int64ConstExprWithType(100)
	between := bind("between", col, bound, MakePlan2Int64ConstExprWithType(150))
	inList := func(items ...*planpb.Expr) *planpb.Expr {
		return bind("in", col, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_List{
			List: &planpb.ExprList{List: items},
		}})
	}
	in := inList(bound, MakePlan2Int64ConstExprWithType(150))
	null := &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	inVector := func(values []int64) *planpb.Expr {
		v := vector.NewVec(types.T_int64.ToType())
		defer v.Free(mp)
		require.NoError(t, vector.AppendFixedList(v, values, nil, mp))
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		return MakeInExpr(ctx, col, int32(len(values)), data, false)
	}
	for _, tc := range []struct {
		name                               string
		expr                               *planpb.Expr
		hints                              *OptimizerHints
		missing, statsErr, partition, want bool
	}{
		{name: "in list full overlap", expr: in, want: true},
		{name: "in vector", expr: inVector([]int64{100, 150}), want: true},
		{name: "in empty vector", expr: inVector(nil), want: true},
		{name: "in missing stats", expr: in, missing: true, want: true},
		{name: "in stats error", expr: in, statsErr: true, want: true},
		{name: "in disabled", expr: in, hints: &OptimizerHints{blockFilter: 2}},
		{name: "in missing disabled", expr: in, missing: true, hints: &OptimizerHints{blockFilter: 2}},
		{name: "in parameter list", expr: inList(bound, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}), want: true},
		{name: "in nullable list", expr: inList(null, bound), want: true},
		{name: "in all null", expr: inList(null), want: true},
		{name: "in correlated list", expr: inList(bound, GetColExpr(col.Typ, 1, 0))},
		{name: "in volatile list", expr: inList(bind("rand"), bound)},
		{name: "in volatile forced", expr: inList(bind("rand"), bound), hints: &OptimizerHints{blockFilter: 1}},
		{name: "in computed column", expr: bind("in", bind("+", col, bound), DeepCopyExpr(in.GetF().Args[1]))},
		{name: "not in", expr: bind("not", DeepCopyExpr(in))},
		{name: "in partition keeps old admission", expr: in, partition: true},
		{name: "full overlap", expr: between, want: true},
		{name: "default hint", expr: between, hints: &OptimizerHints{}, want: true},
		{name: "unrelated hint", expr: between, hints: &OptimizerHints{execType: 2}, want: true},
		{name: "force", expr: between, hints: &OptimizerHints{blockFilter: 1}, want: true},
		{name: "disabled", expr: between, hints: &OptimizerHints{blockFilter: 2}},
		{name: "missing stats", expr: between, missing: true, want: true},
		{name: "stats error", expr: between, statsErr: true, want: true},
		{name: "missing disabled", expr: between, missing: true, hints: &OptimizerHints{blockFilter: 2}},
		{name: "partition keeps old admission", expr: between, partition: true},
		{name: "partition missing stats", expr: between, partition: true, missing: true},
		{name: "column comparison", expr: bind("=", col, GetColExpr(col.Typ, 1, 0))},
		{name: "column arithmetic", expr: bind(">", bind("+", col, bound), bound)},
		{name: "force arithmetic", expr: bind(">", bind("+", col, bound), bound), hints: &OptimizerHints{blockFilter: 1}, want: true},
		{name: "nonmonotonic", expr: bind(">", bind("sin", col), bound), hints: &OptimizerHints{blockFilter: 1}},
		{name: "volatile", expr: bind(">", col, bind("rand"))},
		{name: "volatile forced", expr: bind(">", col, bind("rand")), hints: &OptimizerHints{blockFilter: 1}},
		{name: "constant expression", expr: bind(">", col, bind("+", bound, bound)), want: true},
		{name: "parameter", expr: bind(">", col, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}), want: true},
		{name: "in range", expr: bind("in_range", col, bound, bound, MakePlan2Uint8ConstExprWithType(0)), want: true},
		{name: "commuted", expr: bind("<", bound, col), want: true},
		{name: "eq", expr: bind("=", col, bound), want: true},
		{name: "lt", expr: bind("<", col, bound), want: true},
		{name: "le", expr: bind("<=", col, bound), want: true},
		{name: "gt", expr: bind(">", col, bound), want: true},
		{name: "ge", expr: bind(">=", col, bound), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := &statsinfo.StatsInfo{TableCnt: 60000, BlockNumber: 9,
				ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"k": {Overlap: 1}}}
			if tc.missing {
				stats = nil
			}
			compCtx := &zoneMapStatsTestCompilerContext{MockCompilerContext: &MockCompilerContext{ctx: ctx}, stats: stats}
			if tc.statsErr {
				compCtx.err = errors.New("statistics unavailable")
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, compCtx, false, false)
			builder.optimizerHints = tc.hints
			filter := DeepCopyExpr(tc.expr)
			filter.Selectivity = 0.5
			node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42},
				TableDef: &planpb.TableDef{TblId: 42, Name: "ordinary",
					Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
					Cols: []*planpb.ColDef{{Name: "id", Primary: true, Typ: col.Typ}, {Name: "k", Seqnum: 1, Typ: col.Typ}}},
				FilterList: []*planpb.Expr{filter}}
			if tc.partition {
				node.TableDef.Partition = &planpb.Partition{}
			}
			for range 2 {
				actual := calcScanStats(node, builder)
				if tc.missing || tc.statsErr {
					require.Equal(t, DefaultStats(), actual)
				} else {
					require.Equal(t, 60000*estimateFilterBlockSelectivity(ctx, filter, node.TableDef, stats), actual.Cost)
				}
				if tc.want {
					require.Len(t, node.BlockFilterList, 1)
					require.True(t, exprStructuralEqual(filter, node.BlockFilterList[0]))
					require.NotSame(t, filter, node.BlockFilterList[0])
				} else {
					require.Empty(t, node.BlockFilterList)
				}
				require.Same(t, filter, node.FilterList[0])
			}
		})
	}
}

// 通过真实解析/绑定路径证明常量 IN 的列表和折叠向量都能到达块过滤。
func TestPlanNonPrimaryInBlockFilters(t *testing.T) {
	for _, tc := range []struct {
		sql            string
		scans, filters int
		columnType     types.T
	}{
		{"select n_name from nation where n_name in ('2024.12', '2025.12')", 1, 1, 0},
		{"select n_name from nation where n_name in (null, '2025.12', '2024.12', '2025.12')", 1, 1, 0},
		{"select n_regionkey from nation where n_regionkey in (1, 3)", 1, 1, 0},
		{"select n_name from nation where n_name in (select n_name from nation2)", 2, 0, 0},
		{"select n_regionkey from nation where n_regionkey in (100, cast('NaN' as double))", 1, 0, types.T_float64},
		{"select n_regionkey from nation where n_regionkey in (cast('NaN' as double), 100)", 1, 0, types.T_float64},
		{"select n_regionkey from nation where n_regionkey in (100, 150)", 1, 0, types.T_float32},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockCompilerContext(false)
			// 通用 mock 以 map 遍历序号分配 ID，可能落入系统表保留 ID。
			mock.tables["nation"].TblId = 42001
			mock.tables["nation2"].TblId = 42002
			if tc.columnType != 0 {
				for _, col := range mock.tables["nation"].Cols {
					if col.Name == "n_regionkey" {
						col.Typ = planpb.Type{Id: int32(tc.columnType)}
					}
				}
			}
			ctx := &zoneMapStatsTestCompilerContext{MockCompilerContext: mock, stats: &statsinfo.StatsInfo{
				TableCnt: 60000, BlockNumber: 9,
				ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"n_name": {Overlap: 1}, "n_regionkey": {Overlap: 1}},
			}}
			stmt, err := mysql.ParseOne(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			scans, inFilters := 0, 0
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType != planpb.Node_TABLE_SCAN {
					continue
				}
				scans++
				for _, filter := range node.BlockFilterList {
					if filter.GetF().GetFunc().GetObjName() == "in" {
						inFilters++
						require.NotNil(t, filter.GetF().Args[0].GetCol())
						require.True(t, isRuntimeConstExpr(filter.GetF().Args[1]))
					}
				}
			}
			require.Equal(t, tc.scans, scans)
			require.Equal(t, tc.filters, inFilters, "只有稳定常量列表可下推；子查询 IN 不在该准入范围")
		})
	}
}

// 浮点 List/Vec 可能含执行期才出现的 NaN，不能走新增的无估计收益准入。
func TestFloatInKeepsExistingZoneMapAdmission(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	for _, oid := range []types.T{types.T_float32, types.T_float64} {
		t.Run(oid.String(), func(t *testing.T) {
			col := GetColExpr(planpb.Type{Id: int32(oid)}, 1, 1)
			finite, nan := MakePlan2Float64ConstExprWithType(100), MakePlan2Float64ConstExprWithType(math.NaN())
			v := vector.NewVec(oid.ToType())
			defer v.Free(mp)
			if oid == types.T_float32 {
				finite, nan = MakePlan2Float32ConstExprWithType(100), MakePlan2Float32ConstExprWithType(float32(math.NaN()))
				require.NoError(t, vector.AppendFixedList(v, []float32{100, float32(math.NaN())}, nil, mp))
			} else {
				require.NoError(t, vector.AppendFixedList(v, []float64{100, math.NaN()}, nil, mp))
			}
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			list := func(items ...*planpb.Expr) *planpb.Expr {
				expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*planpb.Expr{col, {
					Typ: col.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: items}},
				}})
				require.NoError(t, err)
				return expr
			}
			for _, tc := range []struct {
				name string
				expr *planpb.Expr
			}{
				{"finite list", list(finite, finite)},
				{"nan last", list(finite, nan)},
				{"nan first", list(nan, finite)},
				{"nan vector", MakeInExpr(ctx, col, 2, data, false)},
				{"parameter", list(finite, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}})},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.False(t, isSimpleZoneMapFilter(tc.expr))
					for _, missing := range []bool{false, true} {
						stats := &statsinfo.StatsInfo{TableCnt: 60000, BlockNumber: 9,
							ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"k": {Overlap: 1}}}
						if missing {
							stats = nil
						}
						builder := NewQueryBuilder(planpb.Query_SELECT, &zoneMapStatsTestCompilerContext{
							MockCompilerContext: &MockCompilerContext{ctx: ctx}, stats: stats,
						}, false, false)
						filter := DeepCopyExpr(tc.expr)
						node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42},
							TableDef: &planpb.TableDef{TblId: 42, Name: "ordinary",
								Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
								Cols: []*planpb.ColDef{{Name: "id", Primary: true, Typ: planpb.Type{Id: int32(types.T_int32)}},
									{Name: "k", Seqnum: 1, Typ: col.Typ}}}, FilterList: []*planpb.Expr{filter}}
						calcScanStats(node, builder)
						require.Empty(t, node.BlockFilterList, "missing statistics: %v", missing)
						require.Same(t, filter, node.FilterList[0])
					}
				})
			}
		})
	}
}

// 新增准入是有界的安全子集，不改变旧统计/显式 hint 的策略。
func TestSimpleInAdmissionBounds(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	check := func(t *testing.T, expr *planpb.Expr, want bool) {
		require.Equal(t, want, isSimpleZoneMapFilter(expr), "%s", expr)
		for _, missing := range []bool{false, true} {
			stats := &statsinfo.StatsInfo{TableCnt: 60000, BlockNumber: 9,
				ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"k": {Overlap: 1}}}
			if missing {
				stats = nil
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, &zoneMapStatsTestCompilerContext{
				MockCompilerContext: &MockCompilerContext{ctx: ctx}, stats: stats,
			}, false, false)
			node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42},
				TableDef: &planpb.TableDef{TblId: 42, Name: "ordinary",
					Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
					Cols: []*planpb.ColDef{{Name: "id", Primary: true, Typ: planpb.Type{Id: int32(types.T_int32)}},
						{Name: "k", Seqnum: 1, Typ: expr.GetF().Args[0].Typ}}}, FilterList: []*planpb.Expr{expr}}
			calcScanStats(node, builder)
			require.Equal(t, want, len(node.BlockFilterList) == 1, "missing=%v", missing)
			require.Same(t, expr, node.FilterList[0])
		}
	}
	col := GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 1, 1)
	for _, n := range []int{63, 64, 65} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			items := make([]*planpb.Expr, n)
			v := vector.NewVec(types.T_int64.ToType())
			defer v.Free(mp)
			for i := range items {
				items[i] = MakePlan2Int64ConstExprWithType(int64(i))
				require.NoError(t, vector.AppendFixed(v, int64(i), i == n-1, mp))
			}
			items[n-1] = &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*planpb.Expr{col, {
				Typ: col.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: items}},
			}})
			require.NoError(t, err)
			check(t, expr, n <= 64)
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			check(t, MakeInExpr(ctx, col, int32(n), data, false), n <= 64)
		})
	}
	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text} {
		for _, n := range []int{30, 31} {
			t.Run(oid.String()+"/"+strconv.Itoa(n), func(t *testing.T) {
				col := GetColExpr(planpb.Type{Id: int32(oid)}, 1, 1)
				// 即使 VARCHAR 含任意字节，也不能凭类型假设上界安全。
				value := strings.Repeat("\xff", n)
				lit := &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: value}}}}
				null := &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
				expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*planpb.Expr{col, {
					Typ: col.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{lit, null}}},
				}})
				require.NoError(t, err)
				// binder 可插入 cast；此处覆盖常量折叠后的 typed List 表示。
				expr.GetF().Args[1].GetList().List = []*planpb.Expr{lit, null}
				want := (oid == types.T_char || oid == types.T_varchar) && n <= 30
				check(t, expr, want)
				v := vector.NewVec(oid.ToType())
				defer v.Free(mp)
				require.NoError(t, vector.AppendBytes(v, []byte(value), false, mp))
				require.NoError(t, vector.AppendBytes(v, nil, true, mp))
				data, err := v.MarshalBinary()
				require.NoError(t, err)
				check(t, MakeInExpr(ctx, col, 2, data, false), want)
				expr.GetF().Args[1].GetList().List[0] = &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
				check(t, expr, false)
			})
		}
	}
}

func TestSmallInPhysicalPayloadBudget(t *testing.T) {
	col := GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 1, 1)
	// 仅检查准入预算，不解码伪造数据；物理上界与逻辑Len独立。
	for _, size := range []int{4095, 4096, 4097} {
		expr := MakeInExpr(context.Background(), col, 2, make([]byte, size), false)
		require.Equal(t, size <= 4096, isSimpleZoneMapFilter(expr))
	}
}

func TestFinalStringInLogicalIdentity(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	col := GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 1, 1)
	a, b, c := strings.Repeat("a", 30), strings.Repeat("b", 30), strings.Repeat("c", 30)
	makeIn := func(values []string, null bool, unused bool, width int32) *planpb.Expr {
		v := vector.NewVec(types.T_varchar.ToType())
		defer v.Free(mp)
		v.GetType().Width = width
		if unused {
			require.NoError(t, vector.AppendBytes(v, []byte(c), false, mp))
		}
		for _, value := range values {
			require.NoError(t, vector.AppendBytes(v, []byte(value), false, mp))
		}
		if unused {
			v.Shrink([]int64{1, 2}, false)
		}
		if null {
			require.NoError(t, vector.AppendBytes(v, nil, true, mp))
		}
		v.InplaceSortAndCompact()
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		return MakeInExpr(context.Background(), col, int32(v.Length()), data, false)
	}
	for _, tc := range []struct {
		name        string
		left, right *planpb.Expr
		want        int
	}{
		{"opposite insertion", makeIn([]string{a, b}, false, false, 30), makeIn([]string{b, a}, false, false, 30), 1},
		{"unreferenced area", makeIn([]string{a, b}, false, false, 30), makeIn([]string{a, b}, false, true, 30), 1},
		{"nullable order", makeIn([]string{a, b}, true, false, 30), makeIn([]string{b, a}, true, false, 30), 1},
		{"different nulls", makeIn([]string{a, b}, false, false, 30), makeIn([]string{a, b}, true, false, 30), 2},
		{"different values", makeIn([]string{a, b}, false, false, 30), makeIn([]string{a, c}, false, false, 30), 2},
		{"different actual type", makeIn([]string{a, b}, false, false, 30), makeIn([]string{a, b}, false, false, 31), 2},
		{"null versus empty", makeIn([]string{a, ""}, false, false, 30), makeIn([]string{a}, true, false, 30), 2},
		{"embedded zero", makeIn([]string{"a\x00b", "c"}, false, false, 30), makeIn([]string{"a", "b\x00c"}, false, false, 30), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeLeft, beforeRight := DeepCopyExpr(tc.left), DeepCopyExpr(tc.right)
			result := DeduplicateFinalBlockFilters([]*planpb.Expr{tc.left, tc.right})
			require.Len(t, result, tc.want)
			require.Same(t, tc.left, result[0])
			require.True(t, exprStructuralEqual(tc.left, beforeLeft))
			require.True(t, exprStructuralEqual(tc.right, beforeRight))
		})
	}
}

func TestPlanLargeNullableInKeepsExistingAdmission(t *testing.T) {
	mock := NewMockCompilerContext(false)
	mock.tables["nation"].TblId = 42001
	ctx := &zoneMapStatsTestCompilerContext{MockCompilerContext: mock}
	values := make([]string, 64)
	for i := range values {
		values[i] = strconv.Itoa(i)
	}
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select n_regionkey from nation where n_regionkey in ("+strings.Join(values, ",")+",null)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	qry, err := NewPrepareOptimizer(ctx).Optimize(stmt, true)
	require.NoError(t, err)
	scans := 0
	for _, node := range qry.Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN {
			scans++
			require.Empty(t, node.BlockFilterList)
			require.Len(t, node.FilterList, 1)
		}
	}
	require.Equal(t, 1, scans)
}

// BuildPlan 之后还会折叠 List/Vec；必须检查完整优化后的 reader 输入。
func TestFinalInDomainBlockFiltersAreUnique(t *testing.T) {
	for _, tc := range []struct {
		name, predicate  string
		prepared         bool
		narrow, residual int
	}{
		{"ordinary difference", "n_nationkey in (1,2,3,4) and n_nationkey not in (2,3)", false, 2, 1},
		{"prepared difference", "n_nationkey in (1,2,3,4) and n_nationkey not in (2,3)", true, 2, 1},
		{"ordinary or", "n_nationkey in (1,2,3,4) and (n_nationkey not in (2,3) or n_nationkey=99)", false, 3, 2},
		{"prepared or", "n_nationkey in (1,2,3,4) and (n_nationkey not in (2,3) or n_nationkey=99)", true, 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockCompilerContext(false)
			mock.tables["nation"].TblId = 42001
			ctx := &zoneMapStatsTestCompilerContext{MockCompilerContext: mock}
			stmt, err := mysql.ParseOne(ctx.GetContext(), "select n_nationkey from nation where "+tc.predicate, 1)
			require.NoError(t, err)
			defer stmt.Free()
			opt := NewBaseOptimizer(ctx)
			if tc.prepared {
				opt = NewPrepareOptimizer(ctx)
			}
			qry, err := opt.Optimize(stmt, tc.prepared)
			require.NoError(t, err)
			scans := 0
			for _, node := range qry.Nodes {
				if node.NodeType != planpb.Node_TABLE_SCAN {
					continue
				}
				scans++
				// 宽域和窄域语义不同，各保留一次；窄域不能执行两次。
				require.Len(t, node.BlockFilterList, 2)
				lengths := make([]int32, 0, 2)
				for _, filter := range node.BlockFilterList {
					require.Equal(t, "in", filter.GetF().GetFunc().GetObjName())
					vec := filter.GetF().Args[1].GetVec()
					require.NotNil(t, vec)
					lengths = append(lengths, vec.Len)
				}
				require.Equal(t, []int32{4, int32(tc.narrow)}, lengths)
				require.Len(t, node.FilterList, tc.residual)
				require.Equal(t, int32(tc.narrow), node.FilterList[0].GetF().Args[1].GetVec().Len)
			}
			require.Equal(t, 1, scans)
		})
	}
}

func TestFinalBlockFiltersPreserveDistinctExpressions(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	col := GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 1, 0)
	bound := MakePlan2Int64ConstExprWithType(1)
	null := &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), name, args)
		require.NoError(t, err)
		return expr
	}
	list := func(items ...*planpb.Expr) *planpb.Expr {
		return bind("in", col, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: items}}})
	}
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	tupleList := list(bound, MakePlan2Int64ConstExprWithType(4))
	tupleList.GetF().Args[1].Typ = planpb.Type{Id: int32(types.T_tuple)}
	variable := &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "block_filter_test"}}}
	large := make([]*planpb.Expr, 65)
	for i := range large {
		large[i] = MakePlan2Int64ConstExprWithType(int64(i))
	}
	for _, tc := range []struct {
		name    string
		filters []*planpb.Expr
		want    int
	}{
		{"empty", nil, 0},
		{"single", []*planpb.Expr{list(bound)}, 1},
		{"folded list order", []*planpb.Expr{list(bound, MakePlan2Int64ConstExprWithType(4)), list(MakePlan2Int64ConstExprWithType(4), bound)}, 1},
		{"vector type annotation", []*planpb.Expr{tupleList, list(bound, MakePlan2Int64ConstExprWithType(4))}, 1},
		{"different values", []*planpb.Expr{list(bound), list(MakePlan2Int64ConstExprWithType(4))}, 2},
		{"null bitmap differs", []*planpb.Expr{list(MakePlan2Int64ConstExprWithType(0), bound), list(null, bound), list(null, bound)}, 2},
		{"same parameter", []*planpb.Expr{list(bound, param(0)), list(bound, param(0))}, 1},
		{"large static vector unchanged", []*planpb.Expr{list(large...), list(large...)}, 2},
		{"non IN unchanged", []*planpb.Expr{bind("=", col, bound), bind("=", col, bound)}, 2},
		{"different parameters", []*planpb.Expr{list(bound, param(0)), list(bound, param(1))}, 2},
		{"different columns", []*planpb.Expr{bind("=", col, bound), bind("=", GetColExpr(col.Typ, 1, 1), bound)}, 2},
		{"different types", []*planpb.Expr{bind("=", col, bound), bind("=", GetColExpr(planpb.Type{Id: int32(types.T_int32)}, 1, 0), bound)}, 2},
		{"variable remains", []*planpb.Expr{list(bound, variable), list(bound, variable)}, 2},
		{"volatile remains", []*planpb.Expr{bind(">", col, bind("rand")), bind(">", col, bind("rand"))}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filters := DeepCopyExprList(tc.filters)
			var first *planpb.Expr
			if len(filters) > 0 {
				first = filters[0]
			}
			node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, BlockFilterList: filters, FilterList: DeepCopyExprList(tc.filters)}
			opt := NewPrepareOptimizer(ctx)
			opt.qry = &planpb.Query{Steps: []int32{0}, Nodes: []*planpb.Node{node}}
			_, err := opt.optimize()
			require.NoError(t, err)
			require.Len(t, node.BlockFilterList, tc.want)
			if first != nil {
				require.Same(t, first, node.BlockFilterList[0], "保留原表达式及顺序，而不是比较视图")
			}
			if tc.name == "vector type annotation" {
				require.Equal(t, int32(types.T_tuple), first.GetF().Args[1].Typ.Id)
			}
			for _, removed := range filters[tc.want:] {
				require.Nil(t, removed, "不能由切片尾部持有已删除的表达式")
			}
			require.Len(t, node.FilterList, len(tc.filters), "不能去掉残余行过滤")
		})
	}
}

func TestMissingStatsPreservesExistingBlockFilters(t *testing.T) {
	ctx := context.Background()
	col := GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 1, 0)
	prefix, err := BindFuncExprImplByPlanExpr(ctx, "prefix_eq", []*planpb.Expr{col, MakePlan2StringConstExprWithType("existing")})
	require.NoError(t, err)
	filter, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{col, MakePlan2StringConstExprWithType("new")})
	require.NoError(t, err)
	for _, disabled := range []bool{false, true} {
		builder := NewQueryBuilder(planpb.Query_SELECT, &zoneMapStatsTestCompilerContext{MockCompilerContext: &MockCompilerContext{ctx: ctx}}, false, false)
		builder.optimizerHints = &OptimizerHints{}
		if disabled {
			builder.optimizerHints.blockFilter = 2
		}
		original := []*planpb.Expr{prefix}
		node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42},
			TableDef:   &planpb.TableDef{TblId: 42, Name: catalog.IndexTableNamePrefix + "test"},
			FilterList: []*planpb.Expr{filter}, BlockFilterList: original}
		for range 2 {
			require.Equal(t, DefaultStats(), calcScanStats(node, builder))
			if disabled {
				require.Empty(t, node.BlockFilterList)
			} else {
				require.Len(t, node.BlockFilterList, 2)
			}
			require.Equal(t, []*planpb.Expr{prefix}, original)
		}
	}
}
